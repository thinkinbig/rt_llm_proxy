package offer

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/modelcb"
	"github.com/thinkinbig/rt-llm-proxy/internal/ratelimit"
	"github.com/thinkinbig/rt-llm-proxy/internal/rtc"
	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

type fakeModel struct {
	closeCount int
}

func (f *fakeModel) SendAudio([]int16) error        { return nil }
func (f *fakeModel) SendText(string) error          { return nil }
func (f *fakeModel) Recv() ([]int16, error)         { return nil, io.EOF }
func (f *fakeModel) RecvInterrupted() (bool, error) { return false, nil }
func (f *fakeModel) SupportsInterruption() bool     { return false }
func (f *fakeModel) HandleInterrupted() error       { return nil }
func (f *fakeModel) Close() error {
	f.closeCount++
	return nil
}

type fakeFactory struct {
	m               model.Model
	err             error
	newN            int
	lastCtxCanceled bool
	lastHistory     []model.RestoredTurn
	lastParams      model.SessionParams
}

func (f *fakeFactory) New(ctx context.Context, _ string, history []model.RestoredTurn, params model.SessionParams) (model.Model, error) {
	f.newN++
	f.lastHistory = history
	f.lastParams = params
	if ctx != nil {
		select {
		case <-ctx.Done():
			f.lastCtxCanceled = true
		default:
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.m, nil
}

type fakeHub struct {
	serveN int
	err    error
	// resume* configure a memory_hit on Resume (zero values = miss, the default).
	resumeFull []transcript.Line
	resumeSeq  uint64
	resumeOK   bool
}

func (h *fakeHub) Serve(_ string, m model.Model, _ rtc.SessionInfo) (string, error) {
	h.serveN++
	if h.err != nil {
		// Serve owns m: it closes it on every error return (see Hub.Serve doc).
		m.Close()
		return "", h.err
	}
	return "v=0", nil
}

func (h *fakeHub) SessionState(identity.SessionID, identity.UserID) (string, uint64, bool) {
	return "", 0, false
}

func (h *fakeHub) Resume(_ identity.SessionID, _ identity.UserID, _ string, afterSeq uint64) ([]transcript.Line, []transcript.Line, uint64, bool) {
	if !h.resumeOK {
		return nil, nil, 0, false
	}
	var replay []transcript.Line
	for _, l := range h.resumeFull {
		if l.Seq > afterSeq {
			replay = append(replay, l)
		}
	}
	return h.resumeFull, replay, h.resumeSeq, true
}

func TestIntakeCircuitOpen(t *testing.T) {
	guard := modelcb.New(modelcb.Config{OpenAfter: 1, OpenFor: time.Hour, HalfOpenSuccess: 1, AuthOpenFor: time.Hour}, nil)
	now := time.Now()
	guard.RecordDial("gemini", errors.New("timeout"), now)

	in := Intake{
		Limiter: ratelimit.New("", 0, time.Minute),
		Guard:   guard,
		Models:  &fakeFactory{},
		Hub:     &fakeHub{},
	}
	res := in.ServeOffer(IntakeRequest{
		Ctx:      context.Background(),
		ClientIP: "1.2.3.4",
		Model:    "gemini",
		OfferSDP: []byte("sdp"),
	})
	if res.Status != 503 || res.Body != "model circuit open" {
		t.Fatalf("got %+v", res)
	}
	if res.Headers["X-Model-CB-State"] != string(modelcb.StateOpen) {
		t.Fatalf("headers %+v", res.Headers)
	}
}

func TestIntakeDialFailureClosesModel(t *testing.T) {
	fm := &fakeModel{}
	factory := &fakeFactory{err: errors.New("dial failed")}
	in := Intake{
		Limiter: ratelimit.New("", 0, time.Minute),
		Guard:   modelcb.New(modelcb.Config{}, nil),
		Models:  factory,
		Hub:     &fakeHub{},
	}
	res := in.ServeOffer(IntakeRequest{
		Ctx:      context.Background(),
		ClientIP: "1.2.3.4",
		Model:    "gemini",
		OfferSDP: []byte("sdp"),
	})
	if res.Status != 502 || fm.closeCount != 0 {
		t.Fatalf("status=%d close=%d", res.Status, fm.closeCount)
	}
	if factory.newN != 1 {
		t.Fatalf("new calls = %d", factory.newN)
	}
}

func TestIntakeReplayProtocolInvalid(t *testing.T) {
	fm := &fakeModel{}
	factory := &fakeFactory{m: fm}
	in := Intake{
		Limiter: ratelimit.New("", 0, time.Minute),
		Guard:   modelcb.New(modelcb.Config{}, nil),
		Models:  factory,
		Hub:     &fakeHub{},
	}
	res := in.ServeOffer(IntakeRequest{
		Ctx:                 context.Background(),
		ClientIP:            "1.2.3.4",
		Model:               "gemini",
		OfferSDP:            []byte("sdp"),
		ReplayVersionHeader: "99",
		SessionIDHeader:     "s",
		LastSeqHeader:       "1",
	})
	if res.Status != 400 || res.Headers["X-Replay-Status"] != "protocol_invalid" {
		t.Fatalf("got %+v", res)
	}
	// Replay resolves before the model is dialed, so a protocol-invalid request
	// is rejected without ever connecting a provider (no wasted dial / close).
	if factory.newN != 0 {
		t.Fatalf("new calls = %d want 0 (rejected before dial)", factory.newN)
	}
	if fm.closeCount != 0 {
		t.Fatalf("model close count = %d want 0", fm.closeCount)
	}
}

func TestIntakeReconnectThreadsHistoryToFactory(t *testing.T) {
	hub := &fakeHub{
		resumeOK:   true,
		resumeSeq:  2,
		resumeFull: []transcript.Line{{Seq: 1, Role: "user", Text: "hi"}, {Seq: 2, Role: "model", Text: "hello"}},
	}
	factory := &fakeFactory{m: &fakeModel{}}
	in := Intake{
		Limiter: ratelimit.New("", 0, time.Minute),
		Guard:   modelcb.New(modelcb.Config{}, nil),
		Models:  factory,
		Hub:     hub,
	}
	res := in.ServeOffer(IntakeRequest{
		Ctx:             context.Background(),
		ClientIP:        "1.2.3.4",
		Model:           "doubao",
		OfferSDP:        []byte("sdp"),
		UserID:          "alice", // non-anonymous: reconnect requires an owner
		SessionIDHeader: "s1",
		LastSeqHeader:   "1",
	})
	if res.Status != 200 || res.Headers["X-Replay-Status"] != "memory_hit" {
		t.Fatalf("got %+v", res)
	}
	// The freshly-dialed model is constructed WITH the restored history so an
	// adapter like doubao can seed dialog_context at session start.
	want := []model.RestoredTurn{{Role: "user", Text: "hi"}, {Role: "model", Text: "hello"}}
	if len(factory.lastHistory) != len(want) {
		t.Fatalf("factory history = %+v, want %+v", factory.lastHistory, want)
	}
	for i := range want {
		if factory.lastHistory[i] != want[i] {
			t.Fatalf("history[%d] = %+v, want %+v", i, factory.lastHistory[i], want[i])
		}
	}
}

func TestIntakeSuccess(t *testing.T) {
	hub := &fakeHub{}
	in := Intake{
		Limiter: ratelimit.New("", 0, time.Minute),
		Guard:   modelcb.New(modelcb.Config{}, nil),
		Models:  &fakeFactory{m: &fakeModel{}},
		Hub:     hub,
	}
	res := in.ServeOffer(IntakeRequest{
		Ctx:      context.Background(),
		ClientIP: "1.2.3.4",
		Model:    "gemini",
		OfferSDP: []byte("sdp"),
	})
	if res.Status != 200 || res.Body != "v=0" || hub.serveN != 1 {
		t.Fatalf("got %+v serveN=%d", res, hub.serveN)
	}
}

func TestIntakeServeFailureClosesModel(t *testing.T) {
	fm := &fakeModel{}
	in := Intake{
		Limiter: ratelimit.New("", 0, time.Minute),
		Guard:   modelcb.New(modelcb.Config{}, nil),
		Models:  &fakeFactory{m: fm},
		Hub:     &fakeHub{err: errors.New("webrtc failed")},
	}
	res := in.ServeOffer(IntakeRequest{
		Ctx:      context.Background(),
		ClientIP: "1.2.3.4",
		Model:    "gemini",
		OfferSDP: []byte("sdp"),
	})
	if res.Status != 500 || fm.closeCount != 1 {
		t.Fatalf("status=%d close=%d", res.Status, fm.closeCount)
	}
}

func TestIntakeUnknownModel(t *testing.T) {
	in := Intake{
		Limiter: ratelimit.New("", 0, time.Minute),
		Hub:     &fakeHub{},
	}
	res := in.ServeOffer(IntakeRequest{
		Ctx:      context.Background(),
		ClientIP: "1.2.3.4",
		Model:    "gpt",
		OfferSDP: []byte("sdp"),
	})
	if res.Status != 400 {
		t.Fatalf("got %+v", res)
	}
}

func TestIntakeDetachesModelContextFromCanceledRequest(t *testing.T) {
	factory := &fakeFactory{m: &fakeModel{}}
	in := Intake{
		Limiter: ratelimit.New("", 0, time.Minute),
		Guard:   modelcb.New(modelcb.Config{}, nil),
		Models:  factory,
		Hub:     &fakeHub{},
	}
	reqCtx, cancel := context.WithCancel(context.Background())
	cancel() // Simulate an HTTP request context that already ended.

	res := in.ServeOffer(IntakeRequest{
		Ctx:      reqCtx,
		ClientIP: "1.2.3.4",
		Model:    "gemini",
		OfferSDP: []byte("sdp"),
	})
	if res.Status != 200 {
		t.Fatalf("got %+v", res)
	}
	if factory.lastCtxCanceled {
		t.Fatalf("model factory received canceled context")
	}
}
