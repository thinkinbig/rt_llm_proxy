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

func (f *fakeModel) SendAudio([]int16) error { return nil }
func (f *fakeModel) SendText(string) error   { return nil }
func (f *fakeModel) Recv() ([]int16, error)  { return nil, io.EOF }
func (f *fakeModel) Close() error {
	f.closeCount++
	return nil
}

type fakeFactory struct {
	m    model.Model
	err  error
	newN int
}

func (f *fakeFactory) New(context.Context, string) (model.Model, error) {
	f.newN++
	if f.err != nil {
		return nil, f.err
	}
	return f.m, nil
}

type fakeHub struct {
	serveN int
	err    error
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

func (h *fakeHub) Resume(identity.SessionID, identity.UserID, string, uint64) ([]transcript.Line, []transcript.Line, uint64, bool) {
	return nil, nil, 0, false
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
	in := Intake{
		Limiter: ratelimit.New("", 0, time.Minute),
		Guard:   modelcb.New(modelcb.Config{}, nil),
		Models:  &fakeFactory{m: fm},
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
	if fm.closeCount != 1 {
		t.Fatalf("model close count = %d want 1", fm.closeCount)
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
