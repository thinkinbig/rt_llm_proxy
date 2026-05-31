package sidechannel

import (
	"io"
	"testing"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

// fakeModel implements model.Model with no transcripts.
type fakeModel struct{ sent []string }

func (f *fakeModel) SendAudio([]int16) error { return nil }
func (f *fakeModel) SendText(t string) error { f.sent = append(f.sent, t); return nil }
func (f *fakeModel) Recv() ([]int16, error)  { return nil, io.EOF }
func (f *fakeModel) Close() error            { return nil }

// fakeTranscriber additionally surfaces RecvTranscript.
type fakeTranscriber struct {
	*fakeModel
	lines []model.Transcript
	i     int
}

func (f *fakeTranscriber) RecvTranscript() (model.Transcript, error) {
	if f.i >= len(f.lines) {
		return model.Transcript{}, io.EOF
	}
	tr := f.lines[f.i]
	f.i++
	return tr, nil
}

type capture struct{ evs []*TranscriptEvent }

func (c *capture) Publish(ev *TranscriptEvent) { c.evs = append(c.evs, ev) }
func (c *capture) Close() error                { return nil }

type recvTranscripter interface {
	RecvTranscript() (model.Transcript, error)
}

func TestPartitionKey(t *testing.T) {
	if got := partitionKey(&TranscriptEvent{UserId: "alice", SessionId: "s1"}); got != "alice" {
		t.Errorf("known user: got %q want alice", got)
	}
	if got := partitionKey(&TranscriptEvent{UserId: "", SessionId: "s1"}); got != "s1" {
		t.Errorf("anonymous falls back to session id: got %q want s1", got)
	}
}

func TestWrapNilPublisherIsPassthrough(t *testing.T) {
	m := &fakeModel{}
	if got := Wrap(m, nil, Meta{}); got != model.Model(m) {
		t.Error("nil publisher should return the model unchanged")
	}
}

func TestWrapNonTranscriberHasNoRecvText(t *testing.T) {
	cap := &capture{}
	w := Wrap(&fakeModel{}, cap, Meta{SessionID: "s1", UserID: "alice", Provider: "gemini"})
	if _, ok := w.(recvTranscripter); ok {
		t.Fatal("wrapper must NOT satisfy transcriber when inner does not")
	}
	if err := w.SendText("hi"); err != nil {
		t.Fatal(err)
	}
	if len(cap.evs) != 1 {
		t.Fatalf("want 1 event, got %d", len(cap.evs))
	}
	ev := cap.evs[0]
	if ev.Role != Role_ROLE_USER || ev.Text != "hi" || ev.Seq != 1 ||
		ev.SessionId != "s1" || ev.UserId != "alice" || ev.Provider != "gemini" || ev.SchemaVersion != 1 {
		t.Errorf("unexpected event: %+v", ev)
	}
}

func TestWrapTranscriberTapsBothDirections(t *testing.T) {
	cap := &capture{}
	inner := &fakeTranscriber{fakeModel: &fakeModel{}, lines: []model.Transcript{{Role: "model", Text: "hello"}}}
	w := Wrap(inner, cap, Meta{SessionID: "s1"})
	rt, ok := w.(recvTranscripter)
	if !ok {
		t.Fatal("wrapper MUST satisfy transcriber when inner does")
	}

	_ = w.SendText("hi")                   // user event, seq 1
	tr, err := rt.RecvTranscript()          // model event, seq 2
	if err != nil || tr.Text != "hello" {
		t.Fatalf("RecvTranscript = %+v, %v", tr, err)
	}
	if len(inner.sent) != 1 || inner.sent[0] != "hi" {
		t.Errorf("SendText not passed through: %v", inner.sent)
	}
	if len(cap.evs) != 2 {
		t.Fatalf("want 2 events, got %d", len(cap.evs))
	}
	if cap.evs[0].Role != Role_ROLE_USER || cap.evs[0].Seq != 1 {
		t.Errorf("first event: %+v", cap.evs[0])
	}
	if cap.evs[1].Role != Role_ROLE_MODEL || cap.evs[1].Seq != 2 || cap.evs[1].Text != "hello" {
		t.Errorf("second event: %+v", cap.evs[1])
	}
}

// On RecvTranscript error nothing is published (we only tap successful lines).
func TestWrapTranscriberErrorNoEmit(t *testing.T) {
	cap := &capture{}
	inner := &fakeTranscriber{fakeModel: &fakeModel{}} // no lines -> immediate EOF
	w := Wrap(inner, cap, Meta{SessionID: "s1"})
	if _, err := w.(recvTranscripter).RecvTranscript(); err != io.EOF {
		t.Fatalf("want EOF, got %v", err)
	}
	if len(cap.evs) != 0 {
		t.Errorf("error path must not emit, got %d events", len(cap.evs))
	}
}
