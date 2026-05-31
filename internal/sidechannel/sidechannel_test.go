package sidechannel

import (
	"testing"

	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

type capture struct{ evs []*TranscriptEvent }

func (c *capture) Publish(ev *TranscriptEvent) { c.evs = append(c.evs, ev) }
func (c *capture) Close() error                { return nil }

func TestPartitionKey(t *testing.T) {
	if got := partitionKey(&TranscriptEvent{UserId: "alice", SessionId: "s1"}); got != "alice" {
		t.Errorf("known user: got %q want alice", got)
	}
	if got := partitionKey(&TranscriptEvent{UserId: "", SessionId: "s1"}); got != "s1" {
		t.Errorf("anonymous falls back to session id: got %q want s1", got)
	}
}

func TestTapNilPublisherIsNop(t *testing.T) {
	l := Tap(nil, transcript.SessionMeta{SessionID: "s1"})
	r := transcript.NewRecorder(0, nil, 16, transcript.SessionMeta{SessionID: "s1"}, l)
	r.Record("user", "hi")
}

func TestTapUsesBridgeSeq(t *testing.T) {
	cap := &capture{}
	meta := transcript.SessionMeta{SessionID: "s1", UserID: "alice", Provider: "gemini"}
	r := transcript.NewRecorder(2, nil, 16, meta, Tap(cap, meta))

	line := r.Record("model", "hello")
	if line.Seq != 3 {
		t.Fatalf("seq = %d want 3", line.Seq)
	}
	if len(cap.evs) != 1 {
		t.Fatalf("want 1 event, got %d", len(cap.evs))
	}
	ev := cap.evs[0]
	if ev.Seq != 3 || ev.Role != Role_ROLE_MODEL || ev.Text != "hello" ||
		ev.SessionId != "s1" || ev.UserId != "alice" || ev.Provider != "gemini" {
		t.Errorf("unexpected event: %+v", ev)
	}
}

func TestLineFromEvent(t *testing.T) {
	ev := &TranscriptEvent{Seq: 5, Role: Role_ROLE_USER, Text: "hi"}
	line := LineFromEvent(ev)
	if line != (transcript.Line{Seq: 5, Role: "user", Text: "hi"}) {
		t.Fatalf("got %+v", line)
	}
}

func TestTapReturnsListener(t *testing.T) {
	l := Tap(&capture{}, transcript.SessionMeta{SessionID: "s1"})
	if _, ok := l.(transcript.Listener); !ok {
		t.Fatal("Tap must return transcript.Listener")
	}
}
