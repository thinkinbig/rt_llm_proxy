package rtc

import (
	"testing"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

func newTestHub() *Hub {
	return &Hub{
		conns:    map[identity.SessionID]*session{},
		archives: map[identity.SessionID]sessionArchive{},
	}
}

func TestResumeOwnershipAndExpiry(t *testing.T) {
	hist := []transcript.Line{{Seq: 1, Role: "user", Text: "a"}, {Seq: 2, Role: "model", Text: "b"}}
	fresh := func() sessionArchive {
		return sessionArchive{provider: "gemini", userID: "alice", history: hist, maxSeq: 2, expiry: time.Now().Add(time.Minute)}
	}

	t.Run("owner resumes", func(t *testing.T) {
		h := newTestHub()
		h.archives["s1"] = fresh()
		full, replay, startSeq, ok := h.Resume("s1", "alice", "gemini", 1)
		if !ok || startSeq != 2 || len(full) != 2 || len(replay) != 1 || replay[0].Seq != 2 {
			t.Fatalf("owner resume = %v %v %d %v", full, replay, startSeq, ok)
		}
	})

	t.Run("other user rejected", func(t *testing.T) {
		h := newTestHub()
		h.archives["s1"] = fresh()
		if _, _, _, ok := h.Resume("s1", "bob", "gemini", 1); ok {
			t.Fatal("cross-user resume must be rejected")
		}
	})

	t.Run("anonymous rejected", func(t *testing.T) {
		h := newTestHub()
		h.archives["s1"] = fresh()
		if _, _, _, ok := h.Resume("s1", "", "gemini", 1); ok {
			t.Fatal("anonymous resume must be rejected")
		}
	})

	t.Run("provider mismatch rejected", func(t *testing.T) {
		h := newTestHub()
		h.archives["s1"] = fresh()
		if _, _, _, ok := h.Resume("s1", "alice", "doubao", 1); ok {
			t.Fatal("provider mismatch must be rejected")
		}
	})

	t.Run("expired archive rejected", func(t *testing.T) {
		h := newTestHub()
		arch := fresh()
		arch.expiry = time.Now().Add(-time.Second)
		h.archives["s1"] = arch
		if _, _, _, ok := h.Resume("s1", "alice", "gemini", 1); ok {
			t.Fatal("expired archive must be rejected")
		}
		if _, _, ok := h.SessionState("s1", "alice"); ok {
			t.Fatal("expired archive must not report session state")
		}
	})
}

func TestSessionRecordNotifiesListener(t *testing.T) {
	var got []transcript.Line
	l := listenerFunc(func(_ transcript.SessionMeta, line transcript.Line) {
		got = append(got, line)
	})
	meta := transcript.SessionMeta{SessionID: "s1", Provider: "gemini"}
	rec := transcript.NewRecorder(0, nil, 256, meta, l)

	line := rec.Record("user", "hello")
	if line.Seq != 1 || line.Role != "user" || line.Text != "hello" {
		t.Fatalf("record = %+v", line)
	}
	if len(got) != 1 || got[0] != line {
		t.Fatalf("listener got %+v", got)
	}
}

func TestRecorderSnapshotAfterSeq(t *testing.T) {
	meta := transcript.SessionMeta{SessionID: "s1"}
	rec := transcript.NewRecorder(0, nil, 256, meta, nil)
	rec.Record("user", "a")
	rec.Record("model", "b")
	rec.Record("user", "c")

	snap := rec.Snapshot(1)
	if len(snap) != 2 || snap[0].Seq != 2 || snap[1].Seq != 3 {
		t.Fatalf("snapshot = %+v", snap)
	}
}

type listenerFunc func(transcript.SessionMeta, transcript.Line)

func (f listenerFunc) OnLine(m transcript.SessionMeta, l transcript.Line) { f(m, l) }
