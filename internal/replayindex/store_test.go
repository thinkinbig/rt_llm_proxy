package replayindex

import (
	"testing"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/sidechannel"
)

func TestStoreIngestAndQuery(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := NewStore(Config{
		MaxSessions:     10,
		MaxLinesPerSess: 10,
		SessionTTL:      time.Hour,
		Now:             func() time.Time { return now },
	})

	s.Ingest(&sidechannel.TranscriptEvent{
		SessionId: "s1", UserId: "alice", Provider: "gemini",
		Seq: 1, Role: sidechannel.Role_ROLE_USER, Text: "hi",
	})
	s.Ingest(&sidechannel.TranscriptEvent{
		SessionId: "s1", UserId: "alice", Provider: "gemini",
		Seq: 2, Role: sidechannel.Role_ROLE_MODEL, Text: "hey",
	})
	s.Ingest(&sidechannel.TranscriptEvent{
		SessionId: "s1", UserId: "alice", Provider: "gemini",
		Seq: 2, Role: sidechannel.Role_ROLE_MODEL, Text: "dup",
	})

	got := s.Query("s1", "alice", "gemini", 1, 10)
	if len(got) != 1 || got[0].GetSeq() != 2 || got[0].GetText() != "hey" {
		t.Fatalf("got %+v", got)
	}
}

func TestStoreQueryOwnership(t *testing.T) {
	s := NewStore(Config{})
	s.Ingest(&sidechannel.TranscriptEvent{
		SessionId: "s1", UserId: "alice", Provider: "gemini",
		Seq: 1, Text: "hi",
	})
	if evs := s.Query("s1", "bob", "gemini", 0, 10); len(evs) != 0 {
		t.Fatalf("wrong user: %+v", evs)
	}
	if evs := s.Query("s1", "alice", "doubao", 0, 10); len(evs) != 0 {
		t.Fatalf("wrong provider: %+v", evs)
	}
}

func TestStoreIgnoresAnonymous(t *testing.T) {
	s := NewStore(Config{})
	s.Ingest(&sidechannel.TranscriptEvent{SessionId: "s1", Seq: 1, Text: "hi"})
	if sessions, events := s.Stats(); sessions != 0 || events != 0 {
		t.Fatalf("anonymous ingest should be ignored: sessions=%d events=%d", sessions, events)
	}
}

func TestStoreLineCap(t *testing.T) {
	s := NewStore(Config{MaxLinesPerSess: 2})
	for i := uint64(1); i <= 3; i++ {
		s.Ingest(&sidechannel.TranscriptEvent{
			SessionId: "s1", UserId: "alice", Provider: "gemini",
			Seq: i, Text: "x",
		})
	}
	got := s.Query("s1", "alice", "gemini", 0, 10)
	if len(got) != 2 || got[0].GetSeq() != 2 || got[1].GetSeq() != 3 {
		t.Fatalf("want newest two lines, got %+v", got)
	}
}
