package offer

import (
	"context"
	"testing"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
	"github.com/thinkinbig/rt-llm-proxy/internal/sidechannel"
	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

func TestParseProvider(t *testing.T) {
	tests := []struct {
		in   string
		want string
		err  bool
	}{
		{"", "gemini", false},
		{"gemini", "gemini", false},
		{"doubao", "doubao", false},
		{"loopback", "loopback", false},
		{"gpt", "", true},
	}
	for _, tc := range tests {
		got, err := ParseProvider(tc.in)
		if tc.err {
			if err == nil {
				t.Errorf("ParseProvider(%q) want error", tc.in)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("ParseProvider(%q) = %q, %v want %q", tc.in, got, err, tc.want)
		}
	}
}

func TestParseReplayHeaders(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		h, err := ParseReplayHeaders("", "", "")
		if err != nil || h.Requested {
			t.Fatalf("got %+v, %v", h, err)
		}
	})

	t.Run("unsupported version", func(t *testing.T) {
		_, err := ParseReplayHeaders("s1", "1", "2")
		if _, ok := err.(*ProtocolInvalidError); !ok {
			t.Fatalf("want ProtocolInvalidError, got %v", err)
		}
	})

	t.Run("invalid seq", func(t *testing.T) {
		_, err := ParseReplayHeaders("s1", "nope", "1")
		if _, ok := err.(*ProtocolInvalidError); !ok {
			t.Fatalf("want ProtocolInvalidError, got %v", err)
		}
	})

	t.Run("incomplete", func(t *testing.T) {
		h, err := ParseReplayHeaders("s1", "", "")
		if err != nil || !h.Incomplete || !h.Requested {
			t.Fatalf("got %+v, %v", h, err)
		}
	})

	t.Run("valid", func(t *testing.T) {
		h, err := ParseReplayHeaders(" s1 ", " 3 ", "1")
		if err != nil || h.SessionID != "s1" || h.LastSeq != 3 {
			t.Fatalf("got %+v, %v", h, err)
		}
	})
}

type fakeStore struct {
	provider string
	maxSeq   uint64
	known    bool
	full     []transcript.Line
	missing  []transcript.Line
	startSeq uint64
	resumeOK bool
}

func (f *fakeStore) SessionState(id identity.SessionID, userID identity.UserID) (string, uint64, bool) {
	if !f.known {
		return "", 0, false
	}
	return f.provider, f.maxSeq, true
}

func (f *fakeStore) Resume(id identity.SessionID, userID identity.UserID, provider string, afterSeq uint64) ([]transcript.Line, []transcript.Line, uint64, bool) {
	if !f.resumeOK {
		return nil, nil, 0, false
	}
	return f.full, f.missing, f.startSeq, true
}

type fakeReplayer struct {
	evs []*sidechannel.TranscriptEvent
	err error
}

func (f *fakeReplayer) Replay(context.Context, identity.SessionID, identity.UserID, string, uint64, int) ([]*sidechannel.TranscriptEvent, error) {
	return f.evs, f.err
}

func TestResolveReplayMemoryHit(t *testing.T) {
	store := &fakeStore{
		known: true, provider: "gemini", maxSeq: 5, resumeOK: true,
		full:     []transcript.Line{{Seq: 1, Role: "user", Text: "hi"}},
		missing:  []transcript.Line{{Seq: 2, Role: "model", Text: "hey"}},
		startSeq: 2,
	}
	out, err := ResolveReplay(context.Background(), "gemini", "alice",
		ReplayHeaders{Requested: true, SessionID: "s1", LastSeq: 1},
		ReplayConfig{Enabled: true}, store, nil, nil, "new-id")
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != "memory_hit" || out.SessionID != "s1" || out.StartSeq != 2 || len(out.ReplayLines) != 1 {
		t.Fatalf("got %+v", out)
	}
}

func TestResolveReplayProviderMismatch(t *testing.T) {
	store := &fakeStore{known: true, provider: "gemini", maxSeq: 5}
	out, err := ResolveReplay(context.Background(), "doubao", "alice",
		ReplayHeaders{Requested: true, SessionID: "s1", LastSeq: 1},
		ReplayConfig{Enabled: true}, store, nil, nil, "new-id")
	if err != nil || out.Status != "miss" || out.SessionID != "new-id" {
		t.Fatalf("got %+v, %v", out, err)
	}
}

func TestResolveReplaySeqTooHigh(t *testing.T) {
	store := &fakeStore{known: true, provider: "gemini", maxSeq: 2}
	_, err := ResolveReplay(context.Background(), "gemini", "alice",
		ReplayHeaders{Requested: true, SessionID: "s1", LastSeq: 5},
		ReplayConfig{Enabled: true}, store, nil, nil, "new-id")
	if _, ok := err.(*ProtocolInvalidError); !ok {
		t.Fatalf("want ProtocolInvalidError, got %v", err)
	}
}

func TestResolveReplayAnonymousMiss(t *testing.T) {
	// An anonymous caller (userID == "") must never resume a session, even when
	// the store would otherwise have a hit — anon sessions are non-reconnectable.
	store := &fakeStore{
		known: true, provider: "gemini", maxSeq: 5, resumeOK: true,
		full:    []transcript.Line{{Seq: 1, Role: "user", Text: "hi"}},
		missing: []transcript.Line{{Seq: 2, Role: "model", Text: "hey"}},
	}
	out, err := ResolveReplay(context.Background(), "gemini", "",
		ReplayHeaders{Requested: true, SessionID: "s1", LastSeq: 1},
		ReplayConfig{Enabled: true}, store, nil, nil, "new-id")
	if err != nil || out.Status != "miss" || out.SessionID != "new-id" || len(out.ReplayLines) != 0 {
		t.Fatalf("anon reconnect should miss: got %+v, %v", out, err)
	}
}

func TestResolveReplayIndexHit(t *testing.T) {
	index := &fakeReplayer{evs: []*sidechannel.TranscriptEvent{
		{Seq: 2, Role: sidechannel.Role_ROLE_MODEL, Text: "hey"},
	}}
	out, err := ResolveReplay(context.Background(), "gemini", "alice",
		ReplayHeaders{Requested: true, SessionID: "s1", LastSeq: 1},
		ReplayConfig{Enabled: true}, &fakeStore{}, index, nil, "new-id")
	if err != nil || out.Status != "index_hit" || out.StartSeq != 2 || len(out.ReplayLines) != 1 {
		t.Fatalf("got %+v, %v", out, err)
	}
}
