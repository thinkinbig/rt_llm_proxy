package rtc

import (
	"testing"

	"github.com/pion/webrtc/v4"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/loopback"
	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

func TestSessionScopeAbortUncommitted(t *testing.T) {
	h, err := NewHub("")
	if err != nil {
		t.Fatal(err)
	}
	pc, err := h.api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	m := loopback.New()
	sess := &session{id: identity.SessionID("s1")}
	scope := newSessionScope(h, pc, m, sess)

	scope.abortIfUncommitted()
	if scope.committed {
		t.Fatal("scope should not be committed")
	}
	if h.Count() != 0 {
		t.Fatalf("hub count = %d, want 0 (uncommitted close must not register)", h.Count())
	}

	scope.abortIfUncommitted() // idempotent
}

func TestSessionScopeCommitThenClose(t *testing.T) {
	h, err := NewHub("")
	if err != nil {
		t.Fatal(err)
	}
	pc, err := h.api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	m := loopback.New()
	sess := &session{
		id:  identity.SessionID("s2"),
		rec: transcript.NewRecorder(0, nil, 8, transcript.SessionMeta{SessionID: "s2"}, nil),
	}
	scope := newSessionScope(h, pc, m, sess)

	scope.commit()
	if h.Count() != 1 {
		t.Fatalf("hub count = %d, want 1", h.Count())
	}

	scope.Close()
	if h.Count() != 0 {
		t.Fatalf("hub count after close = %d, want 0", h.Count())
	}
	scope.Close() // idempotent
}
