package offer

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
)

type fakeMemory struct {
	brief    string
	err      error
	lastUser identity.UserID
}

func (f *fakeMemory) ListenerBrief(_ context.Context, userID identity.UserID) (string, error) {
	f.lastUser = userID
	return f.brief, f.err
}

func TestResolveBriefNoProviderUsesHeader(t *testing.T) {
	header := base64.StdEncoding.EncodeToString([]byte("dev brief"))
	if got := resolveBrief(context.Background(), nil, "alice", header); got != "dev brief" {
		t.Fatalf("no provider should use header, got %q", got)
	}
}

func TestResolveBriefProviderAuthoritative(t *testing.T) {
	mp := &fakeMemory{brief: "from profile"}
	// Even with a (forged) header present, the provider wins.
	header := base64.StdEncoding.EncodeToString([]byte("forged"))
	got := resolveBrief(context.Background(), mp, "alice", header)
	if got != "from profile" {
		t.Fatalf("provider should be authoritative, got %q", got)
	}
	if mp.lastUser != "alice" {
		t.Fatalf("provider queried with %q, want alice", mp.lastUser)
	}
}

func TestResolveBriefAnonymousSkipped(t *testing.T) {
	mp := &fakeMemory{brief: "should not be used"}
	if got := resolveBrief(context.Background(), mp, identity.UserID(""), ""); got != "" {
		t.Fatalf("anonymous should get no brief, got %q", got)
	}
	if mp.lastUser != "" {
		t.Fatal("provider must not be queried for anonymous user")
	}
}

func TestResolveBriefProviderErrorEmpty(t *testing.T) {
	mp := &fakeMemory{err: errors.New("profile down")}
	if got := resolveBrief(context.Background(), mp, "alice", "ignored"); got != "" {
		t.Fatalf("provider error should yield empty, got %q", got)
	}
}
