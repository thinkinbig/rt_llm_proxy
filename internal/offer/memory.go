package offer

import (
	"context"
	"log"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
)

// memoryBudget bounds the per-user memory lookup so a slow upstream cannot stall
// session start (the lookup runs before media flows).
const memoryBudget = 300 * time.Millisecond

// MemoryProvider fetches a per-user "listener brief" — a short text injected as
// system instruction at session start so the model is proactively personalized
// (e.g. "this listener likes Jay Chou, is studying"). Implementations talk to
// the upstream memory service (Profile/mem0) and must only return memory for the
// given userID. Optional: when unset, the proxy falls back to the dev
// X-Listener-Brief header.
type MemoryProvider interface {
	ListenerBrief(ctx context.Context, userID identity.UserID) (string, error)
}

// resolveBrief picks the per-session system suffix. A configured MemoryProvider
// is authoritative — a forged X-Listener-Brief header is ignored once memory is
// wired. With no provider (dev), the header is used. Anonymous users get no
// memory: there is no stable identity to key a per-user brief on.
func resolveBrief(ctx context.Context, mp MemoryProvider, userID identity.UserID, header string) string {
	if mp == nil {
		return decodeListenerBrief(header)
	}
	if userID.Anonymous() {
		return ""
	}
	mctx, cancel := context.WithTimeout(ctx, memoryBudget)
	defer cancel()
	brief, err := mp.ListenerBrief(mctx, userID)
	if err != nil {
		log.Printf("offer: memory lookup: %v", err)
		return ""
	}
	return brief
}
