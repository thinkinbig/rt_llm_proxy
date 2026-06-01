package offer

import (
	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/rtc"
)

// MediaHub starts Bridge media for a resolved session and supplies reconnect
// lookup for ResolveReplay.
type MediaHub interface {
	SessionLookup
	Serve(offerSDP string, m model.Model, info rtc.SessionInfo) (string, error)
}
