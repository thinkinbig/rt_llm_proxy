package offer

import (
	"context"
	"fmt"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

// ModelFactory constructs a provider Model for an offer request. history carries
// the reconnect-restored conversation; adapters that seed context at session
// construction (e.g. doubao's dialog_context) consume it, while adapters that
// restore post-hoc via model.ContextRestorer ignore it (nil for fresh sessions).
type ModelFactory interface {
	New(ctx context.Context, provider string, history []model.RestoredTurn, params model.SessionParams) (model.Model, error)
}

// ParseProvider normalizes ?model= query values.
func ParseProvider(raw string) (string, error) {
	switch raw {
	case "gemini", "":
		return "gemini", nil
	case "doubao", "loopback", "cascade":
		return raw, nil
	default:
		return "", fmt.Errorf("unknown model %q", raw)
	}
}
