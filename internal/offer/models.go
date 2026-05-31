package offer

import (
	"context"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/doubao"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/gemini"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/loopback"
)

// ProdModelFactory connects real provider adapters for production wiring.
type ProdModelFactory struct{}

func (ProdModelFactory) New(ctx context.Context, provider string) (model.Model, error) {
	switch provider {
	case "doubao":
		return doubao.New(ctx)
	case "loopback":
		return loopback.New(), nil
	default:
		return gemini.New(ctx)
	}
}
