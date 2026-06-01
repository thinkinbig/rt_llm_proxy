package offer

import (
	"context"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade/fakestage"
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
	case "cascade":
		// Cascaded ASR->LLM->TTS pipeline. Stages are network-free fakes for now;
		// swap in real managed-API impls via cascade.Config when ready.
		return cascade.New(ctx, cascade.Config{
			ASR:    fakestage.NewASR(),
			LLM:    fakestage.NewLLM(),
			TTS:    fakestage.NewTTS(),
			System: "You are a demo DJ.",
		})
	default:
		return gemini.New(ctx)
	}
}
