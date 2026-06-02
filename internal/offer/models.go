package offer

import (
	"context"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/doubao"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/gemini"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/loopback"
)

// CascadeConfig holds the self-hosted service URLs for the cascade pipeline.
// Populated from CLI flags and passed in at startup.
type CascadeConfig struct {
	WhisperURL string // faster-whisper-server WebSocket endpoint
	LLMURL     string // vLLM base URL
	LLMModel   string // model name served by vLLM
	TTSURL     string // Coqui TTS server base URL
	System     string // system prompt
}

// ProdModelFactory connects real provider adapters for production wiring.
type ProdModelFactory struct {
	Cascade CascadeConfig
}

func (f ProdModelFactory) New(ctx context.Context, provider string) (model.Model, error) {
	switch provider {
	case "doubao":
		return doubao.New(ctx)
	case "loopback":
		return loopback.New(), nil
	case "cascade":
		asr, err := cascade.NewWhisperASR(f.Cascade.WhisperURL)
		if err != nil {
			return nil, err
		}
		return cascade.New(ctx, cascade.Config{
			ASR:    asr,
			LLM:    cascade.NewDeepSeekLLM(f.Cascade.LLMURL, f.Cascade.LLMModel),
			TTS:    cascade.NewCoquiTTS(f.Cascade.TTSURL),
			System: f.Cascade.System,
		})
	default:
		return gemini.New(ctx)
	}
}
