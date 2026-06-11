package offer

import (
	"context"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade/asr"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade/llm"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade/tts"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade/turndetect"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/doubao"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/gemini"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/loopback"
)

// CascadeConfig holds the self-hosted service URLs for the cascade pipeline.
// Populated from CLI flags and passed in at startup.
type CascadeConfig struct {
	WhisperURL    string // streaming ASR WebSocket endpoint (RealtimeSTT sidecar)
	LLMURL        string // vLLM base URL
	LLMModel      string // model name served by vLLM
	TTSURL        string // xtts-streaming-server base URL
	TTSSpeaker    string // XTTS studio speaker name (empty = first available)
	TTSLang       string // XTTS language code (e.g. "en", "zh-cn")
	TurnDetectURL string // turn-detect sidecar (empty = fire immediately)
	System        string // system prompt
}

// ProdModelFactory connects real provider adapters for production wiring.
// Gemini and Doubao carry per-deployment behavior (persona, voice, ASR tuning)
// resolved at startup from flags and the config file; credentials still come
// from the environment inside each adapter.
type ProdModelFactory struct {
	Cascade CascadeConfig
	Gemini  gemini.Config
	Doubao  doubao.Config
}

func (f ProdModelFactory) New(ctx context.Context, provider string, history []model.RestoredTurn) (model.Model, error) {
	switch provider {
	case "doubao":
		return doubao.NewWithConfig(ctx, f.Doubao, history)
	case "loopback":
		return loopback.New(), nil
	case "cascade":
		asrStage, err := asr.NewWhisper(f.Cascade.WhisperURL)
		if err != nil {
			return nil, err
		}
		ttsStage, err := tts.NewXTTSStream(f.Cascade.TTSURL, f.Cascade.TTSSpeaker, f.Cascade.TTSLang)
		if err != nil {
			asrStage.Close()
			return nil, err
		}
		var td cascade.TurnDetector = cascade.NopTurnDetector{}
		if f.Cascade.TurnDetectURL != "" {
			td = turndetect.NewHTTP(f.Cascade.TurnDetectURL)
		}
		return cascade.New(ctx, cascade.Config{
			ASR:        asrStage,
			LLM:        llm.New(f.Cascade.LLMURL, f.Cascade.LLMModel),
			TTS:        ttsStage,
			TurnDetect: td,
			System:     f.Cascade.System,
		})
	default:
		return gemini.NewWithConfig(ctx, f.Gemini)
	}
}
