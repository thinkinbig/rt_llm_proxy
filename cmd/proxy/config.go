package main

import (
	"flag"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/model/gemini"
	"github.com/thinkinbig/rt-llm-proxy/internal/modelcb"
)

type runConfig struct {
	Addr      string
	AdminAddr string

	RedisAddr string
	RLMax     int
	RLWindow  time.Duration
	TrustProxy bool

	SidechannelMode string
	KafkaBrokers    string
	KafkaTopic      string

	ReplayURL     string
	ReplayTimeout time.Duration
	ReplayLimit   int

	ModelCBEnable bool
	ModelCB       modelCBConfigArgs

	OpusComplexity int
	AdaptiveMode   string

	// Cascade stage endpoints (used when ?model=cascade).
	CascadeWhisperURL    string
	CascadeLLMURL        string
	CascadeLLMModel      string
	CascadeTTSURL        string
	CascadeTTSSpeaker    string
	CascadeTTSLang       string
	CascadeTurnDetectURL string
	CascadeSystem        string

	// Provider behavior (config-file only; no CLI flags). Empty fields leave the
	// provider's own defaults in place.
	GeminiSystemPrompt   string
	GeminiTools          []gemini.FunctionDeclaration
	DoubaoModelVersion   string
	DoubaoBotName        string
	DoubaoSystemRole     string
	DoubaoSpeakingStyle  string
	DoubaoVoice          string
	DoubaoASRTwopass     bool
	DoubaoASREndSmoothMs int
	DoubaoHotwords       []string
}

// parseFlags defines and parses CLI flags. It returns the assembled runConfig,
// the set of flag names explicitly provided on the command line (so the config
// file knows which fields it must not override), and the config file path.
func parseFlags() (runConfig, map[string]bool, string) {
	configPath := flag.String("config", "proxy.yaml", "config file path (skipped if absent)")
	addr := flag.String("addr", ":8080", "listen address")
	redisAddr := flag.String("redis", "", "redis address for rate limiting (empty = disabled)")
	rlMax := flag.Int("rl-max", 10, "max sessions per client per window")
	rlWindow := flag.Duration("rl-window", time.Minute, "rate limit window")
	trustProxy := flag.Bool("trust-proxy", false, "trust X-Forwarded-For for the rate-limit client IP (enable only behind a reverse proxy that sets it)")
	scMode := flag.String("sidechannel", "off", "transcript side-channel: off|stdout|kafka")
	kafkaBrokers := flag.String("kafka", "", "kafka seed brokers (csv) for -sidechannel=kafka")
	kafkaTopic := flag.String("kafka-topic", "transcripts", "kafka topic for transcript events")
	replayURL := flag.String("replay-url", "", "replay-index service base URL (enables cross-node reconnect replay when set)")
	replayTimeout := flag.Duration("replay-timeout", 300*time.Millisecond, "replay timeout budget when -replay-url is set")
	replayLimit := flag.Int("replay-limit", 100, "max replay transcript lines on reconnect")
	modelCBEnable := flag.Bool("model-cb", true, "enable model connect circuit breaker")
	modelCBOpenAfter := flag.Int("model-cb-open-after", 5, "consecutive failures before opening model circuit")
	modelCBOpenFor := flag.Duration("model-cb-open-for", 30*time.Second, "open-state duration for transient model failures")
	modelCBHalfOpenSuccess := flag.Int("model-cb-half-open-success", 3, "successful half-open probes required to close model circuit")
	modelCBAuthOpenFor := flag.Duration("model-cb-auth-open-for", 5*time.Minute, "open-state duration for auth failures (401/403)")
	modelCBOpenAfterGemini := flag.Int("model-cb-open-after-gemini", 0, "override model-cb-open-after for gemini (0 = default)")
	modelCBOpenForGemini := flag.Duration("model-cb-open-for-gemini", 0, "override model-cb-open-for for gemini (0 = default)")
	modelCBHalfOpenSuccessGemini := flag.Int("model-cb-half-open-success-gemini", 0, "override model-cb-half-open-success for gemini (0 = default)")
	modelCBAuthOpenForGemini := flag.Duration("model-cb-auth-open-for-gemini", 0, "override model-cb-auth-open-for for gemini (0 = default)")
	modelCBOpenAfterDoubao := flag.Int("model-cb-open-after-doubao", 0, "override model-cb-open-after for doubao (0 = default)")
	modelCBOpenForDoubao := flag.Duration("model-cb-open-for-doubao", 0, "override model-cb-open-for for doubao (0 = default)")
	modelCBHalfOpenSuccessDoubao := flag.Int("model-cb-half-open-success-doubao", 0, "override model-cb-half-open-success for doubao (0 = default)")
	modelCBAuthOpenForDoubao := flag.Duration("model-cb-auth-open-for-doubao", 0, "override model-cb-auth-open-for for doubao (0 = default)")
	adminAddr := flag.String("admin", "", "admin listen address for /stats + /debug/pprof (empty = off)")
	opusComplexity := flag.Int("opus-complexity", -1, "Opus encoder complexity 0-10 (-1 = libopus default; lower = less CPU)")
	adaptiveMode := flag.String("adaptive", "off", "adaptive Opus complexity under load: off|sessions|drift")
	cascadeWhisperURL := flag.String("cascade-whisper", "ws://localhost:9000/v1/audio/transcriptions/streaming", "faster-whisper-server WebSocket URL for cascade ASR")
	cascadeLLMURL := flag.String("cascade-llm", "http://localhost:8000", "vLLM base URL for cascade LLM")
	cascadeLLMModel := flag.String("cascade-llm-model", "Qwen3.5-9B", "model name served by vLLM")
	cascadeTTSURL := flag.String("cascade-tts", "http://localhost:8020", "xtts-streaming-server base URL for cascade TTS")
	cascadeTTSSpeaker := flag.String("cascade-tts-speaker", "", "XTTS studio speaker name (empty = first available)")
	cascadeTTSLang := flag.String("cascade-tts-lang", "en", "XTTS language code (e.g. en, zh-cn)")
	cascadeTurnDetectURL := flag.String("cascade-turndetect", "", "turn-detect sidecar URL (empty = fire LLM immediately after ASR final)")
	cascadeSystem := flag.String("cascade-system", "You are a helpful voice assistant.", "system prompt for cascade LLM")
	flag.Parse()

	cfg := runConfig{
		Addr:            *addr,
		AdminAddr:       *adminAddr,
		RedisAddr:       *redisAddr,
		RLMax:           *rlMax,
		RLWindow:        *rlWindow,
		TrustProxy:      *trustProxy,
		SidechannelMode: *scMode,
		KafkaBrokers:    *kafkaBrokers,
		KafkaTopic:      *kafkaTopic,
		ReplayURL:       *replayURL,
		ReplayTimeout:   *replayTimeout,
		ReplayLimit:     *replayLimit,
		ModelCBEnable:   *modelCBEnable,
		ModelCB: modelCBConfigArgs{
			OpenAfter:       *modelCBOpenAfter,
			OpenFor:         *modelCBOpenFor,
			HalfOpenSuccess: *modelCBHalfOpenSuccess,
			AuthOpenFor:     *modelCBAuthOpenFor,
			Gemini: modelcb.Config{
				OpenAfter:       *modelCBOpenAfterGemini,
				OpenFor:         *modelCBOpenForGemini,
				HalfOpenSuccess: *modelCBHalfOpenSuccessGemini,
				AuthOpenFor:     *modelCBAuthOpenForGemini,
			},
			Doubao: modelcb.Config{
				OpenAfter:       *modelCBOpenAfterDoubao,
				OpenFor:         *modelCBOpenForDoubao,
				HalfOpenSuccess: *modelCBHalfOpenSuccessDoubao,
				AuthOpenFor:     *modelCBAuthOpenForDoubao,
			},
		},
		OpusComplexity:    *opusComplexity,
		AdaptiveMode:      *adaptiveMode,
		CascadeWhisperURL:    *cascadeWhisperURL,
		CascadeLLMURL:        *cascadeLLMURL,
		CascadeLLMModel:      *cascadeLLMModel,
		CascadeTTSURL:        *cascadeTTSURL,
		CascadeTTSSpeaker:    *cascadeTTSSpeaker,
		CascadeTTSLang:       *cascadeTTSLang,
		CascadeTurnDetectURL: *cascadeTurnDetectURL,
		CascadeSystem:        *cascadeSystem,
	}

	set := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { set[f.Name] = true })
	return cfg, set, *configPath
}
