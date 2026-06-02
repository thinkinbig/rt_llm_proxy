package cascade

import (
	"context"
	"time"
)

// AudioSource is the output-mix seam. Implement it to inject any audio into
// the outbound stream — a real music track, a mixed voice+music feed, etc.
//
// Read returns the next PCM chunk (mono s16, 48kHz). It must return io.EOF
// when the source is exhausted so Cascade can fall back to TTS audio.
// Cascade calls Close when the source is replaced or the session ends.
type AudioSource interface {
	Read() ([]int16, error)
	Close() error
}


// Stage audio contract mirrors model.Model: mono signed-16 PCM at 48kHz. Each
// stage implementation resamples to/from its provider wire rate internally,
// exactly like the gemini/doubao adapters do.

// Message is one conversation turn fed to the LLM. Role is "system"|"user"|"model".
//
// This slice is also where the P3 context-injection seam will live: a downstream
// (RAG / tools) prepends retrieved context here before generation. Out of scope
// for the MVP — the cascade just carries system + alternating user/model turns.
type Message struct {
	Role string
	Text string
}

// ASREventKind tags a recognition event from the ASR stage.
type ASREventKind int

const (
	// ASRPartial is an interim transcript; Text is the cumulative text of the
	// current (still in-progress) utterance.
	ASRPartial ASREventKind = iota
	// ASRFinal marks end-of-utterance — the turn boundary that fires the LLM.
	// Text is the finalized user turn.
	ASRFinal
	// ASRSpeechStarted signals the user began speaking; the barge-in trigger.
	// Text is empty.
	ASRSpeechStarted
)

type ASREvent struct {
	Kind ASREventKind
	Text string
}

// ASR is a streaming speech-to-text stage. It is stateful per session: feed mic
// PCM with Write and read recognition events from Events until Close. Endpointing
// (deciding when a turn ends) is the provider's job, surfaced as ASRFinal.
type ASR interface {
	Write(pcm []int16) error // mono s16, 48kHz
	Events() <-chan ASREvent
	Close() error
}

// LLM is a streaming text-generation stage. It is stateless per call: the full
// conversation is passed in, which keeps managed chat APIs trivially swappable.
type LLM interface {
	// Generate streams reply text deltas; the channel closes at end of turn.
	// Cancelling ctx aborts mid-generation (barge-in).
	Generate(ctx context.Context, history []Message) (<-chan string, error)
	Close() error
}

// TTS is a streaming text-to-speech stage.
type TTS interface {
	// Synthesize streams PCM (mono s16, 48kHz) for text; the channel closes when
	// synthesis completes. Cancelling ctx aborts (barge-in).
	Synthesize(ctx context.Context, text string) (<-chan []int16, error)
	Close() error
}

// QuickSynthesizer is an optional TTS capability. A stage that implements it
// can synthesize the quick answer (the first segment of a turn) with settings
// tuned for minimum time-to-first-audio, trading a little quality/throughput.
// Stages that don't implement it transparently fall back to Synthesize.
type QuickSynthesizer interface {
	SynthesizeQuick(ctx context.Context, text string) (<-chan []int16, error)
}

// TurnDetector decides how long to wait after an ASR final before committing
// the turn to the LLM. A complete sentence gets a short pause; a fragment gets
// a longer one so the user has time to continue.
//
// SuggestedPause must return quickly (it is called on the hot path). Use
// NopTurnDetector when no sidecar is configured.
type TurnDetector interface {
	SuggestedPause(ctx context.Context, text string) time.Duration
}

// NopTurnDetector fires the LLM immediately after ASR final (legacy behaviour).
// It is the default when Config.TurnDetect is nil. Sidecar-backed detectors
// live in the turndetect subpackage.
type NopTurnDetector struct{}

func (NopTurnDetector) SuggestedPause(_ context.Context, _ string) time.Duration {
	return 0
}
