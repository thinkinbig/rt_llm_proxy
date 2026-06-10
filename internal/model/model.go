// Package model is the provider-agnostic Model seam. Concrete adapters live in
// subpackages (gemini, doubao); the bridge and cmd/proxy depend only on Model.
//
// Contract: all audio crossing Model is mono signed-16 PCM at 48kHz (WebRTC's
// native Opus rate). Each adapter resamples to/from its own wire format internally.
package model

// Transcript is one speech-to-text turn returned by a model that supports
// transcription. Role is "user" or "model". Seq is assigned by the Bridge
// recorder, not by the provider.
type Transcript struct {
	Role string
	Text string
}

// RestoredTurn is one prior conversation turn replayed into a model on
// reconnect. Role is "user" or "model".
type RestoredTurn struct {
	Role string
	Text string
}

// ContextRestorer is an optional Model capability: providers that can be seeded
// with prior conversation turns implement RestoreContext. On reconnect the
// Bridge type-asserts to this and calls it with the restored transcript before
// the session goes live, so the model resumes with dialogue context instead of
// starting amnesiac. Providers that cannot accept injected context — e.g. a pure
// speech-to-speech model with no text-in path — simply do not implement it.
type ContextRestorer interface {
	RestoreContext(turns []RestoredTurn) error
}

// Transcriber is an optional Model capability: providers that surface STT
// implement RecvTranscript. The Bridge type-asserts to this to forward
// transcripts to the browser data channel.
type Transcriber interface {
	RecvTranscript() (Transcript, error)
	// RecvInterrupted checks if the model detected user speech interruption (barge-in).
	// Returns (true, nil) if interrupted, (false, nil) if not, or (_, err) on error.
	RecvInterrupted() (bool, error)
}

type Model interface {
	// SendAudio sends a chunk of microphone PCM (mono s16, 48kHz).
	SendAudio(pcm []int16) error
	// SendText sends a text/control message (from the WebRTC data channel).
	SendText(text string) error
	// Recv blocks for the next chunk of model audio (mono s16, 48kHz).
	// Returns io.EOF when the session is closed.
	Recv() ([]int16, error)
	Close() error
	// SupportsInterruption returns true if the model natively supports VAD-based interruption.
	SupportsInterruption() bool
	// HandleInterrupted is called when user speech is detected during model reply.
	// Implementations should cancel in-flight generation and drain queued audio.
	HandleInterrupted() error
}
