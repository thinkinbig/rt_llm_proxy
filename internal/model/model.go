// Package model is the provider-agnostic Model seam. Concrete adapters live in
// subpackages (gemini, doubao); the bridge and cmd/proxy depend only on Model.
//
// Contract: all audio crossing Model is mono signed-16 PCM at 48kHz (WebRTC's
// native Opus rate). Each adapter resamples to/from its own wire format internally.
package model

import "encoding/json"

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

// SessionParams carries per-session knobs supplied at construction, distinct
// from the process-global provider config. SystemSuffix is appended to the
// provider's system prompt for this session only (e.g. a per-user "listener
// brief" from an upstream memory service); it is injected as system instruction,
// never as a dialogue turn, so it cannot loop back into the transcript.
type SessionParams struct {
	SystemSuffix string
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

// ToolCall is a function-call request emitted by a model that supports tool use.
// Args is the raw JSON arguments object the model produced.
type ToolCall struct {
	ID   string
	Name string
	Args json.RawMessage
}

// ToolResult is the outcome of executing a ToolCall, returned to the model.
// Response is a JSON object the model consumes as the function's return value.
type ToolResult struct {
	ID       string
	Name     string
	Response json.RawMessage
}

// ToolDispatcher is an optional Model capability: providers that support
// function calling implement it. The Bridge type-asserts to this and, when
// present, forwards each tool call to the browser data channel and returns the
// browser's result to the model. Providers without native tool calling (e.g.
// the Doubao direct protocol) simply do not implement it.
type ToolDispatcher interface {
	// RecvToolCall blocks for the next tool call; returns io.EOF when closed.
	RecvToolCall() (ToolCall, error)
	// SendToolResult returns a function result to the model.
	SendToolResult(ToolResult) error
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
