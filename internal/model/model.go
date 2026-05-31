// Package model is the provider-agnostic Model seam. Concrete adapters live in
// subpackages (gemini, doubao); the bridge and cmd/proxy depend only on Model.
//
// Contract: all audio crossing Model is mono signed-16 PCM at 48kHz (WebRTC's
// native Opus rate). Each adapter resamples to/from its own wire format internally.
package model

type Model interface {
	// SendAudio sends a chunk of microphone PCM (mono s16, 48kHz).
	SendAudio(pcm []int16) error
	// SendText sends a text/control message (from the WebRTC data channel).
	SendText(text string) error
	// Recv blocks for the next chunk of model audio (mono s16, 48kHz).
	// Returns io.EOF when the session is closed.
	Recv() ([]int16, error)
	Close() error
}
