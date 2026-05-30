// Package model defines the provider-agnostic interface the WebRTC bridge talks
// to, plus one implementation per streaming LLM provider.
//
// Contract: all audio crossing this interface is mono signed-16 PCM at 48kHz
// (WebRTC's native Opus rate). Each provider resamples to/from its own rate
// internally, so the bridge never has to know a provider's audio format.
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
