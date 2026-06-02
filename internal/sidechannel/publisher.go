// Package sidechannel taps conversation transcripts off the media bridge and
// publishes them, business-agnostically, to a message queue for personalization
// ("旁路个性化推荐"). It is strictly off the real-time media path: a slow or
// broken backend degrades the feature, never the call (ARCHITECTURE §3.3).
//
// Publisher seam lets the core server stay free of any specific broker; the
// Tap listener (tap.go) depends only on Publisher. A nil/Nop
// publisher disables the side-channel, mirroring how an empty -redis disables
// rate limiting.
package sidechannel

import (
	"log"
)

// Publisher accepts transcript events for asynchronous delivery. Publish must
// not block the caller: the media bridge calls it on the hot text path, so a
// slow backend has to be absorbed (bounded buffer + drop), never back-pressured.
type Publisher interface {
	Publish(ev *TranscriptEvent)
	Close() error
}

// Nop discards every event. It is the default when no broker is configured.
type Nop struct{}

func (Nop) Publish(*TranscriptEvent) {}
func (Nop) Close() error             { return nil }

// Stdout logs each event — the runnable "consumer-side" demo view without a
// broker, for teaching and local debugging.
type Stdout struct{}

func (Stdout) Publish(ev *TranscriptEvent) {
	log.Printf("sidechannel: session=%s user=%q seq=%d role=%s text=%q",
		ev.GetSessionId(), ev.GetUserId(), ev.GetSeq(), ev.GetRole(), ev.GetText())
}
func (Stdout) Close() error { return nil }
