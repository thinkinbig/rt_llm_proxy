// Package loopback is a fake model.Model for load testing: it never touches the
// network, so a benchmark measures the proxy itself (WebRTC termination, Opus
// encode/decode, pacing, goroutine scheduling, the side-channel) instead of a
// real provider's latency, cost, and rate limits. Select it with ?model=loopback.
package loopback

import (
	"fmt"
	"io"
	"math"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

const (
	rate            = audio.OpusRate   // 48kHz, the Model audio contract
	frameSamples    = rate * 20 / 1000 // 960 = 20ms
	toneHz          = 440              // a steady signal, not silence (DTX
	toneAmp         = 8000             // would suppress silent frames and
	transcriptEvery = 2 * time.Second  // under-test the encoder)
)

// Model emits a continuous sine tone as "model audio" and a synthetic
// transcript line every few seconds. Inbound audio/text is discarded.
//
// Recv deliberately does not pace itself: the bridge's writeOutbound Ticker
// paces output, calling Recv once per drained frame, so returning a frame
// immediately yields exactly real-time playback without a sleep here.
type Model struct {
	tone   []int16
	pos    int // Recv goroutine only
	n      int // RecvTranscript goroutine only
	closed chan struct{}
}

// New builds a loopback model with one second of precomputed sine.
func New() *Model {
	tone := make([]int16, rate)
	for i := range tone {
		tone[i] = int16(toneAmp * math.Sin(2*math.Pi*toneHz*float64(i)/float64(rate)))
	}
	return &Model{tone: tone, closed: make(chan struct{})}
}

func (m *Model) SendAudio([]int16) error { return nil }
func (m *Model) SendText(string) error   { return nil }

// Recv returns the next 20ms frame of the looping tone, or io.EOF once closed.
func (m *Model) Recv() ([]int16, error) {
	select {
	case <-m.closed:
		return nil, io.EOF
	default:
	}
	out := make([]int16, frameSamples)
	for i := range out {
		out[i] = m.tone[m.pos]
		m.pos = (m.pos + 1) % len(m.tone)
	}
	return out, nil
}

// RecvTranscript emits a synthetic transcript on a slow cadence so load tests
// exercise the transcript + side-channel path without flooding it.
func (m *Model) RecvTranscript() (model.Transcript, error) {
	select {
	case <-m.closed:
		return model.Transcript{}, io.EOF
	case <-time.After(transcriptEvery):
		m.n++
		return model.Transcript{Role: "model", Text: fmt.Sprintf("loopback transcript %d", m.n)}, nil
	}
}

func (m *Model) RecvInterrupted() (bool, error) {
	return false, nil
}

func (m *Model) SupportsInterruption() bool {
	return false
}

func (m *Model) HandleInterrupted() error {
	return nil
}

func (m *Model) Close() error {
	select {
	case <-m.closed:
	default:
		close(m.closed)
	}
	return nil
}
