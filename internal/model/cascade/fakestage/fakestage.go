// Package fakestage provides network-free ASR/LLM/TTS stages so the cascade
// pipeline can be exercised end-to-end (select ?model=cascade) before any real
// provider is wired. It mirrors the loopback model's role for the STS path: no
// network, no keys, just enough behaviour to prove the orchestration works.
//
//   - ASR fires a synthetic user turn on a fixed cadence (mic audio is ignored).
//   - LLM echoes the last user turn back.
//   - TTS "speaks" the reply as a short 440Hz tone so output audio is audible.
package fakestage

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade"
)

const (
	rate     = audio.OpusRate // 48kHz, the Model audio contract
	turnpace = 4 * time.Second
	toneHz   = 440
	toneAmp  = 8000
	toneDur  = 600 * time.Millisecond
)

// ASR emits a canned ASRFinal every turnpace; inbound audio is discarded.
type ASR struct {
	events chan cascade.ASREvent
	stop   chan struct{}
}

func NewASR() *ASR {
	a := &ASR{events: make(chan cascade.ASREvent, 4), stop: make(chan struct{})}
	go a.loop()
	return a
}

func (a *ASR) loop() {
	t := time.NewTicker(turnpace)
	defer t.Stop()
	n := 0
	for {
		select {
		case <-a.stop:
			return
		case <-t.C:
			n++
			select {
			case a.events <- cascade.ASREvent{Kind: cascade.ASRFinal, Text: fmt.Sprintf("demo turn %d", n)}:
			case <-a.stop:
				return
			}
		}
	}
}

func (a *ASR) Write([]int16) error             { return nil }
func (a *ASR) Events() <-chan cascade.ASREvent { return a.events }
func (a *ASR) Close() error {
	select {
	case <-a.stop:
	default:
		close(a.stop)
	}
	return nil
}

// LLM echoes the most recent user turn.
type LLM struct{}

func NewLLM() *LLM { return &LLM{} }

func (*LLM) Generate(_ context.Context, history []cascade.Message) (<-chan string, error) {
	last := ""
	if n := len(history); n > 0 {
		last = history[n-1].Text
	}
	ch := make(chan string, 1)
	ch <- "you said: " + last
	close(ch)
	return ch, nil
}

func (*LLM) Close() error { return nil }

// TTS renders any text as a fixed 440Hz tone burst (mono s16, 48kHz).
type TTS struct{}

func NewTTS() *TTS { return &TTS{} }

func (*TTS) Synthesize(_ context.Context, _ string) (<-chan []int16, error) {
	n := int(rate * toneDur / time.Second)
	buf := make([]int16, n)
	for i := range buf {
		buf[i] = int16(toneAmp * math.Sin(2*math.Pi*toneHz*float64(i)/float64(rate)))
	}
	ch := make(chan []int16, 1)
	ch <- buf
	close(ch)
	return ch, nil
}

func (*TTS) Close() error { return nil }
