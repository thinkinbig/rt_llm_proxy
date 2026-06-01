// Package cascade is a model.Model that chains a streaming ASR -> LLM -> TTS
// pipeline behind the provider-agnostic Model seam. The bridge, outbound pump,
// transcript recorder and side-channel see an ordinary Model and never learn it
// is cascaded, so it runs side-by-side with the STS providers (gemini/doubao)
// and is selected by model name — letting you A/B and roll back per session.
//
// The three stages are injected via Config (mirroring internal/ratelimit's
// clean-default-plus-injectable-seam style): managed-API impls now, self-hosted
// later, fakes in tests. Concrete stage impls live in subpackages.
package cascade

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

// Config wires the three stages. New requires all three to be non-nil.
type Config struct {
	ASR    ASR
	LLM    LLM
	TTS    TTS
	System string // optional system prompt, seeded as history[0]
}

type Cascade struct {
	ctx    context.Context
	cancel context.CancelFunc

	asr ASR
	llm LLM
	tts TTS

	recvCh       chan []int16          // TTS audio out -> Recv()
	transcriptCh chan model.Transcript // user + model text -> RecvTranscript()
	textIn       chan string           // SendText -> orchestrator (typed user turn)

	history []Message // touched only by the run() goroutine; no lock needed

	// barge-in handle: cancels the in-flight LLM+TTS for the current turn.
	genMu     sync.Mutex
	genCancel context.CancelFunc

	wg sync.WaitGroup
}

var (
	_ model.Model       = (*Cascade)(nil)
	_ model.Transcriber = (*Cascade)(nil)
)

func New(ctx context.Context, cfg Config) (*Cascade, error) {
	if cfg.ASR == nil || cfg.LLM == nil || cfg.TTS == nil {
		return nil, errors.New("cascade: ASR, LLM and TTS stages are all required")
	}
	cctx, cancel := context.WithCancel(ctx)
	c := &Cascade{
		ctx: cctx, cancel: cancel,
		asr: cfg.ASR, llm: cfg.LLM, tts: cfg.TTS,
		recvCh:       make(chan []int16, 64),
		transcriptCh: make(chan model.Transcript, 64),
		textIn:       make(chan string, 8),
	}
	if cfg.System != "" {
		c.history = append(c.history, Message{Role: "system", Text: cfg.System})
	}
	c.wg.Add(1)
	go c.run()
	return c, nil
}

// SendAudio: mic PCM (48k mono) -> ASR. Resampling lives inside the ASR impl.
func (c *Cascade) SendAudio(pcm []int16) error { return c.asr.Write(pcm) }

// SendText: a typed message from the data channel, handled as a user turn on the
// same path as an ASR final.
func (c *Cascade) SendText(text string) error {
	select {
	case c.textIn <- text:
		return nil
	case <-c.ctx.Done():
		return io.EOF
	}
}

// Recv: next chunk of TTS audio (48k mono), or io.EOF on close. Blocks during
// silence, exactly like the STS adapters — the pump simply waits, emitting no
// frame, and Opus DTX covers the gap.
func (c *Cascade) Recv() ([]int16, error) {
	select {
	case <-c.ctx.Done():
		return nil, io.EOF
	case pcm, ok := <-c.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return pcm, nil
	}
}

// RecvTranscript implements model.Transcriber for free: text already exists at
// the ASR (user) and LLM (model) stages.
func (c *Cascade) RecvTranscript() (model.Transcript, error) {
	select {
	case <-c.ctx.Done():
		return model.Transcript{}, io.EOF
	case t, ok := <-c.transcriptCh:
		if !ok {
			return model.Transcript{}, io.EOF
		}
		return t, nil
	}
}

func (c *Cascade) Close() error {
	c.cancel()
	c.asr.Close()
	c.llm.Close()
	c.tts.Close()
	c.wg.Wait()
	return nil
}

func (c *Cascade) setGenCancel(cancel context.CancelFunc) {
	c.genMu.Lock()
	c.genCancel = cancel
	c.genMu.Unlock()
}
