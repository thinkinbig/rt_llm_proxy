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
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

// Config wires the three stages. New requires all three to be non-nil.
type Config struct {
	ASR         ASR
	LLM         LLM
	TTS         TTS
	TurnDetect  TurnDetector // nil → NopTurnDetector (fire immediately)
	System string // optional system prompt, seeded as history[0]

	// OnLLMToken is called for each token emitted by the LLM before it is
	// forwarded to TTS. It receives the current token and the accumulated
	// reply so far.
	//
	// Return (replacement, true) to substitute the token (e.g. inject a
	// tool-call sentinel, modify wording). Return ("", false) to pass the
	// original token through unchanged. Returning ("", true) silently drops
	// the token from TTS without affecting history accumulation.
	//
	// nil means no interception (default, zero overhead).
	OnLLMToken func(token, accumulated string) (string, bool)
}

type Cascade struct {
	ctx    context.Context
	cancel context.CancelFunc

	asr        ASR
	llm        LLM
	tts        TTS
	turnDetect TurnDetector
	onLLMToken func(token, accumulated string) (string, bool)

	recvCh       chan []int16          // TTS audio out -> Recv()
	transcriptCh chan model.Transcript // user + model text -> RecvTranscript()
	textIn       chan string           // SendText -> orchestrator (typed user turn)
	modelTurnCh  chan string           // respond() -> run(): completed model reply to append
	restoreCh    chan []Message        // RestoreContext -> run(): prior turns to seed on reconnect

	history []Message // touched only by the run() goroutine; no lock needed

	// barge-in handle: cancels the in-flight LLM+TTS for the current turn.
	// genDone is closed by respond() on exit so bargeIn() can wait for it.
	genMu     sync.Mutex
	genCancel context.CancelFunc
	genDone   chan struct{}

	// pending turn-end timer: owned exclusively by the run() goroutine (no lock).
	// schedulePending() replaces it; cancelPending() stops it; run() reads pendingCh.
	pendingCh     chan struct{}
	pendingCancel context.CancelFunc

	// speculative execution state: true when the last history entry is a
	// tentative user turn committed from a stable partial (not yet ASR-final).
	// Only touched by the run() goroutine.
	speculativeActive bool

	// partial stability tracker (run() goroutine only).
	partialStab struct {
		text      string
		count     int
		firstSeen time.Time
	}

	// output-mix seam: when non-nil, Recv() reads from audioSrc instead of
	// recvCh. Falls back to recvCh on io.EOF.
	audioMu  sync.Mutex
	audioSrc AudioSource

	// wg tracks every goroutine the cascade spawns — run(), each respond(),
	// and the turn-detect timers — so Close() is a real shutdown barrier.
	wg sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

var (
	_ model.Model           = (*Cascade)(nil)
	_ model.Transcriber     = (*Cascade)(nil)
	_ model.ContextRestorer = (*Cascade)(nil)
)

func New(ctx context.Context, cfg Config) (*Cascade, error) {
	if cfg.ASR == nil || cfg.LLM == nil || cfg.TTS == nil {
		return nil, errors.New("cascade: ASR, LLM and TTS stages are all required")
	}
	td := cfg.TurnDetect
	if td == nil {
		td = NopTurnDetector{}
	}
	cctx, cancel := context.WithCancel(ctx)
	c := &Cascade{
		ctx: cctx, cancel: cancel,
		asr: cfg.ASR, llm: cfg.LLM, tts: cfg.TTS,
		turnDetect:   td,
		onLLMToken:   cfg.OnLLMToken,
		recvCh:       make(chan []int16, 64),
		transcriptCh: make(chan model.Transcript, 64),
		textIn:       make(chan string, 8),
		modelTurnCh:  make(chan string, 8),
		restoreCh:    make(chan []Message, 1),
		pendingCh:    make(chan struct{}, 1),
	}
	if cfg.System != "" {
		c.history = append(c.history, Message{Role: "system", Text: cfg.System})
	}
	c.wg.Go(c.run)
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

// RestoreContext implements model.ContextRestorer: on reconnect it seeds prior
// conversation turns into history so the LLM resumes with dialogue context
// instead of starting amnesiac. Only "user" and "model" turns are kept (New
// already seeds the system prompt). The turns are handed to run(), which owns
// history, and are appended after any system prompt and before the first live
// turn. Must be called before live dialogue begins.
func (c *Cascade) RestoreContext(turns []model.RestoredTurn) error {
	if len(turns) == 0 {
		return nil
	}
	msgs := make([]Message, 0, len(turns))
	for _, t := range turns {
		if t.Role != "user" && t.Role != "model" {
			continue
		}
		msgs = append(msgs, Message{Role: t.Role, Text: t.Text})
	}
	if len(msgs) == 0 {
		return nil
	}
	select {
	case c.restoreCh <- msgs:
		return nil
	case <-c.ctx.Done():
		return io.EOF
	}
}

// Recv: next chunk of audio (48k mono), or io.EOF on close.
//
// When an AudioSource is active (set via SetAudioSource), reads from it first.
// On io.EOF from the source the source is cleared and Recv falls back to the
// TTS recvCh, so the bot's voice resumes seamlessly after a track ends.
// Blocks during silence exactly like the STS adapters.
func (c *Cascade) Recv() ([]int16, error) {
	c.audioMu.Lock()
	src := c.audioSrc
	c.audioMu.Unlock()

	if src != nil {
		pcm, err := src.Read()
		if err == nil {
			return pcm, nil
		}
		// Source exhausted (io.EOF) or errored — clear it and fall through.
		c.audioMu.Lock()
		if c.audioSrc == src { // guard against concurrent SetAudioSource
			src.Close()
			c.audioSrc = nil
		}
		c.audioMu.Unlock()
	}

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

// SetAudioSource injects an AudioSource into the outbound stream. Subsequent
// Recv() calls read from src until it returns io.EOF, then fall back to TTS
// audio. Any previously active source is closed immediately.
//
// Pass nil to clear an active source without a replacement.
func (c *Cascade) SetAudioSource(src AudioSource) {
	c.audioMu.Lock()
	old := c.audioSrc
	c.audioSrc = src
	c.audioMu.Unlock()
	if old != nil {
		old.Close()
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

// Close cancels the pipeline, waits for every spawned goroutine to exit, then
// releases the stages and any active audio source. It is idempotent and
// aggregates the stages' close errors. Order matters: goroutines that call into
// the stages (run, respond) must finish before the stages are closed.
func (c *Cascade) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.wg.Wait() // run(), respond()s and timers have exited; stages are now idle

		c.closeErr = errors.Join(c.asr.Close(), c.llm.Close(), c.tts.Close())

		// Release any audio source still playing at teardown (every other path —
		// SetAudioSource replacement, Recv EOF fallback — already closes it).
		c.audioMu.Lock()
		src := c.audioSrc
		c.audioSrc = nil
		c.audioMu.Unlock()
		if src != nil {
			c.closeErr = errors.Join(c.closeErr, src.Close())
		}
	})
	return c.closeErr
}

func (c *Cascade) RecvInterrupted() (bool, error) {
	// Cascade's barge-in is triggered by the ASRSpeechStarted event, handled
	// in the run loop (see handleASR in turn.go).
	return false, nil
}

func (c *Cascade) SupportsInterruption() bool {
	return true
}

func (c *Cascade) HandleInterrupted() error {
	c.bargeIn()
	return nil
}

func (c *Cascade) setGenCancel(cancel context.CancelFunc, done chan struct{}) {
	c.genMu.Lock()
	c.genCancel = cancel
	c.genDone = done
	c.genMu.Unlock()
}

func (c *Cascade) emitTranscript(role, text string) {
	select {
	case c.transcriptCh <- model.Transcript{Role: role, Text: text}:
	case <-c.ctx.Done():
	}
}
