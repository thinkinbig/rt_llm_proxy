package cascade

import (
	"context"
	"strings"
)

// respond streams the LLM reply through TTS into recvCh as a producer/consumer
// pipeline that cuts time-to-first-audio (TTFA) and keeps the LLM unblocked:
//
//   - segmenter (this goroutine): reads LLM deltas, applies the OnLLMToken
//     seam, and pushes each completed sentence (split on . ? ! newline / CJK
//     punctuation) onto the segments channel. The first sentence is the
//     "quick answer" — it ships the moment the boundary appears, while the
//     LLM keeps generating the rest.
//
//   - TTS worker: drains segments and synthesizes each one in order into the
//     shared recvCh. Because it runs concurrently, synthesising/playing
//     sentence N overlaps generating sentence N+1 — the LLM is never blocked
//     waiting for TTS (only by the segments buffer).
//
// With a streaming TTS stage (tts.XTTSStream) each sentence also streams its
// own audio incrementally, so playback begins well before the sentence is
// fully rendered. Both run under genCtx so bargeIn() cancels them at once.
//
// history is a snapshot taken by run(); respond() never touches c.history.
// The completed reply is handed back over modelTurnCh so run() — the sole
// owner of history — performs the append.
func (c *Cascade) respond(history []Message) {
	genCtx, cancel := context.WithCancel(c.ctx)
	done := make(chan struct{})
	c.setGenCancel(cancel, done)
	defer close(done)
	defer cancel()

	deltas, err := c.llm.Generate(genCtx, history)
	if err != nil {
		return
	}

	segments := make(chan string, 8)

	// TTS worker: synthesize each segment in arrival order. On barge-in /
	// shutdown synthesize returns false; drain the rest so the segmenter's
	// sends never block, then exit.
	ttsDone := make(chan struct{})
	go func() {
		defer close(ttsDone)
		quick := true // first segment is the quick answer
		for seg := range segments {
			if !c.synthesize(genCtx, seg, quick) {
				for range segments {
				}
				return
			}
			quick = false
		}
	}()

	var (
		full strings.Builder // complete reply for history (original tokens)
		seg  strings.Builder // current sentence being accumulated
	)
	for delta := range deltas {
		// LLM intercept seam: let business logic inspect / replace the token.
		tts := delta
		if c.onLLMToken != nil {
			if replacement, ok := c.onLLMToken(delta, full.String()); ok {
				tts = replacement
			}
		}

		full.WriteString(delta) // history always gets the original token
		if tts == "" {
			continue // token was dropped by the hook (or was a replacement no-op)
		}

		seg.WriteString(tts)
		if containsSentenceEnd(tts) {
			segments <- seg.String()
			seg.Reset()
		}
	}
	// Flush any trailing text with no sentence boundary (e.g. a one-liner).
	if seg.Len() > 0 {
		segments <- seg.String()
	}
	close(segments)
	<-ttsDone

	text := full.String()
	if text == "" {
		return
	}
	// Hand the reply back to run() to append + emit (single history owner).
	select {
	case c.modelTurnCh <- text:
	case <-c.ctx.Done():
	}
}

// synthesize calls TTS and forwards all PCM chunks to recvCh. Returns false if
// the context was cancelled before synthesis completed (barge-in / shutdown).
//
// When quick is true and the stage implements QuickSynthesizer, the low-latency
// path is used (the quick answer); otherwise it falls back to Synthesize.
func (c *Cascade) synthesize(ctx context.Context, text string, quick bool) bool {
	var (
		pcmCh <-chan []int16
		err   error
	)
	if qs, ok := c.tts.(QuickSynthesizer); ok && quick {
		pcmCh, err = qs.SynthesizeQuick(ctx, text)
	} else {
		pcmCh, err = c.tts.Synthesize(ctx, text)
	}
	if err != nil {
		return true // TTS error: skip this segment, do not abort the turn
	}
	for pcm := range pcmCh {
		select {
		case c.recvCh <- pcm:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

// bargeIn cancels the in-flight generation and drains any buffered TTS audio
// so the bot stops talking immediately. It blocks until the respond() goroutine
// has acknowledged the cancellation (genDone is closed), preventing a race
// where the old goroutine writes to recvCh after the new turn begins.
//
// Cancel sequence mirrors RealtimeVoiceChat process_abort_generation():
//  1. Cancel genCtx  → stops LLM HTTP stream + TTS context
//  2. Wait genDone   → respond() goroutine has exited
//  3. Drain recvCh   → discard any chunks already queued
func (c *Cascade) bargeIn() {
	c.genMu.Lock()
	cancel := c.genCancel
	done := c.genDone
	c.genMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done // wait for respond() to exit before draining
	}

	for {
		select {
		case <-c.recvCh:
		default:
			return
		}
	}
}
