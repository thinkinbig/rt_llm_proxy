package cascade

import (
	"context"
	"strings"
	"time"
	"unicode"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

// sentenceEnders are runes that mark the end of a speakable sentence and
// trigger the quick TTS phase. Chosen to balance latency vs. fragment risk.
const sentenceEnders = ".?!\n"

// similarityThreshold is the Jaccard score above which a new user turn is
// considered a duplicate of the in-flight one. Mirrors the 0.95 threshold in
// RealtimeVoiceChat's check_abort().
const similarityThreshold = 0.9

// run is the single orchestration goroutine. It owns history, lastUserText,
// and turn state, so those need no locking. The only cross-goroutine handoff
// is genCancel / genDone.
func (c *Cascade) run() {
	defer c.wg.Done()
	events := c.asr.Events()
	var lastUserText string
	for {
		select {
		case <-c.ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			lastUserText = c.handleASR(ev, lastUserText)
		case <-c.pendingCh:
			go c.respond()
		case text := <-c.textIn:
			if jaccard(text, lastUserText) >= similarityThreshold {
				continue // duplicate typed turn — ignore
			}
			lastUserText = text
			c.history = append(c.history, Message{Role: "user", Text: text})
			c.emitTranscript("user", text)
			go c.respond()
		}
	}
}

// handleASR processes one ASR event and returns the updated lastUserText.
func (c *Cascade) handleASR(ev ASREvent, lastUserText string) string {
	switch ev.Kind {

	case ASRPartial:
		c.emitTranscript("user", ev.Text) // live caption
		c.cancelPending()
		c.removeSpeculative() // discard previous speculative turn if text changed
		if c.isPotentialSentence(ev.Text) {
			// Stable partial that looks complete — start LLM speculatively.
			c.history = append(c.history, Message{Role: "user", Text: ev.Text})
			c.speculativeActive = true
			c.scheduleSpeculative()
		}

	case ASRSpeechStarted:
		c.cancelPending()
		c.removeSpeculative()
		c.bargeIn()

	case ASRFinal:
		if jaccard(ev.Text, lastUserText) >= similarityThreshold {
			c.removeSpeculative()
			return lastUserText // duplicate utterance — ignore
		}
		c.cancelPending()
		if c.speculativeActive {
			specText := c.history[len(c.history)-1].Text
			if jaccard(ev.Text, specText) >= similarityThreshold {
				// Final confirms speculation — patch text, LLM already running.
				c.history[len(c.history)-1].Text = ev.Text
				c.speculativeActive = false
				return ev.Text
			}
			// Final differs — discard speculation, start fresh.
			c.removeSpeculative()
		}
		c.history = append(c.history, Message{Role: "user", Text: ev.Text})
		c.emitTranscript("user", ev.Text)
		c.schedulePending(ev.Text)
		return ev.Text
	}
	return lastUserText
}

// isPotentialSentence returns true when text ends with sentence-ending
// punctuation and the same normalised text has been seen stabTrigger times
// within stabWindow — matching RealtimeVoiceChat's stability check.
func (c *Cascade) isPotentialSentence(text string) bool {
	const (
		stabWindow  = 200 * time.Millisecond
		stabTrigger = 3
	)
	if !strings.ContainsAny(text, sentenceEnders) {
		c.partialStab = struct {
			text      string
			count     int
			firstSeen time.Time
		}{}
		return false
	}
	now := time.Now()
	if text == c.partialStab.text && now.Sub(c.partialStab.firstSeen) <= stabWindow {
		c.partialStab.count++
	} else {
		c.partialStab.text = text
		c.partialStab.count = 1
		c.partialStab.firstSeen = now
	}
	return c.partialStab.count >= stabTrigger
}

// removeSpeculative removes the last history entry if a speculative turn is active.
// Must be called from the run() goroutine.
func (c *Cascade) removeSpeculative() {
	if c.speculativeActive && len(c.history) > 0 {
		c.history = c.history[:len(c.history)-1]
	}
	c.speculativeActive = false
}

// scheduleSpeculative fires respond() immediately (speculation is high-confidence).
// Must be called from the run() goroutine.
func (c *Cascade) scheduleSpeculative() {
	// drain any stale signal before sending
	select {
	case <-c.pendingCh:
	default:
	}
	c.pendingCh <- struct{}{}
}

// schedulePending cancels any in-flight turn-detect wait and starts a new one.
// After the suggested pause elapses, it signals pendingCh so run() fires respond().
// Must be called from the run() goroutine.
func (c *Cascade) schedulePending(text string) {
	c.cancelPending()
	ctx, cancel := context.WithCancel(c.ctx)
	c.pendingCancel = cancel
	go func() {
		defer cancel()
		pause := c.turnDetect.SuggestedPause(ctx, text)
		select {
		case <-time.After(pause):
			select {
			case c.pendingCh <- struct{}{}:
			default:
			}
		case <-ctx.Done():
		}
	}()
}

// cancelPending cancels any in-flight turn-detect wait and drains any already-
// queued signal so a subsequent scheduleSpeculative/schedulePending starts clean.
// Must be called from the run() goroutine.
func (c *Cascade) cancelPending() {
	if c.pendingCancel != nil {
		c.pendingCancel()
		c.pendingCancel = nil
	}
	select {
	case <-c.pendingCh:
	default:
	}
}

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
// With a streaming TTS stage (XTTSStreamTTS) each sentence also streams its
// own audio incrementally, so playback begins well before the sentence is
// fully rendered. Both run under genCtx so bargeIn() cancels them at once.
// History is updated only after the whole turn completes (not on abort).
func (c *Cascade) respond() {
	genCtx, cancel := context.WithCancel(c.ctx)
	done := make(chan struct{})
	c.setGenCancel(cancel, done)
	defer close(done)
	defer cancel()

	deltas, err := c.llm.Generate(genCtx, c.history)
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
		seg  strings.Builder  // current sentence being accumulated
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
	c.history = append(c.history, Message{Role: "model", Text: text})
	c.emitTranscript("model", text)
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

// containsSentenceEnd reports whether s contains any sentence-ending rune.
func containsSentenceEnd(s string) bool {
	return strings.ContainsAny(s, sentenceEnders) ||
		strings.IndexFunc(s, func(r rune) bool {
			return unicode.Is(unicode.Po, r) // other punctuation (CJK 。？！)
		}) >= 0
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

// jaccard returns the token-level Jaccard similarity between two strings.
// Tokens are whitespace-split words, lowercased. Returns 1.0 for equal
// strings and 0.0 when both are empty.
func jaccard(a, b string) float64 {
	sa := tokenSet(a)
	sb := tokenSet(b)
	if len(sa) == 0 && len(sb) == 0 {
		return 1.0
	}
	var inter int
	for t := range sa {
		if sb[t] {
			inter++
		}
	}
	union := len(sa) + len(sb) - inter
	if union == 0 {
		return 1.0
	}
	return float64(inter) / float64(union)
}

func tokenSet(s string) map[string]bool {
	m := make(map[string]bool)
	for w := range strings.FieldsSeq(strings.ToLower(s)) {
		m[w] = true
	}
	return m
}

func (c *Cascade) emitTranscript(role, text string) {
	select {
	case c.transcriptCh <- model.Transcript{Role: role, Text: text}:
	case <-c.ctx.Done():
	}
}
