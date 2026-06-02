package cascade

import (
	"context"
	"strings"
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
		c.emitTranscript("user", ev.Text) // live caption; not a committed turn
	case ASRSpeechStarted:
		c.bargeIn()
	case ASRFinal:
		if jaccard(ev.Text, lastUserText) >= similarityThreshold {
			return lastUserText // duplicate utterance — ignore
		}
		c.history = append(c.history, Message{Role: "user", Text: ev.Text})
		c.emitTranscript("user", ev.Text)
		go c.respond()
		return ev.Text
	}
	return lastUserText
}

// respond streams the LLM reply through TTS into recvCh using a two-phase
// strategy that cuts time-to-first-audio (TTFA):
//
//  1. Quick phase: accumulate LLM tokens until the first sentence boundary
//     (. ? ! newline), then immediately synthesize and play that fragment
//     while the LLM continues generating.
//
//  2. Final phase: once the quick TTS finishes, synthesize all remaining
//     tokens as a single call (they were buffered while quick was playing).
//
// Both phases share genCtx so bargeIn() cancels either one immediately.
// History is updated only after both phases complete or the turn is aborted.
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

	var (
		quick strings.Builder // tokens up to (and including) first sentence end
		final strings.Builder // tokens after the sentence boundary
		full  strings.Builder // complete reply for history
		found bool            // true once the quick boundary has been found
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

		if found {
			final.WriteString(tts)
			continue
		}
		quick.WriteString(tts)
		if containsSentenceEnd(tts) {
			found = true
			if !c.synthesize(genCtx, quick.String()) {
				return
			}
		}
	}

	// If the entire LLM reply had no sentence boundary (e.g. a one-liner with
	// no punctuation), quick.String() holds everything and final is empty.
	// Synthesize whichever bucket has the remaining text.
	remaining := final.String()
	if !found {
		remaining = quick.String()
	}
	if remaining != "" {
		if !c.synthesize(genCtx, remaining) {
			return
		}
	}

	text := full.String()
	if text == "" {
		return
	}
	c.history = append(c.history, Message{Role: "model", Text: text})
	c.emitTranscript("model", text)
}

// synthesize calls TTS and forwards all PCM chunks to recvCh. Returns false if
// the context was cancelled before synthesis completed (barge-in / shutdown).
func (c *Cascade) synthesize(ctx context.Context, text string) bool {
	pcmCh, err := c.tts.Synthesize(ctx, text)
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
