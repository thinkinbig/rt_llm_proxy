package cascade

import (
	"context"
	"strings"
	"time"
)

// similarityThreshold is the Jaccard score above which a new user turn is
// considered a duplicate of the in-flight one. Mirrors the 0.95 threshold in
// RealtimeVoiceChat's check_abort().
const similarityThreshold = 0.9

// run is the single orchestration goroutine. It owns history, lastUserText,
// and turn state, so those need no locking. The only cross-goroutine handoff
// is genCancel / genDone.
func (c *Cascade) run() {
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
			c.startRespond()
		case msgs := <-c.restoreCh:
			// Reconnect: seed prior turns into history before live dialogue.
			// Only run() mutates history, so the append happens here.
			c.history = append(c.history, msgs...)
		case text := <-c.textIn:
			if jaccard(text, lastUserText) >= similarityThreshold {
				continue // duplicate typed turn — ignore
			}
			lastUserText = text
			c.history = append(c.history, Message{Role: "user", Text: text})
			c.emitTranscript("user", text)
			c.startRespond()
		case text := <-c.modelTurnCh:
			// respond() finished a turn; only run() mutates history, so the
			// model reply is appended here rather than in the respond goroutine.
			c.history = append(c.history, Message{Role: "model", Text: text})
			c.emitTranscript("model", text)
		}
	}
}

// startRespond launches a respond() goroutine on a snapshot of the current
// history, tracking it in wg so Close() waits for it. Must be called from
// run(), whose own wg token keeps the counter positive across the Add. The
// snapshot is taken here (in run()) before the goroutine starts.
func (c *Cascade) startRespond() {
	snapshot := c.snapshotHistory()
	c.wg.Go(func() { c.respond(snapshot) })
}

// snapshotHistory returns a copy of history for a respond() goroutine to read
// without racing run()'s subsequent mutations. Must be called from run().
func (c *Cascade) snapshotHistory() []Message {
	h := make([]Message, len(c.history))
	copy(h, c.history)
	return h
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
	c.wg.Go(func() {
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
	})
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
