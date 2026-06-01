package cascade

import (
	"context"
	"strings"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

// run is the single orchestration goroutine. It owns history and turn state, so
// those need no locking. The only cross-goroutine handoff is genCancel.
//
// M1 limitation: respond() runs inline here, so it blocks the event loop for the
// duration of a reply — meaning an ASRSpeechStarted that arrives mid-reply is not
// acted on until the reply finishes. The barge-in plumbing (genCancel/bargeIn) is
// wired but only becomes effective in M3, which moves respond() onto its own
// goroutine so this loop keeps consuming events while the bot talks.
func (c *Cascade) run() {
	defer c.wg.Done()
	events := c.asr.Events()
	for {
		select {
		case <-c.ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			c.handleASR(ev)
		case text := <-c.textIn:
			c.history = append(c.history, Message{Role: "user", Text: text})
			c.emitTranscript("user", text)
			c.respond()
		}
	}
}

func (c *Cascade) handleASR(ev ASREvent) {
	switch ev.Kind {
	case ASRPartial:
		c.emitTranscript("user", ev.Text) // live caption; not yet a committed turn
	case ASRSpeechStarted:
		c.bargeIn()
	case ASRFinal:
		c.history = append(c.history, Message{Role: "user", Text: ev.Text})
		c.emitTranscript("user", ev.Text)
		c.respond()
	}
}

// respond streams the LLM reply through TTS into recvCh, under a per-turn
// cancellable ctx so bargeIn() can abort it.
//
// This is the M2 long pole. The MVP synthesizes each LLM delta as it arrives;
// TODO(M2): buffer deltas to sentence boundaries and start TTS on the first full
// sentence to cut first-audio latency.
func (c *Cascade) respond() {
	genCtx, cancel := context.WithCancel(c.ctx)
	c.setGenCancel(cancel)
	defer cancel()

	deltas, err := c.llm.Generate(genCtx, c.history)
	if err != nil {
		return
	}
	var reply strings.Builder
	for delta := range deltas {
		reply.WriteString(delta)
		pcmCh, err := c.tts.Synthesize(genCtx, delta)
		if err != nil {
			continue
		}
		for pcm := range pcmCh {
			select {
			case c.recvCh <- pcm:
			case <-genCtx.Done():
				return // barge-in or shutdown: drop the rest of this reply
			}
		}
	}
	full := reply.String()
	if full == "" {
		return
	}
	c.history = append(c.history, Message{Role: "model", Text: full})
	c.emitTranscript("model", full)
}

// bargeIn cancels the in-flight generation and drops queued TTS audio so the bot
// stops talking immediately. It does NOT close recvCh — Recv just blocks until
// the next turn, the same as STS silence.
func (c *Cascade) bargeIn() {
	c.genMu.Lock()
	if c.genCancel != nil {
		c.genCancel()
	}
	c.genMu.Unlock()
	for {
		select {
		case <-c.recvCh:
		default:
			return
		}
	}
}

func (c *Cascade) emitTranscript(role, text string) {
	select {
	case c.transcriptCh <- model.Transcript{Role: role, Text: text}:
	case <-c.ctx.Done():
	}
}
