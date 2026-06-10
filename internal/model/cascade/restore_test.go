package cascade

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

// capturingLLM records the history slice handed to each Generate call so a test
// can assert what context the LLM actually saw.
type capturingLLM struct {
	reply string
	mu    sync.Mutex
	seen  [][]Message
}

func (l *capturingLLM) Generate(_ context.Context, msgs []Message) (<-chan string, error) {
	l.mu.Lock()
	cp := make([]Message, len(msgs))
	copy(cp, msgs)
	l.seen = append(l.seen, cp)
	l.mu.Unlock()
	ch := make(chan string, 1)
	ch <- l.reply
	close(ch)
	return ch, nil
}
func (l *capturingLLM) Close() error { return nil }

func (l *capturingLLM) lastSeen() []Message {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.seen) == 0 {
		return nil
	}
	return l.seen[len(l.seen)-1]
}

// TestRestoreContextSeedsHistory verifies that prior turns handed to
// RestoreContext are seeded into history (in order, after any system prompt)
// so the next LLM turn sees them ahead of the live user turn. Non user/model
// roles are dropped.
func TestRestoreContextSeedsHistory(t *testing.T) {
	asr := &fakeASR{events: make(chan ASREvent, 4)}
	llm := &capturingLLM{reply: "ok"}
	c, err := New(context.Background(), Config{
		ASR: asr,
		LLM: llm,
		TTS: &fakeTTS{frame: make([]int16, 960)},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	if err := c.RestoreContext([]model.RestoredTurn{
		{Role: "user", Text: "capital of france?"},
		{Role: "model", Text: "paris"},
		{Role: "system", Text: "dropped"}, // non user/model is ignored
	}); err != nil {
		t.Fatalf("RestoreContext: %v", err)
	}

	// The bridge calls RestoreContext before live dialogue begins. Let the single
	// run() goroutine drain restoreCh (the only ready channel) before the live
	// turn, so the seed lands ahead of the user turn deterministically.
	time.Sleep(20 * time.Millisecond)

	asr.events <- ASREvent{Kind: ASRFinal, Text: "and germany?"}
	if _, err := c.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}

	got := llm.lastSeen()
	want := []Message{
		{Role: "user", Text: "capital of france?"},
		{Role: "model", Text: "paris"},
		{Role: "user", Text: "and germany?"},
	}
	if len(got) != len(want) {
		t.Fatalf("history = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("history[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestRestoreContextEmptyIsNoop verifies an empty / all-dropped restore is a
// silent no-op (no panic, nothing seeded).
func TestRestoreContextEmptyIsNoop(t *testing.T) {
	asr := &fakeASR{events: make(chan ASREvent, 4)}
	llm := &capturingLLM{reply: "ok"}
	c, err := New(context.Background(), Config{ASR: asr, LLM: llm, TTS: &fakeTTS{frame: make([]int16, 960)}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	if err := c.RestoreContext(nil); err != nil {
		t.Fatalf("RestoreContext(nil): %v", err)
	}
	if err := c.RestoreContext([]model.RestoredTurn{{Role: "system", Text: "x"}}); err != nil {
		t.Fatalf("RestoreContext(system-only): %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	asr.events <- ASREvent{Kind: ASRFinal, Text: "hi"}
	if _, err := c.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	got := llm.lastSeen()
	want := []Message{{Role: "user", Text: "hi"}}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("history = %+v, want %+v", got, want)
	}
}
