package cascade

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

type fakeASR struct{ events chan ASREvent }

func (f *fakeASR) Write([]int16) error     { return nil }
func (f *fakeASR) Events() <-chan ASREvent { return f.events }
func (f *fakeASR) Close() error            { return nil }

type fakeLLM struct{ reply string }

func (f *fakeLLM) Generate(_ context.Context, _ []Message) (<-chan string, error) {
	ch := make(chan string, 1)
	ch <- f.reply
	close(ch)
	return ch, nil
}
func (f *fakeLLM) Close() error { return nil }

type fakeTTS struct{ frame []int16 }

func (f *fakeTTS) Synthesize(_ context.Context, _ string) (<-chan []int16, error) {
	ch := make(chan []int16, 1)
	ch <- f.frame
	close(ch)
	return ch, nil
}
func (f *fakeTTS) Close() error { return nil }

func newTestCascade(t *testing.T) (*Cascade, *fakeASR) {
	t.Helper()
	asr := &fakeASR{events: make(chan ASREvent, 4)}
	c, err := New(context.Background(), Config{
		ASR: asr,
		LLM: &fakeLLM{reply: "hi there"},
		TTS: &fakeTTS{frame: make([]int16, 960)},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, asr
}

func TestNewRequiresAllStages(t *testing.T) {
	if _, err := New(context.Background(), Config{}); err == nil {
		t.Fatal("expected an error when stages are nil")
	}
}

func TestFullTurnProducesAudioAndTranscripts(t *testing.T) {
	c, asr := newTestCascade(t)
	defer c.Close()

	asr.events <- ASREvent{Kind: ASRFinal, Text: "hello"}

	pcm, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if len(pcm) != 960 {
		t.Fatalf("frame len = %d, want 960", len(pcm))
	}

	got := map[string]string{}
	for range 2 {
		tr, err := c.RecvTranscript()
		if err != nil {
			t.Fatalf("RecvTranscript: %v", err)
		}
		got[tr.Role] = tr.Text
	}
	if got["user"] != "hello" || got["model"] != "hi there" {
		t.Fatalf("transcripts = %+v", got)
	}
}

func TestOnLLMTokenHookFires(t *testing.T) {
	asr := &fakeASR{events: make(chan ASREvent, 4)}
	var got []string
	c, err := New(context.Background(), Config{
		ASR: asr,
		LLM: &fakeLLM{reply: "hello world"},
		TTS: &fakeTTS{frame: make([]int16, 960)},
		OnLLMToken: func(token, _ string) (string, bool) {
			got = append(got, token)
			return "", false // pass through
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	asr.events <- ASREvent{Kind: ASRFinal, Text: "hi"}
	if _, err := c.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("OnLLMToken hook never called")
	}
}

// streamLLM emits each element of deltas as a separate token, so sentence
// boundaries can land on different segments (unlike fakeLLM's single chunk).
type streamLLM struct{ deltas []string }

func (f *streamLLM) Generate(_ context.Context, _ []Message) (<-chan string, error) {
	ch := make(chan string, len(f.deltas))
	for _, d := range f.deltas {
		ch <- d
	}
	close(ch)
	return ch, nil
}
func (f *streamLLM) Close() error { return nil }

// quickTTS records, in order, whether each segment was synthesized via the
// quick (low-latency) path or the normal one.
type quickTTS struct {
	mu    sync.Mutex
	calls []string
}

func (f *quickTTS) record(kind string) (<-chan []int16, error) {
	f.mu.Lock()
	f.calls = append(f.calls, kind)
	f.mu.Unlock()
	ch := make(chan []int16, 1)
	ch <- make([]int16, 960)
	close(ch)
	return ch, nil
}
func (f *quickTTS) Synthesize(context.Context, string) (<-chan []int16, error) {
	return f.record("final")
}
func (f *quickTTS) SynthesizeQuick(context.Context, string) (<-chan []int16, error) {
	return f.record("quick")
}
func (f *quickTTS) Close() error { return nil }

func TestQuickAnswerUsesQuickSynthesizer(t *testing.T) {
	asr := &fakeASR{events: make(chan ASREvent, 4)}
	tts := &quickTTS{}
	c, err := New(context.Background(), Config{
		ASR: asr,
		LLM: &streamLLM{deltas: []string{"Hi there. ", "How are you?"}},
		TTS: tts,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	asr.events <- ASREvent{Kind: ASRFinal, Text: "hello"}

	// The model transcript is emitted only after both segments finish
	// synthesizing, so waiting for it guarantees calls is complete.
	for {
		tr, err := c.RecvTranscript()
		if err != nil {
			t.Fatalf("RecvTranscript: %v", err)
		}
		if tr.Role == "model" {
			break
		}
	}

	tts.mu.Lock()
	defer tts.mu.Unlock()
	want := []string{"quick", "final"}
	if len(tts.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", tts.calls, want)
	}
	for i := range want {
		if tts.calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", tts.calls, want)
		}
	}
}

// fakeAudioSource streams a fixed buffer then returns io.EOF.
type fakeAudioSource struct {
	frames [][]int16
	pos    int
	closed bool
}

func (f *fakeAudioSource) Read() ([]int16, error) {
	if f.pos >= len(f.frames) {
		return nil, io.EOF
	}
	pcm := f.frames[f.pos]
	f.pos++
	return pcm, nil
}
func (f *fakeAudioSource) Close() error { f.closed = true; return nil }

func TestSetAudioSourceTakesPriority(t *testing.T) {
	c, _ := newTestCascade(t)
	defer c.Close()

	sentinel := make([]int16, 480) // distinct from fakeTTS frame (960 samples)
	for i := range sentinel {
		sentinel[i] = 1
	}
	c.SetAudioSource(&fakeAudioSource{frames: [][]int16{sentinel}})

	pcm, err := c.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if len(pcm) != 480 || pcm[0] != 1 {
		t.Fatalf("expected audio source frame, got len=%d val=%d", len(pcm), pcm[0])
	}
}

// slowLLM emits its reply after a short delay so a respond() goroutine stays
// alive while run() keeps mutating history for later turns — the overlap that
// exercises the history-ownership invariant. Run under -race.
type slowLLM struct{}

func (slowLLM) Generate(ctx context.Context, _ []Message) (<-chan string, error) {
	ch := make(chan string, 1)
	go func() {
		defer close(ch)
		select {
		case <-time.After(2 * time.Millisecond):
		case <-ctx.Done():
			return
		}
		select {
		case ch <- "reply.":
		case <-ctx.Done():
		}
	}()
	return ch, nil
}
func (slowLLM) Close() error { return nil }

func TestConcurrentTurnsNoHistoryRace(t *testing.T) {
	asr := &fakeASR{events: make(chan ASREvent, 64)}
	c, err := New(context.Background(), Config{
		ASR: asr,
		LLM: slowLLM{},
		TTS: &fakeTTS{frame: make([]int16, 480)},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	// Drain audio + transcripts so respond()/run() never block on a full channel.
	go func() {
		for {
			if _, err := c.Recv(); err != nil {
				return
			}
		}
	}()
	go func() {
		for {
			if _, err := c.RecvTranscript(); err != nil {
				return
			}
		}
	}()

	// Fire many distinct finals back-to-back; with NopTurnDetector each spawns
	// a respond() that overlaps run()'s appends for subsequent turns.
	for i := range 50 {
		asr.events <- ASREvent{Kind: ASRFinal, Text: fmt.Sprintf("utterance %d", i)}
	}
	time.Sleep(50 * time.Millisecond)
}

func TestCloseReleasesActiveAudioSource(t *testing.T) {
	c, _ := newTestCascade(t)
	src := &fakeAudioSource{frames: [][]int16{make([]int16, 480)}}
	c.SetAudioSource(src)

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !src.closed {
		t.Fatal("Close did not release the active AudioSource")
	}
}

// blockTTS blocks inside Synthesize until release is closed, ignoring ctx, so a
// test can hold a respond() goroutine mid-stage while it calls Close().
type blockTTS struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockTTS) Synthesize(context.Context, string) (<-chan []int16, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	ch := make(chan []int16)
	close(ch)
	return ch, nil
}
func (b *blockTTS) Close() error { return nil }

// TestCloseWaitsForInflightRespond proves Close() is a real barrier: it must not
// return while a respond() goroutine is still executing inside a stage.
func TestCloseWaitsForInflightRespond(t *testing.T) {
	asr := &fakeASR{events: make(chan ASREvent, 4)}
	tts := &blockTTS{entered: make(chan struct{}), release: make(chan struct{})}
	c, err := New(context.Background(), Config{
		ASR: asr,
		LLM: &fakeLLM{reply: "hello"},
		TTS: tts,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	asr.events <- ASREvent{Kind: ASRFinal, Text: "hi"}
	<-tts.entered // a respond() goroutine is now blocked inside Synthesize

	closeDone := make(chan error, 1)
	go func() { closeDone <- c.Close() }()

	select {
	case <-closeDone:
		t.Fatal("Close returned while a respond() goroutine was still in a stage")
	case <-time.After(30 * time.Millisecond):
	}

	close(tts.release) // let the stage finish
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the in-flight goroutine finished")
	}
}

// errCloseTTS returns a fixed error from Close to verify error aggregation.
type errCloseTTS struct{ err error }

func (e errCloseTTS) Synthesize(context.Context, string) (<-chan []int16, error) {
	ch := make(chan []int16)
	close(ch)
	return ch, nil
}
func (e errCloseTTS) Close() error { return e.err }

func TestCloseAggregatesStageErrors(t *testing.T) {
	wantErr := errors.New("tts boom")
	asr := &fakeASR{events: make(chan ASREvent, 4)}
	c, err := New(context.Background(), Config{
		ASR: asr,
		LLM: &fakeLLM{reply: "x"},
		TTS: errCloseTTS{err: wantErr},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close err = %v, want it to wrap %v", err, wantErr)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	c, _ := newTestCascade(t)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
