package cascade

import (
	"context"
	"io"
	"testing"
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

// fakeAudioSource streams a fixed buffer then returns io.EOF.
type fakeAudioSource struct {
	frames [][]int16
	pos    int
}

func (f *fakeAudioSource) Read() ([]int16, error) {
	if f.pos >= len(f.frames) {
		return nil, io.EOF
	}
	pcm := f.frames[f.pos]
	f.pos++
	return pcm, nil
}
func (f *fakeAudioSource) Close() error { return nil }

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

func TestCloseIsIdempotent(t *testing.T) {
	c, _ := newTestCascade(t)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
