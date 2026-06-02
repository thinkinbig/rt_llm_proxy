package gemini

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

func TestParseInputTranscription(t *testing.T) {
	raw := `{"serverContent":{"inputTranscription":{"text":"你好"}}}`
	var msg geminiServerMsg
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatal(err)
	}
	g := &Gemini{ctx: context.Background(), textCh: make(chan model.Transcript, 1)}
	g.handleTranscription("user", msg.ServerContent.InputTranscription)
	select {
	case tr := <-g.textCh:
		if tr.Role != "user" || tr.Text != "你好" {
			t.Fatalf("got %+v", tr)
		}
	case <-time.After(time.Second):
		t.Fatal("no transcript")
	}
}

func TestAccumulatesFragments(t *testing.T) {
	g := &Gemini{ctx: context.Background(), textCh: make(chan model.Transcript, 8)}
	for _, frag := range []string{"今", "天", "天气"} {
		g.handleTranscription("user", &geminiTranscription{Text: frag})
	}
	want := []string{"今", "今天", "今天天气"}
	for _, w := range want {
		if got := <-g.textCh; got.Role != "user" || got.Text != w {
			t.Fatalf("got %+v want {user %q}", got, w)
		}
	}
}

func TestResetsAfterFinished(t *testing.T) {
	g := &Gemini{ctx: context.Background(), textCh: make(chan model.Transcript, 8)}
	g.handleTranscription("user", &geminiTranscription{Text: "你好"})
	g.handleTranscription("user", &geminiTranscription{Text: "吗", Finished: true})
	g.handleTranscription("user", &geminiTranscription{Text: "再见"})
	want := []string{"你好", "你好吗", "再见"}
	for _, w := range want {
		if got := <-g.textCh; got.Role != "user" || got.Text != w {
			t.Fatalf("got %+v want {user %q}", got, w)
		}
	}
}

func TestParseOutputTranscription(t *testing.T) {
	raw := `{"outputTranscription":{"text":"Hi there"}}`
	var msg geminiServerMsg
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatal(err)
	}
	g := &Gemini{ctx: context.Background(), textCh: make(chan model.Transcript, 1)}
	g.handleTranscription("model", msg.OutputTranscription)
	tr := <-g.textCh
	if tr.Role != "model" || tr.Text != "Hi there" {
		t.Fatalf("got %+v", tr)
	}
}

func TestStripsCJKOutputSpaces(t *testing.T) {
	g := &Gemini{ctx: context.Background(), textCh: make(chan model.Transcript, 16)}
	for _, frag := range []string{"你", " 好", " ！", " 很", " 高", " 兴"} {
		g.handleTranscription("model", &geminiTranscription{Text: frag})
	}
	var last model.Transcript
	for {
		select {
		case tr := <-g.textCh:
			last = tr
		default:
			if last.Role != "model" || last.Text != "你好！很高兴" {
				t.Fatalf("got %+v", last)
			}
			return
		}
	}
}

func TestPreservesEnglishOutputSpaces(t *testing.T) {
	g := &Gemini{ctx: context.Background(), textCh: make(chan model.Transcript, 4)}
	g.handleTranscription("model", &geminiTranscription{Text: "Hi"})
	g.handleTranscription("model", &geminiTranscription{Text: " there"})
	if got := <-g.textCh; got.Role != "model" || got.Text != "Hi" {
		t.Fatalf("first: got %+v", got)
	}
	if got := <-g.textCh; got.Role != "model" || got.Text != "Hi there" {
		t.Fatalf("second: got %+v", got)
	}
}

func TestHandleInterruptedDrainsQueuedAudio(t *testing.T) {
	g := &Gemini{
		ctx:    context.Background(),
		recvCh: make(chan []int16, 4),
	}
	g.recvCh <- []int16{1, 2}
	g.recvCh <- []int16{3, 4}

	if err := g.HandleInterrupted(); err != nil {
		t.Fatalf("HandleInterrupted error: %v", err)
	}
	select {
	case <-g.recvCh:
		t.Fatal("expected recvCh to be drained after interruption")
	default:
	}
}
