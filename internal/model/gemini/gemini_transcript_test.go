package gemini

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestParseInputTranscription(t *testing.T) {
	raw := `{"serverContent":{"inputTranscription":{"text":"你好"}}}`
	var msg geminiServerMsg
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatal(err)
	}
	g := &Gemini{ctx: context.Background(), textCh: make(chan string, 1)}
	g.handleTranscription("user", msg.ServerContent.InputTranscription)
	select {
	case line := <-g.textCh:
		if line != "user: 你好" {
			t.Fatalf("got %q", line)
		}
	case <-time.After(time.Second):
		t.Fatal("no transcript")
	}
}

func TestAccumulatesFragments(t *testing.T) {
	g := &Gemini{ctx: context.Background(), textCh: make(chan string, 8)}
	for _, frag := range []string{"今", "天", "天气"} {
		g.handleTranscription("user", &geminiTranscription{Text: frag})
	}
	want := []string{"user: 今", "user: 今天", "user: 今天天气"}
	for _, w := range want {
		if got := <-g.textCh; got != w {
			t.Fatalf("got %q want %q", got, w)
		}
	}
}

func TestResetsAfterFinished(t *testing.T) {
	g := &Gemini{ctx: context.Background(), textCh: make(chan string, 8)}
	g.handleTranscription("user", &geminiTranscription{Text: "你好"})
	g.handleTranscription("user", &geminiTranscription{Text: "吗", Finished: true})
	g.handleTranscription("user", &geminiTranscription{Text: "再见"})
	want := []string{"user: 你好", "user: 你好吗", "user: 再见"}
	for _, w := range want {
		if got := <-g.textCh; got != w {
			t.Fatalf("got %q want %q", got, w)
		}
	}
}

func TestParseOutputTranscription(t *testing.T) {
	raw := `{"outputTranscription":{"text":"Hi there"}}`
	var msg geminiServerMsg
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatal(err)
	}
	g := &Gemini{ctx: context.Background(), textCh: make(chan string, 1)}
	g.handleTranscription("model", msg.OutputTranscription)
	line := <-g.textCh
	if line != "model: Hi there" {
		t.Fatalf("got %q", line)
	}
}

func TestStripsCJKOutputSpaces(t *testing.T) {
	g := &Gemini{ctx: context.Background(), textCh: make(chan string, 16)}
	for _, frag := range []string{"你", " 好", " ！", " 很", " 高", " 兴"} {
		g.handleTranscription("model", &geminiTranscription{Text: frag})
	}
	var last string
	for {
		select {
		case line := <-g.textCh:
			last = line
		default:
			if last != "model: 你好！很高兴" {
				t.Fatalf("got %q", last)
			}
			return
		}
	}
}

func TestPreservesEnglishOutputSpaces(t *testing.T) {
	g := &Gemini{ctx: context.Background(), textCh: make(chan string, 4)}
	g.handleTranscription("model", &geminiTranscription{Text: "Hi"})
	g.handleTranscription("model", &geminiTranscription{Text: " there"})
	if got := <-g.textCh; got != "model: Hi" {
		t.Fatalf("first line got %q", got)
	}
	if got := <-g.textCh; got != "model: Hi there" {
		t.Fatalf("second line got %q", got)
	}
}
