package doubao

import (
	"context"
	"testing"
	"time"
)

func TestHandleASRInterim(t *testing.T) {
	d := &Doubao{ctx: context.Background(), transcriptCh: make(chan transcript, 1)}
	d.handleASR([]byte(`{"results":[{"text":"你好","is_interim":true}]}`))
	select {
	case tr := <-d.transcriptCh:
		if tr.Role != "user" || tr.Text != "你好" || tr.Final {
			t.Fatalf("got %+v", tr)
		}
	case <-time.After(time.Second):
		t.Fatal("no transcript")
	}
}

func TestHandleASRFinal(t *testing.T) {
	d := &Doubao{ctx: context.Background(), transcriptCh: make(chan transcript, 1)}
	d.handleASR([]byte(`{"results":[{"text":"今天天气怎么样","is_interim":false}]}`))
	tr := <-d.transcriptCh
	if tr.Role != "user" || tr.Text != "今天天气怎么样" || !tr.Final {
		t.Fatalf("got %+v", tr)
	}
}

func TestHandleChat(t *testing.T) {
	d := &Doubao{ctx: context.Background(), transcriptCh: make(chan transcript, 1)}
	d.handleChat([]byte(`{"content":"我很好"}`))
	tr := <-d.transcriptCh
	if tr.Role != "model" || tr.Text != "我很好" || tr.Final {
		t.Fatalf("got %+v", tr)
	}
}

func TestChatAccumulates(t *testing.T) {
	d := &Doubao{ctx: context.Background(), transcriptCh: make(chan transcript, 4)}
	for _, frag := range []string{"我", "很好", "，谢谢"} {
		d.handleChat([]byte(`{"content":"` + frag + `"}`))
	}
	var last transcript
	for len(d.transcriptCh) > 0 {
		last = <-d.transcriptCh
	}
	if last.Text != "我很好，谢谢" {
		t.Fatalf("got %+v", last)
	}
}
