package rtc

import (
	"testing"

	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

func TestSessionRecordNotifiesListener(t *testing.T) {
	var got []transcript.Line
	l := listenerFunc(func(_ transcript.SessionMeta, line transcript.Line) {
		got = append(got, line)
	})
	meta := transcript.SessionMeta{SessionID: "s1", Provider: "gemini"}
	rec := transcript.NewRecorder(0, nil, 256, meta, l)

	line := rec.Record("user", "hello")
	if line.Seq != 1 || line.Role != "user" || line.Text != "hello" {
		t.Fatalf("record = %+v", line)
	}
	if len(got) != 1 || got[0] != line {
		t.Fatalf("listener got %+v", got)
	}
}

func TestRecorderSnapshotAfterSeq(t *testing.T) {
	meta := transcript.SessionMeta{SessionID: "s1"}
	rec := transcript.NewRecorder(0, nil, 256, meta, nil)
	rec.Record("user", "a")
	rec.Record("model", "b")
	rec.Record("user", "c")

	snap := rec.Snapshot(1)
	if len(snap) != 2 || snap[0].Seq != 2 || snap[1].Seq != 3 {
		t.Fatalf("snapshot = %+v", snap)
	}
}

type listenerFunc func(transcript.SessionMeta, transcript.Line)

func (f listenerFunc) OnLine(m transcript.SessionMeta, l transcript.Line) { f(m, l) }
