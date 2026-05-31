package transcript

import (
	"encoding/json"
	"testing"
)

func TestLineJSONRoundTrip(t *testing.T) {
	in := Line{Seq: 3, Role: "user", Text: "hello"}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Line
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestRecordListenerReceivesSameSeq(t *testing.T) {
	var got []Line
	l := listenerFunc(func(_ SessionMeta, line Line) { got = append(got, line) })
	r := NewRecorder(0, nil, 256, SessionMeta{}, l)

	l1 := r.Record("user", "a")
	l2 := r.Record("model", "b")
	if l1.Seq != 1 || l2.Seq != 2 {
		t.Fatalf("seq = %d,%d want 1,2", l1.Seq, l2.Seq)
	}
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 2 {
		t.Fatalf("listener got %+v", got)
	}
}

type listenerFunc func(SessionMeta, Line)

func (f listenerFunc) OnLine(m SessionMeta, l Line) { f(m, l) }
