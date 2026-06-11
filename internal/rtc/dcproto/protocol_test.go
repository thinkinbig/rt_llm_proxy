package dcproto

import (
	"encoding/json"
	"testing"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

func TestEncodeLineUntagged(t *testing.T) {
	got := Encode(transcript.Line{Seq: 7, Role: "model", Text: "hi"})
	want := `{"seq":7,"role":"model","text":"hi"}`
	if got != want {
		t.Fatalf("Encode = %s, want %s", got, want)
	}
	// The browser keys transcript lines by shape — they must NOT carry a type tag.
	var m map[string]any
	_ = json.Unmarshal([]byte(got), &m)
	if _, tagged := m["type"]; tagged {
		t.Fatalf("transcript line must be untagged: %s", got)
	}
}

func TestEncodeToolCallTagged(t *testing.T) {
	got := EncodeToolCall(model.ToolCall{ID: "a", Name: "play", Args: json.RawMessage(`{"track":"x"}`)})
	want := `{"type":"tool_call","id":"a","name":"play","args":{"track":"x"}}`
	if got != want {
		t.Fatalf("EncodeToolCall = %s, want %s", got, want)
	}
}

func TestDecodeToolResult(t *testing.T) {
	res, ok := Decode([]byte(`{"type":"tool_result","id":"c1","name":"get_weather","response":{"temp":"25C"}}`))
	if !ok {
		t.Fatal("valid tool_result not parsed")
	}
	if res.ID != "c1" || res.Name != "get_weather" || string(res.Response) != `{"temp":"25C"}` {
		t.Fatalf("parsed wrong: %+v", res)
	}
}

func TestDecodeRejectsNonToolResult(t *testing.T) {
	// Plain user text, transcript lines, and other envelopes are user text, not
	// tool results — this is the load-bearing fallback invariant.
	for _, in := range []string{
		`hello there`,
		`{"role":"user","text":"hi","seq":3}`,
		`{"type":"something_else","id":"x"}`,
	} {
		if _, ok := Decode([]byte(in)); ok {
			t.Fatalf("non-tool-result accepted: %s", in)
		}
	}
}
