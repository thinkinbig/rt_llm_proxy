package rtc

import "testing"

func TestParseToolResult(t *testing.T) {
	res, ok := parseToolResult([]byte(`{"type":"tool_result","id":"c1","name":"get_weather","response":{"temp":"25C"}}`))
	if !ok {
		t.Fatal("valid tool_result not parsed")
	}
	if res.ID != "c1" || res.Name != "get_weather" || string(res.Response) != `{"temp":"25C"}` {
		t.Fatalf("parsed wrong: %+v", res)
	}
}

func TestParseToolResultRejectsOther(t *testing.T) {
	// Plain user text and transcript lines must NOT be taken as tool results.
	for _, in := range []string{
		`hello there`,
		`{"role":"user","text":"hi","seq":3}`,
		`{"type":"something_else","id":"x"}`,
	} {
		if _, ok := parseToolResult([]byte(in)); ok {
			t.Fatalf("non-tool-result accepted: %s", in)
		}
	}
}
