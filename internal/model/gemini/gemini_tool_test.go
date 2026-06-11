package gemini

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

func TestBuildSetupTools(t *testing.T) {
	setup := buildSetup("models/x", Config{
		VAD: VADConfig{Enabled: true},
		Tools: []FunctionDeclaration{{
			Name:        "get_weather",
			Description: "Get weather for a city",
			Parameters: map[string]any{
				"type":       "object",
				"properties": map[string]any{"city": map[string]any{"type": "string"}},
			},
		}},
	})
	b, _ := json.Marshal(setup)
	s := string(b)
	if !strings.Contains(s, `"functionDeclarations"`) || !strings.Contains(s, `"get_weather"`) {
		t.Fatalf("tools missing from setup wire: %s", s)
	}
}

func TestBuildSetupNoTools(t *testing.T) {
	setup := buildSetup("models/x", Config{VAD: VADConfig{Enabled: true}})
	b, _ := json.Marshal(setup)
	if strings.Contains(string(b), "tools") {
		t.Fatalf("tools must be omitted when none declared: %s", b)
	}
}

func TestHandleToolCallFansOut(t *testing.T) {
	g := &Gemini{ctx: context.Background(), toolCallCh: make(chan model.ToolCall, 4)}
	g.handleToolCall(&geminiToolCall{
		FunctionCalls: []struct {
			ID   string          `json:"id"`
			Name string          `json:"name"`
			Args json.RawMessage `json:"args"`
		}{
			{ID: "c1", Name: "get_weather", Args: json.RawMessage(`{"city":"北京"}`)},
		},
	})
	got := <-g.toolCallCh
	if got.ID != "c1" || got.Name != "get_weather" || string(got.Args) != `{"city":"北京"}` {
		t.Fatalf("tool call wrong: %+v", got)
	}
}

func TestToolResponseWire(t *testing.T) {
	var msg geminiToolResponse
	msg.ToolResponse.FunctionResponses = []geminiFunctionResponse{{
		ID: "c1", Name: "get_weather", Response: json.RawMessage(`{"temp":"25C"}`),
	}}
	b, _ := json.Marshal(msg)
	s := string(b)
	for _, want := range []string{`"toolResponse"`, `"functionResponses"`, `"c1"`, `"temp":"25C"`} {
		if !strings.Contains(s, want) {
			t.Fatalf("toolResponse wire missing %s: %s", want, s)
		}
	}
}
