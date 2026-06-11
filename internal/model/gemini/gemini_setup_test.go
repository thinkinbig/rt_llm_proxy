package gemini

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildSetupSystemPrompt(t *testing.T) {
	setup := buildSetup("models/x", Config{SystemPrompt: "你是一个DJ助手", VAD: VADConfig{Enabled: true}})
	if setup.Setup.SystemInstruction == nil {
		t.Fatal("systemInstruction not set")
	}
	if got := setup.Setup.SystemInstruction.Parts[0].Text; got != "你是一个DJ助手" {
		t.Fatalf("system text = %q", got)
	}
	// Auto VAD on → automaticActivityDetection must be omitted entirely.
	b, _ := json.Marshal(setup)
	if strings.Contains(string(b), "automaticActivityDetection") {
		t.Fatalf("auto VAD should omit activity detection: %s", b)
	}
	if !strings.Contains(string(b), "systemInstruction") {
		t.Fatalf("systemInstruction missing from wire: %s", b)
	}
}

func TestRealtimeInputAudioWire(t *testing.T) {
	// Live 3.1 rejects realtimeInput.mediaChunks; audio must go in realtimeInput.audio.
	var msg geminiRealtimeInput
	msg.RealtimeInput.Audio = &geminiBlob{MimeType: "audio/pcm;rate=16000", Data: "AAAA"}
	b, _ := json.Marshal(msg)
	if !strings.Contains(string(b), `"audio"`) {
		t.Fatalf("realtimeInput.audio missing: %s", b)
	}
	if strings.Contains(string(b), "mediaChunks") {
		t.Fatalf("deprecated mediaChunks must not be sent: %s", b)
	}
}

func TestBuildSetupNoSystemPrompt(t *testing.T) {
	setup := buildSetup("models/x", Config{VAD: VADConfig{Enabled: false}})
	if setup.Setup.SystemInstruction != nil {
		t.Fatal("systemInstruction should be nil when prompt empty")
	}
	b, _ := json.Marshal(setup)
	if strings.Contains(string(b), "systemInstruction") {
		t.Fatalf("empty prompt must omit systemInstruction: %s", b)
	}
	// Manual VAD (disabled) → activity detection present and disabled.
	if !strings.Contains(string(b), `"disabled":true`) {
		t.Fatalf("manual VAD should mark disabled: %s", b)
	}
}
