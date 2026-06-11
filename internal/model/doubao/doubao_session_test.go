package doubao

import (
	"testing"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

func TestBuildStartSessionPersonaVoice(t *testing.T) {
	start := buildStartSession(Config{
		BotName:       "DJ小包",
		SystemRole:    "你是一个DJ",
		SpeakingStyle: "轻松",
		Voice:         "zh_female_vv_jupiter_bigtts",
	}, nil)

	dialog := start["dialog"].(map[string]any)
	if dialog["bot_name"] != "DJ小包" || dialog["system_role"] != "你是一个DJ" || dialog["speaking_style"] != "轻松" {
		t.Fatalf("dialog persona wrong: %#v", dialog)
	}
	tts := start["tts"].(map[string]any)
	if tts["speaker"] != "zh_female_vv_jupiter_bigtts" {
		t.Fatalf("speaker wrong: %#v", tts)
	}
	if _, ok := start["asr"]; ok {
		t.Fatalf("asr should be absent when unconfigured: %#v", start)
	}
}

func TestBuildStartSessionModelVersion(t *testing.T) {
	start := buildStartSession(Config{BotName: "豆包", ModelVersion: "2.2.0.0"}, nil)
	extra := start["dialog"].(map[string]any)["extra"].(map[string]any)
	if extra["model"] != "2.2.0.0" {
		t.Fatalf("dialog.extra.model = %v, want 2.2.0.0", extra["model"])
	}
}

func TestBuildStartSessionOmitsEmpty(t *testing.T) {
	start := buildStartSession(Config{BotName: "豆包"}, nil)
	dialog := start["dialog"].(map[string]any)
	if _, ok := dialog["system_role"]; ok {
		t.Fatal("system_role should be omitted when empty")
	}
	if _, ok := dialog["speaking_style"]; ok {
		t.Fatal("speaking_style should be omitted when empty")
	}
	tts := start["tts"].(map[string]any)
	if _, ok := tts["speaker"]; ok {
		t.Fatal("speaker should be omitted when empty")
	}
}

func TestBuildStartSessionHistory(t *testing.T) {
	start := buildStartSession(Config{BotName: "豆包"}, []model.RestoredTurn{
		{Role: "user", Text: "hi"},
		{Role: "model", Text: "hello"},
	})
	dialog := start["dialog"].(map[string]any)
	dc := dialog["dialog_context"].([]map[string]string)
	if len(dc) != 2 || dc[1]["role"] != "assistant" {
		t.Fatalf("dialog_context wrong: %#v", dc)
	}
}

func TestBuildASRExtra(t *testing.T) {
	if buildASRExtra(Config{}) != nil {
		t.Fatal("empty ASR config should yield nil")
	}
	asr := buildASRExtra(Config{ASRTwopass: true, ASREndSmoothMs: 800, Hotwords: []string{"火山引擎"}})
	extra := asr["extra"].(map[string]any)
	if extra["enable_asr_twopass"] != true {
		t.Fatalf("twopass not set: %#v", extra)
	}
	if extra["end_smooth_window_ms"] != 800 {
		t.Fatalf("end_smooth_window_ms wrong: %#v", extra)
	}
	ctx := extra["context"].(map[string]any)
	hw := ctx["hotwords"].([]map[string]string)
	if len(hw) != 1 || hw[0]["word"] != "火山引擎" {
		t.Fatalf("hotwords wrong: %#v", hw)
	}
}
