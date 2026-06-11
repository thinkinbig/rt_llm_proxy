package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "proxy.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestApplyConfigFileProviderFields(t *testing.T) {
	path := writeTemp(t, `
gemini:
  system_prompt: "你是DJ"
doubao:
  bot_name: "DJ小包"
  system_role: "你是一个DJ"
  voice: "zh_female_vv_jupiter_bigtts"
  asr:
    twopass: true
    end_smooth_ms: 800
    hotwords: ["火山引擎"]
`)
	cfg := runConfig{}
	if err := applyConfigFile(path, &cfg, map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	if cfg.GeminiSystemPrompt != "你是DJ" {
		t.Fatalf("gemini system prompt = %q", cfg.GeminiSystemPrompt)
	}
	if cfg.DoubaoBotName != "DJ小包" || cfg.DoubaoVoice != "zh_female_vv_jupiter_bigtts" {
		t.Fatalf("doubao persona/voice wrong: %+v", cfg)
	}
	if !cfg.DoubaoASRTwopass || cfg.DoubaoASREndSmoothMs != 800 || len(cfg.DoubaoHotwords) != 1 {
		t.Fatalf("doubao asr wrong: %+v", cfg)
	}
}

func TestApplyConfigFileGeminiTools(t *testing.T) {
	path := writeTemp(t, `
gemini:
  system_prompt: "你是助手"
  tools:
    - name: get_weather
      description: 查询天气
      parameters:
        type: object
        properties:
          city:
            type: string
        required: [city]
`)
	cfg := runConfig{}
	if err := applyConfigFile(path, &cfg, map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	if len(cfg.GeminiTools) != 1 {
		t.Fatalf("tools = %d, want 1", len(cfg.GeminiTools))
	}
	tool := cfg.GeminiTools[0]
	if tool.Name != "get_weather" || tool.Description != "查询天气" {
		t.Fatalf("tool meta wrong: %+v", tool)
	}
	if tool.Parameters["type"] != "object" {
		t.Fatalf("tool parameters not parsed: %+v", tool.Parameters)
	}
}

func TestApplyConfigFileFlagWins(t *testing.T) {
	path := writeTemp(t, `
cascade:
  system_prompt: "from file"
  tts_lang: "zh-cn"
`)
	cfg := runConfig{CascadeSystem: "from flag", CascadeTTSLang: "en"}
	// cascade-system was set on the CLI, cascade-tts-lang was not.
	set := map[string]bool{"cascade-system": true}
	if err := applyConfigFile(path, &cfg, set); err != nil {
		t.Fatal(err)
	}
	if cfg.CascadeSystem != "from flag" {
		t.Fatalf("set flag must win, got %q", cfg.CascadeSystem)
	}
	if cfg.CascadeTTSLang != "zh-cn" {
		t.Fatalf("unset flag should take file value, got %q", cfg.CascadeTTSLang)
	}
}

func TestApplyConfigFileMissingIsOK(t *testing.T) {
	cfg := runConfig{}
	if err := applyConfigFile(filepath.Join(t.TempDir(), "nope.yaml"), &cfg, map[string]bool{}); err != nil {
		t.Fatalf("missing config file must not error: %v", err)
	}
}
