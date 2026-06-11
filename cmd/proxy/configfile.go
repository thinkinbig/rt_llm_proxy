package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/thinkinbig/rt-llm-proxy/internal/model/gemini"
)

// fileConfig mirrors the YAML config file. It covers provider behavior only;
// infrastructure knobs (rate limiting, circuit breaker, kafka, …) stay on CLI
// flags and environment variables. Credentials are never read from here.
type fileConfig struct {
	Gemini struct {
		SystemPrompt string `yaml:"system_prompt"`
		Tools        []struct {
			Name        string         `yaml:"name"`
			Description string         `yaml:"description"`
			Parameters  map[string]any `yaml:"parameters"`
		} `yaml:"tools"`
	} `yaml:"gemini"`
	Doubao struct {
		Model         string `yaml:"model"`
		BotName       string `yaml:"bot_name"`
		SystemRole    string `yaml:"system_role"`
		SpeakingStyle string `yaml:"speaking_style"`
		Voice         string `yaml:"voice"`
		ASR           struct {
			Twopass     bool     `yaml:"twopass"`
			EndSmoothMs int      `yaml:"end_smooth_ms"`
			Hotwords    []string `yaml:"hotwords"`
		} `yaml:"asr"`
	} `yaml:"doubao"`
	Cascade struct {
		SystemPrompt string `yaml:"system_prompt"`
		TTSSpeaker   string `yaml:"tts_speaker"`
		TTSLang      string `yaml:"tts_lang"`
		LLMModel     string `yaml:"llm_model"`
	} `yaml:"cascade"`
}

// applyConfigFile loads the YAML config at path and folds it into cfg. A missing
// file is not an error — the config file is optional. Precedence: a CLI flag
// explicitly set (present in setFlags) always wins; otherwise the config file
// value applies. Provider-behavior fields have no flags, so the file is their
// sole source.
func applyConfigFile(path string, cfg *runConfig, setFlags map[string]bool) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read %s: %w", path, err)
	}
	var fc fileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}

	// Provider behavior — no CLI flags, so the file is authoritative.
	cfg.GeminiSystemPrompt = fc.Gemini.SystemPrompt
	cfg.GeminiTools = nil
	for _, td := range fc.Gemini.Tools {
		cfg.GeminiTools = append(cfg.GeminiTools, gemini.FunctionDeclaration{
			Name:        td.Name,
			Description: td.Description,
			Parameters:  td.Parameters,
		})
	}
	cfg.DoubaoModelVersion = fc.Doubao.Model
	cfg.DoubaoBotName = fc.Doubao.BotName
	cfg.DoubaoSystemRole = fc.Doubao.SystemRole
	cfg.DoubaoSpeakingStyle = fc.Doubao.SpeakingStyle
	cfg.DoubaoVoice = fc.Doubao.Voice
	cfg.DoubaoASRTwopass = fc.Doubao.ASR.Twopass
	cfg.DoubaoASREndSmoothMs = fc.Doubao.ASR.EndSmoothMs
	cfg.DoubaoHotwords = fc.Doubao.ASR.Hotwords

	// Cascade fields back existing flags, so a set flag wins over the file.
	if !setFlags["cascade-system"] && fc.Cascade.SystemPrompt != "" {
		cfg.CascadeSystem = fc.Cascade.SystemPrompt
	}
	if !setFlags["cascade-tts-speaker"] && fc.Cascade.TTSSpeaker != "" {
		cfg.CascadeTTSSpeaker = fc.Cascade.TTSSpeaker
	}
	if !setFlags["cascade-tts-lang"] && fc.Cascade.TTSLang != "" {
		cfg.CascadeTTSLang = fc.Cascade.TTSLang
	}
	if !setFlags["cascade-llm-model"] && fc.Cascade.LLMModel != "" {
		cfg.CascadeLLMModel = fc.Cascade.LLMModel
	}
	return nil
}
