// Package llm provides concrete cascade.LLM stage implementations.
package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade"
)

// OpenAI is a streaming LLM stage backed by a vLLM endpoint serving
// any model via the OpenAI-compatible chat completions API.
//
// Wire protocol: POST /v1/chat/completions with stream=true.
// Server sends Server-Sent Events; each data line is a JSON chunk with
// choices[0].delta.content holding the token text. The stream ends with
// data: [DONE].
type OpenAI struct {
	baseURL    string
	model      string
	httpClient *http.Client
}

// openAIRequest is the request body for /v1/chat/completions.
type openAIRequest struct {
	Model    string          `json:"model"`
	Messages []openAIMessage `json:"messages"`
	Stream   bool            `json:"stream"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// openAIChunk is one SSE data payload from the streaming response.
type openAIChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

// New creates an OpenAI LLM that calls the vLLM endpoint at baseURL
// (e.g. "http://localhost:8000") using the given model name.
func New(baseURL, model string) *OpenAI {
	return &OpenAI{
		baseURL:    strings.TrimRight(baseURL, "/"),
		model:      model,
		httpClient: &http.Client{},
	}
}

// Generate streams reply tokens from the vLLM endpoint. The returned channel
// closes when the reply is complete or ctx is cancelled (barge-in).
// On a transient error (network / 5xx) it retries once before giving up.
func (d *OpenAI) Generate(ctx context.Context, history []cascade.Message) (<-chan string, error) {
	msgs := make([]openAIMessage, len(history))
	for i, m := range history {
		role := m.Role
		if role == "model" {
			role = "assistant"
		}
		msgs[i] = openAIMessage{Role: role, Content: m.Text}
	}

	body, err := json.Marshal(openAIRequest{
		Model:    d.model,
		Messages: msgs,
		Stream:   true,
	})
	if err != nil {
		return nil, fmt.Errorf("openai_llm marshal: %w", err)
	}

	var resp *http.Response
	for attempt := range 2 {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			d.baseURL+"/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("openai_llm request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err = d.httpClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		if attempt == 1 {
			if err != nil {
				return nil, fmt.Errorf("openai_llm http (retry exhausted): %w", err)
			}
			return nil, fmt.Errorf("openai_llm status %d (retry exhausted)", resp.StatusCode)
		}
		// transient — retry once
	}

	ch := make(chan string, 16)
	go func() {
		defer close(ch)
		defer resp.Body.Close()
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			payload := strings.TrimPrefix(line, "data: ")
			if payload == "[DONE]" {
				return
			}
			var chunk openAIChunk
			if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
				continue
			}
			if len(chunk.Choices) == 0 {
				continue
			}
			token := chunk.Choices[0].Delta.Content
			if token == "" {
				continue
			}
			select {
			case ch <- token:
			case <-ctx.Done():
				return
			}
		}
		_ = scanner.Err()
	}()

	return ch, nil
}

func (d *OpenAI) Close() error { return nil }

var _ cascade.LLM = (*OpenAI)(nil)
