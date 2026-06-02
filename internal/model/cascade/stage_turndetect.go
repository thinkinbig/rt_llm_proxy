package cascade

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// defaultPause is used when the sidecar is unreachable or returns an error.
const defaultPause = 400 * time.Millisecond

// NopTurnDetector fires the LLM immediately after ASR final (legacy behaviour).
type NopTurnDetector struct{}

func (NopTurnDetector) SuggestedPause(_ context.Context, _ string) time.Duration {
	return 0
}

// HTTPTurnDetector calls the turndetect Python sidecar to get a dynamic pause.
type HTTPTurnDetector struct {
	url    string
	client *http.Client
}

// NewHTTPTurnDetector creates a detector that calls baseURL (e.g.
// "http://localhost:7000"). It times out after 200ms to stay off the hot path.
func NewHTTPTurnDetector(baseURL string) *HTTPTurnDetector {
	return &HTTPTurnDetector{
		url:    strings.TrimRight(baseURL, "/") + "/detect",
		client: &http.Client{Timeout: 200 * time.Millisecond},
	}
}

type turnDetectRequest struct {
	Text string `json:"text"`
}

type turnDetectResponse struct {
	PauseMs int `json:"pause_ms"`
}

func (d *HTTPTurnDetector) SuggestedPause(ctx context.Context, text string) time.Duration {
	body, _ := json.Marshal(turnDetectRequest{Text: text})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(body))
	if err != nil {
		return defaultPause
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return defaultPause
	}
	defer resp.Body.Close()

	var r turnDetectResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return defaultPause
	}
	return time.Duration(r.PauseMs) * time.Millisecond
}
