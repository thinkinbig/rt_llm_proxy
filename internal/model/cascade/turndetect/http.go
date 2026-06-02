// Package turndetect provides concrete cascade.TurnDetector implementations.
// The no-op default lives in the engine package; this package holds the
// sidecar-backed detector.
package turndetect

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

// HTTP calls the turndetect Python sidecar to get a dynamic pause.
type HTTP struct {
	url    string
	client *http.Client
}

// NewHTTP creates a detector that calls baseURL (e.g. "http://localhost:7000").
// It times out after 200ms to stay off the hot path.
func NewHTTP(baseURL string) *HTTP {
	return &HTTP{
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

func (d *HTTP) SuggestedPause(ctx context.Context, text string) time.Duration {
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
