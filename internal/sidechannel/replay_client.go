package sidechannel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
)

// ReplayClient queries the replay-index HTTP service for cross-node reconnect.
type ReplayClient struct {
	base   string
	client *http.Client
}

// NewReplayClient targets a replay-index service base URL (e.g. http://replay:8090).
func NewReplayClient(baseURL string) *ReplayClient {
	return &ReplayClient{
		base:   strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		client: &http.Client{},
	}
}

// Replay implements offer.Replayer against the replay-index service.
func (c *ReplayClient) Replay(ctx context.Context, sessionID identity.SessionID, userID identity.UserID, provider string, afterSeq uint64, limit int) ([]*TranscriptEvent, error) {
	if c == nil || c.base == "" || sessionID == "" || userID.Anonymous() {
		return nil, nil
	}
	if limit <= 0 {
		limit = 256
	}
	u, err := url.Parse(c.base + "/v1/replay")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("session_id", string(sessionID))
	q.Set("user_id", string(userID))
	q.Set("provider", provider)
	q.Set("after_seq", strconv.FormatUint(afterSeq, 10))
	q.Set("limit", strconv.Itoa(limit))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("replay-index: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var payload struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	out := make([]*TranscriptEvent, 0, len(payload.Events))
	for _, raw := range payload.Events {
		ev := &TranscriptEvent{}
		if err := protojson.Unmarshal(raw, ev); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, nil
}
