package sidechannel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
)

func TestReplayClient(t *testing.T) {
	ev := &TranscriptEvent{
		SessionId: "s1",
		UserId:    "alice",
		Provider:  "gemini",
		Seq:       2,
		Role:      Role_ROLE_MODEL,
		Text:      "hey",
	}
	raw, err := protojson.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/replay" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("session_id") != "s1" || r.URL.Query().Get("after_seq") != "1" {
			t.Fatalf("query: %s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"events": []json.RawMessage{raw}})
	}))
	defer srv.Close()

	client := NewReplayClient(srv.URL)
	got, err := client.Replay(context.Background(), "s1", "alice", "gemini", 1, 10)
	if err != nil || len(got) != 1 || got[0].GetSeq() != 2 {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestReplayClientAnonymous(t *testing.T) {
	client := NewReplayClient("http://example.com")
	got, err := client.Replay(context.Background(), "s1", identity.UserID(""), "gemini", 0, 10)
	if err != nil || got != nil {
		t.Fatalf("got %+v, %v", got, err)
	}
}
