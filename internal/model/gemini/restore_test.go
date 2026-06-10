package gemini

import (
	"encoding/json"
	"testing"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

// TestRestoreClientContent verifies the reconnect payload: turnComplete is false
// (seeding context must not trigger a reply) and prior turns are replayed in
// order with roles mapped to the Gemini user/model vocabulary.
func TestRestoreClientContent(t *testing.T) {
	turns := []model.RestoredTurn{
		{Role: "user", Text: "capital of france?"},
		{Role: "model", Text: "paris"},
		{Role: "weird", Text: "treated as user"},
	}
	msg := restoreClientContent(turns)

	if msg.ClientContent.TurnComplete {
		t.Fatal("turnComplete must be false so restore does not prompt a reply")
	}
	if len(msg.ClientContent.Turns) != 3 {
		t.Fatalf("turns = %d, want 3", len(msg.ClientContent.Turns))
	}
	wantRoles := []string{"user", "model", "user"}
	wantText := []string{"capital of france?", "paris", "treated as user"}
	for i, turn := range msg.ClientContent.Turns {
		if turn.Role != wantRoles[i] {
			t.Errorf("turn[%d].Role = %q, want %q", i, turn.Role, wantRoles[i])
		}
		if len(turn.Parts) != 1 || turn.Parts[0].Text != wantText[i] {
			t.Errorf("turn[%d].Parts = %+v, want text %q", i, turn.Parts, wantText[i])
		}
	}

	// Round-trips to the wire shape the Live API expects.
	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	cc, ok := back["clientContent"].(map[string]any)
	if !ok {
		t.Fatalf("no clientContent in %s", b)
	}
	if tc, _ := cc["turnComplete"].(bool); tc {
		t.Errorf("wire turnComplete = true, want false: %s", b)
	}
}
