package doubao

import (
	"testing"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

// TestDialogContext verifies the reconnect history is mapped to Doubao's
// dialog_context shape: the model role becomes "assistant", everything else
// maps to "user", and an empty history yields nil (field omitted).
func TestDialogContext(t *testing.T) {
	if got := dialogContext(nil); got != nil {
		t.Fatalf("empty history -> %+v, want nil", got)
	}

	got := dialogContext([]model.RestoredTurn{
		{Role: "user", Text: "hi"},
		{Role: "model", Text: "hello"},
		{Role: "weird", Text: "x"},
	})
	want := []map[string]string{
		{"role": "user", "text": "hi"},
		{"role": "assistant", "text": "hello"},
		{"role": "user", "text": "x"},
	}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i]["role"] != want[i]["role"] || got[i]["text"] != want[i]["text"] {
			t.Errorf("turn[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}
