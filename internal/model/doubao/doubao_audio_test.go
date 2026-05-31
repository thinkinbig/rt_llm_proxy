package doubao

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestF32leToPCM(t *testing.T) {
	in := make([]byte, 16)
	for i, f := range []float32{0, 1, -1, 0.5} {
		binary.LittleEndian.PutUint32(in[i*4:], math.Float32bits(f))
	}
	got := f32leToPCM(in)
	want := []int16{0, 32767, -32767, 16383}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sample %d = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestTTSToModelPCM(t *testing.T) {
	// f32le @ 24kHz: 100 samples (400 bytes) -> 200 samples @ 48kHz.
	got := ttsToModelPCM(make([]byte, 100*4))
	if len(got) != 200 {
		t.Fatalf("len = %d, want 200 (24k->48k of 100 f32 samples)", len(got))
	}
}
