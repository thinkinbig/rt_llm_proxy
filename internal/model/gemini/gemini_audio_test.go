package gemini

import "testing"

func TestInlineAudioToModelPCM(t *testing.T) {
	// s16le, rate from MIME: 100 samples (200 bytes).
	in := make([]byte, 100*2)
	if got := inlineAudioToModelPCM(in, "audio/pcm;rate=24000"); len(got) != 200 {
		t.Errorf("rate=24000: len = %d, want 200 (24k->48k)", len(got))
	}
	if got := inlineAudioToModelPCM(in, "audio/pcm;rate=48000"); len(got) != 100 {
		t.Errorf("rate=48000: len = %d, want 100 (no resample)", len(got))
	}
}
