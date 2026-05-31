package loopback

import (
	"io"
	"testing"
)

func TestRecvReturnsNonSilentFrame(t *testing.T) {
	m := New()
	pcm, err := m.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if len(pcm) != frameSamples {
		t.Fatalf("frame size = %d, want %d", len(pcm), frameSamples)
	}
	nonzero := false
	for _, s := range pcm {
		if s != 0 {
			nonzero = true
			break
		}
	}
	if !nonzero {
		t.Error("tone frame is all silence; DTX would suppress it")
	}
}

func TestCloseStopsRecvAndIsIdempotent(t *testing.T) {
	m := New()
	m.Close()
	if _, err := m.Recv(); err != io.EOF {
		t.Errorf("Recv after Close = %v, want EOF", err)
	}
	if _, err := m.RecvTranscript(); err != io.EOF {
		t.Errorf("RecvTranscript after Close = %v, want EOF", err)
	}
	m.Close() // must not panic on double close
}
