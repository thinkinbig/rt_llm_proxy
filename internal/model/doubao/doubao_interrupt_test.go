package doubao

import (
	"context"
	"testing"
)

func TestSignalInterruptedDrainsAndSignals(t *testing.T) {
	d := &Doubao{
		ctx:           context.Background(),
		recvCh:        make(chan []int16, 4),
		interruptedCh: make(chan struct{}, 1),
	}
	d.recvCh <- []int16{1, 2}
	d.recvCh <- []int16{3, 4}

	d.signalInterrupted()

	// Queued model audio is dropped so playback stops immediately.
	if len(d.recvCh) != 0 {
		t.Fatalf("recvCh not drained: %d left", len(d.recvCh))
	}
	// Bridge poll observes the barge-in exactly once, then false.
	if got, _ := d.RecvInterrupted(); !got {
		t.Fatal("RecvInterrupted = false, want true after signal")
	}
	if got, _ := d.RecvInterrupted(); got {
		t.Fatal("RecvInterrupted = true on second poll, want false")
	}
}

func TestSupportsInterruption(t *testing.T) {
	d := &Doubao{}
	if !d.SupportsInterruption() {
		t.Fatal("Doubao should support interruption (server VAD event 450)")
	}
}
