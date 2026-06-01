package metrics

import (
	"testing"
	"time"
)

func TestOutboundMediaStats(t *testing.T) {
	RecordOutboundFrameWritten()
	RecordOutboundFrameWritten()
	RecordOutboundPumpExit("write_sample")
	RecordOutboundPumpExit("recv")
	RecordOutboundPumpExit("ctx")

	s := OutboundMediaStats()
	if s["frames_written"] != 2 {
		t.Fatalf("frames_written = %d, want 2", s["frames_written"])
	}
	if s["pump_exit_write_sample"] != 1 || s["pump_exit_recv"] != 1 || s["pump_exit_ctx"] != 1 {
		t.Fatalf("pump exits = %+v", s)
	}
}

func TestObserveFrameIntervalBuckets(t *testing.T) {
	ObserveFrameInterval(19 * time.Millisecond)                      // <20ms
	ObserveFrameInterval(20*time.Millisecond + 500*time.Microsecond) // 20-21ms
	ObserveFrameInterval(24 * time.Millisecond)                      // 23-25ms
	ObserveFrameInterval(40 * time.Millisecond)                      // >=30ms (drift)

	b := FrameIntervalBuckets()
	for label, want := range map[string]uint64{
		"<20ms":   1,
		"20-21ms": 1,
		"23-25ms": 1,
		">=30ms":  1,
	} {
		if b[label] != want {
			t.Errorf("bucket %q = %d, want %d (all: %+v)", label, b[label], want, b)
		}
	}
}
