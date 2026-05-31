// Package metrics holds process-wide, lock-free telemetry for load testing.
// The headline signal is the outbound frame-interval histogram: it measures the
// realized 20ms pacing cadence (ARCHITECTURE §3.1) under concurrency, which is
// this proxy's real SLO — it degrades before raw CPU saturates. Buckets are
// plain atomics so observing never perturbs the pacing it measures.
package metrics

import (
	"sync/atomic"
	"time"
)

// upper edges (ms) of all but the last (overflow) bucket.
var frameEdgesMs = [...]float64{20, 21, 23, 25, 30}

var frameLabels = [...]string{"<20ms", "20-21ms", "21-23ms", "23-25ms", "25-30ms", ">=30ms"}

var frameBuckets [len(frameLabels)]atomic.Uint64

// ObserveFrameInterval records the wall-clock gap between two consecutive
// outbound frames. Called once per frame on the media hot path; keep it cheap.
func ObserveFrameInterval(d time.Duration) {
	ms := float64(d) / float64(time.Millisecond)
	for i, edge := range frameEdgesMs {
		if ms < edge {
			frameBuckets[i].Add(1)
			return
		}
	}
	frameBuckets[len(frameEdgesMs)].Add(1)
}

// FrameIntervalBuckets returns a snapshot of the histogram, label -> count.
func FrameIntervalBuckets() map[string]uint64 {
	out := make(map[string]uint64, len(frameLabels))
	for i, l := range frameLabels {
		out[l] = frameBuckets[i].Load()
	}
	return out
}
