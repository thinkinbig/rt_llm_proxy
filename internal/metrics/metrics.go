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
var replaySources = [...]string{"memory", "index"}
var replayLatencyEdgesMs = [...]float64{20, 50, 100, 200, 300}
var replayLatencyLabels = [...]string{"<20ms", "20-50ms", "50-100ms", "100-200ms", "200-300ms", ">=300ms"}

var replayAttempts [len(replaySources)]atomic.Uint64
var replayHits [len(replaySources)]atomic.Uint64
var replayTimeouts [len(replaySources)]atomic.Uint64 // index-only in practice
var replayErrors [len(replaySources)]atomic.Uint64   // index-only in practice
var replayLatencyBuckets [len(replaySources)][len(replayLatencyLabels)]atomic.Uint64

var outboundFramesWritten atomic.Uint64
var outboundPumpExitWriteSample atomic.Uint64
var outboundPumpExitRecv atomic.Uint64
var outboundPumpExitCtx atomic.Uint64

// RecordOutboundFrameWritten counts one successful outbound WriteSample.
func RecordOutboundFrameWritten() { outboundFramesWritten.Add(1) }

// RecordOutboundPumpExit counts why an outbound media pump goroutine stopped.
// reason is one of: write_sample, recv, ctx.
func RecordOutboundPumpExit(reason string) {
	switch reason {
	case "write_sample":
		outboundPumpExitWriteSample.Add(1)
	case "recv":
		outboundPumpExitRecv.Add(1)
	case "ctx":
		outboundPumpExitCtx.Add(1)
	}
}

// OutboundMediaStats returns outbound pump counters for /stats and load tests.
func OutboundMediaStats() map[string]uint64 {
	return map[string]uint64{
		"frames_written":         outboundFramesWritten.Load(),
		"pump_exit_write_sample": outboundPumpExitWriteSample.Load(),
		"pump_exit_recv":         outboundPumpExitRecv.Load(),
		"pump_exit_ctx":          outboundPumpExitCtx.Load(),
	}
}

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

// ObserveReplayAttempt records one replay attempt for source memory|index.
func ObserveReplayAttempt(source string) {
	if i, ok := replaySourceIndex(source); ok {
		replayAttempts[i].Add(1)
	}
}

// ObserveReplayHit records a successful replay with its wall-clock latency.
func ObserveReplayHit(source string, d time.Duration) {
	i, ok := replaySourceIndex(source)
	if !ok {
		return
	}
	replayHits[i].Add(1)
	observeReplayLatency(i, d)
}

// ObserveReplayTimeout records a replay timeout (index path).
func ObserveReplayTimeout(source string) {
	if i, ok := replaySourceIndex(source); ok {
		replayTimeouts[i].Add(1)
	}
}

// ObserveReplayError records a replay error (index path).
func ObserveReplayError(source string) {
	if i, ok := replaySourceIndex(source); ok {
		replayErrors[i].Add(1)
	}
}

// ReplayStats returns replay counters and latency histograms by source.
func ReplayStats() map[string]map[string]any {
	out := make(map[string]map[string]any, len(replaySources))
	for i, source := range replaySources {
		lat := make(map[string]uint64, len(replayLatencyLabels))
		for j, label := range replayLatencyLabels {
			lat[label] = replayLatencyBuckets[i][j].Load()
		}
		out[source] = map[string]any{
			"attempts": replayAttempts[i].Load(),
			"hits":     replayHits[i].Load(),
			"timeouts": replayTimeouts[i].Load(),
			"errors":   replayErrors[i].Load(),
			"latency":  lat,
		}
	}
	return out
}

func replaySourceIndex(source string) (int, bool) {
	for i, s := range replaySources {
		if s == source {
			return i, true
		}
	}
	return 0, false
}

func observeReplayLatency(sourceIdx int, d time.Duration) {
	ms := float64(d) / float64(time.Millisecond)
	for i, edge := range replayLatencyEdgesMs {
		if ms < edge {
			replayLatencyBuckets[sourceIdx][i].Add(1)
			return
		}
	}
	replayLatencyBuckets[sourceIdx][len(replayLatencyEdgesMs)].Add(1)
}
