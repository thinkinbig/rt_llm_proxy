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
var replaySources = [...]string{"memory", "kafka"}
var replayLatencyEdgesMs = [...]float64{20, 50, 100, 200, 300}
var replayLatencyLabels = [...]string{"<20ms", "20-50ms", "50-100ms", "100-200ms", "200-300ms", ">=300ms"}

var replayAttempts [len(replaySources)]atomic.Uint64
var replayHits [len(replaySources)]atomic.Uint64
var replayTimeouts [len(replaySources)]atomic.Uint64 // kafka-only in practice
var replayErrors [len(replaySources)]atomic.Uint64   // kafka-only in practice
var replayLatencyBuckets [len(replaySources)][len(replayLatencyLabels)]atomic.Uint64

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

// ObserveReplayAttempt records one replay attempt for source memory|kafka.
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

// ObserveReplayTimeout records a replay timeout (kafka path).
func ObserveReplayTimeout(source string) {
	if i, ok := replaySourceIndex(source); ok {
		replayTimeouts[i].Add(1)
	}
}

// ObserveReplayError records a replay error (kafka path).
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
