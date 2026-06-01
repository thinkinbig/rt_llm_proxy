// Package adaptive lowers Opus encoder complexity under load and raises it back,
// so quality is preserved when the proxy can afford it and CPU is shed
// gracefully when it can't. Two strategies, picked at startup:
//
//   - SessionController (proactive): complexity is a step function of the active
//     session count, with hysteresis. Degrades BEFORE pacing slips and cannot
//     oscillate (no feedback loop) — but session count is only a load proxy.
//   - DriftController (reactive): steps complexity down when the recent fraction
//     of frames missing their 20ms slot rises, back up when it falls. Tracks the
//     real SLO and self-calibrates, but is a feedback loop (lagging; damped with
//     hysteresis + a minimum dwell to avoid the kind of oscillation the shared
//     pacer experiment hit).
//
// Both run in one goroutine off the media path; a misbehaving controller can
// only mis-pick quality, never stall a session.
package adaptive

import (
	"sync"
	"time"
)

// SessionController maps active session count -> complexity via a descending
// step function. comps[0] is the best (ceiling); downAt[i]/upAt[i] are the
// hysteresis thresholds for the boundary between level i and i+1 (downAt > upAt).
type SessionController struct {
	count  func() int
	set    func(int)
	comps  []int
	downAt []int
	upAt   []int

	mu        sync.Mutex
	idx       int
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// NewSession starts a proactive controller. len(downAt) == len(upAt) == len(comps)-1.
func NewSession(count func() int, set func(int), comps, downAt, upAt []int, interval time.Duration) *SessionController {
	c := &SessionController{count: count, set: set, comps: comps, downAt: downAt, upAt: upAt, stop: make(chan struct{}), done: make(chan struct{})}
	set(comps[0])
	go loop(interval, c.stop, c.done, func() { c.Step(c.count()) })
	return c
}

// Step applies one decision for load n (exported for tests).
func (c *SessionController) Step(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.idx < len(c.comps)-1 && n >= c.downAt[c.idx]:
		c.idx++
	case c.idx > 0 && n < c.upAt[c.idx-1]:
		c.idx--
	default:
		return
	}
	c.set(c.comps[c.idx])
}

// Level returns the current complexity.
func (c *SessionController) Level() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.comps[c.idx]
}

// Close stops the controller goroutine and waits for it to exit. Idempotent.
func (c *SessionController) Close() {
	c.closeOnce.Do(func() { close(c.stop) })
	<-c.done
}

// DriftController steps complexity from the recent share of frames that missed
// their slot (delta of the >=30ms bucket over total). highWM > lowWM gives
// hysteresis; dwell is the minimum ticks between changes (damping).
type DriftController struct {
	buckets func() map[string]uint64
	set     func(int)
	comps   []int
	highWM  float64
	lowWM   float64
	dwell   int

	mu        sync.Mutex
	idx       int
	since     int
	last      map[string]uint64
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// NewDrift starts a reactive controller. highWM/lowWM are drift fractions [0,1].
func NewDrift(buckets func() map[string]uint64, set func(int), comps []int, highWM, lowWM float64, dwell int, interval time.Duration) *DriftController {
	c := &DriftController{buckets: buckets, set: set, comps: comps, highWM: highWM, lowWM: lowWM, dwell: dwell, last: buckets(), stop: make(chan struct{}), done: make(chan struct{})}
	set(comps[0])
	go loop(interval, c.stop, c.done, func() {
		cur := c.buckets()
		c.Step(recentDrift(c.last, cur))
		c.last = cur
	})
	return c
}

// Step takes the recent drift fraction [0,1] and adjusts at most one level,
// damped by dwell (exported for tests).
func (c *DriftController) Step(drift float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.since++
	if c.since < c.dwell {
		return
	}
	switch {
	case drift > c.highWM && c.idx < len(c.comps)-1:
		c.idx++
	case drift < c.lowWM && c.idx > 0:
		c.idx--
	default:
		return
	}
	c.set(c.comps[c.idx])
	c.since = 0
}

func (c *DriftController) Level() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.comps[c.idx]
}

// Close stops the controller goroutine and waits for it to exit. Idempotent.
func (c *DriftController) Close() {
	c.closeOnce.Do(func() { close(c.stop) })
	<-c.done
}

// recentDrift is delta(>=30ms) / delta(total) between two histogram snapshots.
func recentDrift(last, cur map[string]uint64) float64 {
	var total uint64
	for k, v := range cur {
		total += v - last[k]
	}
	if total == 0 {
		return 0
	}
	return float64(cur[">=30ms"]-last[">=30ms"]) / float64(total)
}

func loop(interval time.Duration, stop <-chan struct{}, done chan<- struct{}, tick func()) {
	defer close(done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			tick()
		}
	}
}
