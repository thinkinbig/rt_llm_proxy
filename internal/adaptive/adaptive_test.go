package adaptive

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestSessionHysteresis(t *testing.T) {
	var got int
	c := &SessionController{
		comps:  []int{10, 5, 3},
		downAt: []int{40, 90},
		upAt:   []int{30, 75},
		set:    func(v int) { got = v },
	}
	c.set(c.comps[0]) // got = 10, idx = 0

	for i, s := range []struct{ n, want int }{
		{20, 10}, // below downAt[0]=40 -> stay
		{45, 5},  // >=40 -> step down to 5
		{50, 5},  // in band -> stay
		{95, 3},  // >=90 -> step down to 3
		{80, 3},  // 80 >= upAt[1]=75 -> hysteresis holds at 3
		{70, 5},  // 70 < 75 -> step up to 5
		{70, 5},  // 70 >= upAt[0]=30 -> stay
		{20, 10}, // < 30 -> step up to 10
	} {
		c.Step(s.n)
		if got != s.want {
			t.Errorf("step %d (n=%d): got complexity %d, want %d", i, s.n, got, s.want)
		}
	}
}

func TestDriftDwellAndHysteresis(t *testing.T) {
	var got int
	c := &DriftController{
		comps:  []int{10, 5, 3},
		highWM: 0.10,
		lowWM:  0.03,
		dwell:  2,
		set:    func(v int) { got = v },
	}
	c.set(c.comps[0]) // got = 10

	c.Step(0.20) // since=1 < dwell -> hold
	if got != 10 {
		t.Fatalf("dwell should hold the first high tick, got %d", got)
	}
	c.Step(0.20) // since=2 -> step down to 5
	if got != 5 {
		t.Fatalf("want 5, got %d", got)
	}
	c.Step(0.20) // since=1 -> hold
	if got != 5 {
		t.Fatalf("dwell hold, got %d", got)
	}
	c.Step(0.20) // since=2 -> step down to 3 (floor)
	if got != 3 {
		t.Fatalf("want 3, got %d", got)
	}
	c.Step(0.05) // in band [0.03,0.10] -> no change (still counts toward dwell)
	c.Step(0.00) // low -> step up to 5
	if got != 5 {
		t.Fatalf("recover to 5, got %d", got)
	}
}

func TestRecentDrift(t *testing.T) {
	last := map[string]uint64{"<20ms": 100, ">=30ms": 10}
	cur := map[string]uint64{"<20ms": 150, ">=30ms": 30}
	// delta total = 50 + 20 = 70, delta bad = 20
	if d := recentDrift(last, cur); math.Abs(d-20.0/70.0) > 1e-9 {
		t.Errorf("recentDrift = %v, want %v", d, 20.0/70.0)
	}
	if d := recentDrift(cur, cur); d != 0 {
		t.Errorf("no traffic -> drift 0, got %v", d)
	}
}

// Close must be idempotent: a second (or concurrent) call neither panics nor
// blocks. Run with -race to also catch a double close(stop).
func TestControllerCloseIdempotent(t *testing.T) {
	noop := func(int) {}
	session := NewSession(func() int { return 0 }, noop, []int{10, 5}, []int{40}, []int{30}, time.Hour)
	drift := NewDrift(func() map[string]uint64 { return nil }, noop, []int{10, 5}, 0.1, 0.03, 1, time.Hour)

	for _, c := range []interface{ Close() }{session, drift} {
		var wg sync.WaitGroup
		for range 3 {
			wg.Go(c.Close)
		}
		wg.Wait()
	}
}
