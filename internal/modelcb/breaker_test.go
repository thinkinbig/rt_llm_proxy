package modelcb

import (
	"errors"
	"testing"
	"time"
)

func TestBreakerOpensAfterFailuresAndRecovers(t *testing.T) {
	m := New(Config{OpenAfter: 2, OpenFor: 10 * time.Second, HalfOpenSuccess: 2, AuthOpenFor: time.Minute}, nil)
	now := time.Unix(1000, 0)

	if !m.AllowDial("gemini", now).Allowed {
		t.Fatal("first request should pass")
	}
	m.RecordDial("gemini", errors.New("timeout"), now)
	if !m.AllowDial("gemini", now).Allowed {
		t.Fatal("second request should pass before threshold")
	}
	m.RecordDial("gemini", errors.New("timeout"), now)

	d := m.AllowDial("gemini", now)
	if d.Allowed || d.State != StateOpen {
		t.Fatalf("want open reject, got %+v", d)
	}

	half := m.AllowDial("gemini", now.Add(11*time.Second))
	if !half.Allowed || half.State != StateHalfOpen {
		t.Fatalf("want half-open probe, got %+v", half)
	}
	if m.AllowDial("gemini", now.Add(11*time.Second)).Allowed {
		t.Fatal("second half-open probe should be rejected")
	}
	m.RecordDial("gemini", nil, now.Add(11*time.Second))
	if !m.AllowDial("gemini", now.Add(11*time.Second)).Allowed {
		t.Fatal("second probe should be allowed")
	}
	m.RecordDial("gemini", nil, now.Add(12*time.Second))
	if !m.AllowDial("gemini", now.Add(12*time.Second)).Allowed {
		t.Fatal("breaker should be closed again")
	}
}

func TestDialSuccessResetsFailures(t *testing.T) {
	m := New(Config{OpenAfter: 2, OpenFor: time.Minute, HalfOpenSuccess: 1, AuthOpenFor: time.Minute}, nil)
	now := time.Unix(1500, 0)
	m.RecordDial("gemini", errors.New("timeout"), now)
	m.RecordDial("gemini", nil, now.Add(time.Second))
	m.RecordDial("gemini", errors.New("timeout"), now.Add(2*time.Second))
	if !m.AllowDial("gemini", now.Add(2*time.Second)).Allowed {
		t.Fatal("success should reset dial failure streak")
	}
}

func TestAuthFailureOpensImmediately(t *testing.T) {
	m := New(Config{OpenAfter: 5, OpenFor: time.Second, HalfOpenSuccess: 1, AuthOpenFor: 5 * time.Minute}, nil)
	now := time.Unix(2000, 0)
	m.RecordDial("doubao", errors.New("upstream 401 unauthorized"), now)
	d := m.AllowDial("doubao", now)
	if d.Allowed || d.Reason != "auth" {
		t.Fatalf("want immediate auth open, got %+v", d)
	}
	if d.RetryAfter < 4*time.Minute {
		t.Fatalf("auth breaker open duration too short: %v", d.RetryAfter)
	}
}

func TestLoopbackSkipsGuard(t *testing.T) {
	m := New(Config{OpenAfter: 1, OpenFor: time.Hour, HalfOpenSuccess: 1, AuthOpenFor: time.Hour}, nil)
	now := time.Unix(3000, 0)
	m.RecordDial("loopback", errors.New("timeout"), now)
	if !m.AllowDial("loopback", now).Allowed {
		t.Fatal("loopback dial should not be gated")
	}
	report := m.StreamFaultBinder("loopback")
	if report != nil {
		t.Fatal("loopback should not install stream fault reporter")
	}
}

func TestRecordStreamFaultRespectsWindowAndAudio(t *testing.T) {
	m := New(Config{OpenAfter: 3, OpenFor: time.Minute, HalfOpenSuccess: 1, AuthOpenFor: time.Minute}, nil)
	start := time.Unix(4000, 0)

	m.RecordStreamFault("gemini", start, true, errors.New("timeout"), start.Add(time.Second))
	if !m.AllowDial("gemini", start).Allowed {
		t.Fatal("fault after audio should not open circuit")
	}

	m.RecordStreamFault("gemini", start, false, errors.New("timeout"), start.Add(2*time.Second))
	m.RecordStreamFault("gemini", start, false, errors.New("timeout"), start.Add(3*time.Second))
	if !m.AllowDial("gemini", start.Add(3*time.Second)).Allowed {
		t.Fatal("two early faults below threshold should stay closed")
	}
	m.RecordStreamFault("gemini", start, false, errors.New("timeout"), start.Add(4*time.Second))
	if m.AllowDial("gemini", start.Add(4*time.Second)).Allowed {
		t.Fatal("third early fault should open circuit")
	}

	m.RecordStreamFault("gemini", start, false, errors.New("timeout"), start.Add(EarlyFaultWindow+time.Second))
	if m.AllowDial("gemini", start.Add(EarlyFaultWindow+time.Second)).Allowed {
		t.Fatal("fault outside window should not count toward open")
	}
}

func TestDialAndStreamFailuresAreIndependent(t *testing.T) {
	m := New(Config{OpenAfter: 2, OpenFor: time.Minute, HalfOpenSuccess: 1, AuthOpenFor: time.Minute}, nil)
	start := time.Unix(4100, 0)

	m.RecordDial("gemini", errors.New("timeout"), start)
	m.RecordStreamFault("gemini", start, false, errors.New("connection reset"), start.Add(time.Second))
	if !m.AllowDial("gemini", start.Add(time.Second)).Allowed {
		t.Fatal("single dial + single stream failure should not open circuit")
	}
	m.RecordDial("gemini", errors.New("timeout"), start.Add(2*time.Second))
	d := m.AllowDial("gemini", start.Add(2*time.Second))
	if d.Allowed || d.Reason != "transient" {
		t.Fatalf("want open from dial streak, got %+v", d)
	}
}

func TestStreamFaultBinder(t *testing.T) {
	m := New(Config{OpenAfter: 1, OpenFor: time.Minute, HalfOpenSuccess: 1, AuthOpenFor: time.Minute}, nil)
	start := time.Unix(5000, 0)
	m.now = func() time.Time { return start.Add(time.Second) }
	bind := m.StreamFaultBinder("gemini")
	if bind == nil {
		t.Fatal("want binder")
	}
	report := bind(start)
	report(false, errors.New("connection reset"))
	if m.AllowDial("gemini", start).Allowed {
		t.Fatal("binder report should open circuit")
	}
}
