package modelcb

import (
	"errors"
	"testing"
	"time"
)

func TestBreakerOpensAfterFailuresAndRecovers(t *testing.T) {
	m := New(Config{OpenAfter: 2, OpenFor: 10 * time.Second, HalfOpenSuccess: 2, AuthOpenFor: time.Minute}, nil)
	now := time.Unix(1000, 0)

	if !m.Allow("gemini", now).Allowed {
		t.Fatal("first request should pass")
	}
	m.Record("gemini", errors.New("timeout"), now)
	if !m.Allow("gemini", now).Allowed {
		t.Fatal("second request should pass before threshold")
	}
	m.Record("gemini", errors.New("timeout"), now)

	d := m.Allow("gemini", now)
	if d.Allowed || d.State != StateOpen {
		t.Fatalf("want open reject, got %+v", d)
	}

	half := m.Allow("gemini", now.Add(11*time.Second))
	if !half.Allowed || half.State != StateHalfOpen {
		t.Fatalf("want half-open probe, got %+v", half)
	}
	if m.Allow("gemini", now.Add(11*time.Second)).Allowed {
		t.Fatal("second half-open probe should be rejected")
	}
	m.Record("gemini", nil, now.Add(11*time.Second))
	if !m.Allow("gemini", now.Add(11*time.Second)).Allowed {
		t.Fatal("second probe should be allowed")
	}
	m.Record("gemini", nil, now.Add(12*time.Second))
	if !m.Allow("gemini", now.Add(12*time.Second)).Allowed {
		t.Fatal("breaker should be closed again")
	}
}

func TestAuthFailureOpensImmediately(t *testing.T) {
	m := New(Config{OpenAfter: 5, OpenFor: time.Second, HalfOpenSuccess: 1, AuthOpenFor: 5 * time.Minute}, nil)
	now := time.Unix(2000, 0)
	m.Record("doubao", errors.New("upstream 401 unauthorized"), now)
	d := m.Allow("doubao", now)
	if d.Allowed || d.Reason != "auth" {
		t.Fatalf("want immediate auth open, got %+v", d)
	}
	if d.RetryAfter < 4*time.Minute {
		t.Fatalf("auth breaker open duration too short: %v", d.RetryAfter)
	}
}
