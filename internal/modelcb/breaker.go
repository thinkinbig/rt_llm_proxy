// Package modelcb provides lightweight circuit breakers for model connect
// attempts. It protects the offer path from repeatedly dialing an unhealthy
// upstream provider.
package modelcb

import (
	"strings"
	"sync"
	"time"
)

type State string

const (
	StateClosed   State = "closed"
	StateOpen     State = "open"
	StateHalfOpen State = "half_open"
)

type Config struct {
	OpenAfter       int
	OpenFor         time.Duration
	HalfOpenSuccess int
	AuthOpenFor     time.Duration
}

type Decision struct {
	Allowed    bool
	State      State
	Reason     string
	RetryAfter time.Duration
}

type breaker struct {
	cfg Config

	state     State
	reason    string
	openUntil time.Time

	failures int
	success  int

	halfOpenProbeInFlight bool

	mu sync.Mutex
}

type Manager struct {
	mu       sync.Mutex
	defaults Config
	provider map[string]*breaker
}

func New(defaults Config, overrides map[string]Config) *Manager {
	m := &Manager{
		defaults: normalize(defaults),
		provider: make(map[string]*breaker),
	}
	for p, cfg := range overrides {
		c := normalize(merge(m.defaults, cfg))
		m.provider[p] = &breaker{cfg: c, state: StateClosed}
	}
	return m
}

func (m *Manager) Allow(provider string, now time.Time) Decision {
	b := m.get(provider)
	return b.allow(now)
}

func (m *Manager) Record(provider string, err error, now time.Time) {
	if err == nil {
		m.get(provider).recordSuccess()
		return
	}
	m.get(provider).recordFailure(classify(err), now)
}

func (m *Manager) Stats() map[string]map[string]any {
	m.mu.Lock()
	snapshot := make(map[string]*breaker, len(m.provider))
	for p, b := range m.provider {
		snapshot[p] = b
	}
	m.mu.Unlock()

	out := make(map[string]map[string]any, len(snapshot))
	now := time.Now()
	for p, b := range snapshot {
		b.mu.Lock()
		retry := int64(0)
		if b.state == StateOpen {
			if d := b.openUntil.Sub(now); d > 0 {
				retry = int64(d / time.Second)
			}
		}
		out[p] = map[string]any{
			"state":             string(b.state),
			"reason":            b.reason,
			"retry_after_sec":   retry,
			"failures":          b.failures,
			"half_open_success": b.success,
		}
		b.mu.Unlock()
	}
	return out
}

func (m *Manager) get(provider string) *breaker {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok := m.provider[provider]; ok {
		return b
	}
	b := &breaker{cfg: m.defaults, state: StateClosed}
	m.provider[provider] = b
	return b
}

func (b *breaker) allow(now time.Time) Decision {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return Decision{Allowed: true, State: StateClosed}
	case StateOpen:
		if now.Before(b.openUntil) {
			return Decision{
				Allowed:    false,
				State:      StateOpen,
				Reason:     b.reason,
				RetryAfter: b.openUntil.Sub(now),
			}
		}
		b.state = StateHalfOpen
		b.success = 0
		b.halfOpenProbeInFlight = false
		fallthrough
	case StateHalfOpen:
		if b.halfOpenProbeInFlight {
			return Decision{Allowed: false, State: StateHalfOpen, Reason: b.reason}
		}
		b.halfOpenProbeInFlight = true
		return Decision{Allowed: true, State: StateHalfOpen}
	default:
		return Decision{Allowed: true, State: StateClosed}
	}
}

func (b *breaker) recordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case StateClosed:
		b.failures = 0
	case StateHalfOpen:
		b.halfOpenProbeInFlight = false
		b.success++
		if b.success >= b.cfg.HalfOpenSuccess {
			b.state = StateClosed
			b.reason = ""
			b.failures = 0
			b.success = 0
		}
	}
}

func (b *breaker) recordFailure(reason string, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	open := false
	openFor := b.cfg.OpenFor
	if reason == "auth" {
		open = true
		openFor = b.cfg.AuthOpenFor
	}

	switch b.state {
	case StateHalfOpen:
		b.halfOpenProbeInFlight = false
		open = true
	case StateClosed:
		if !open {
			b.failures++
			if b.failures >= b.cfg.OpenAfter {
				open = true
			}
		}
	case StateOpen:
		// Already open; keep existing timer.
		return
	}
	if !open {
		return
	}
	b.state = StateOpen
	b.reason = reason
	b.openUntil = now.Add(openFor)
	b.failures = 0
	b.success = 0
}

func classify(err error) string {
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "401") ||
		strings.Contains(s, "403") ||
		strings.Contains(s, "unauthorized") ||
		strings.Contains(s, "forbidden") {
		return "auth"
	}
	if strings.Contains(s, "timeout") ||
		strings.Contains(s, "429") ||
		strings.Contains(s, "502") ||
		strings.Contains(s, "503") ||
		strings.Contains(s, "504") ||
		strings.Contains(s, "connection reset") {
		return "transient"
	}
	return "other"
}

func normalize(c Config) Config {
	if c.OpenAfter <= 0 {
		c.OpenAfter = 5
	}
	if c.OpenFor <= 0 {
		c.OpenFor = 30 * time.Second
	}
	if c.HalfOpenSuccess <= 0 {
		c.HalfOpenSuccess = 3
	}
	if c.AuthOpenFor <= 0 {
		c.AuthOpenFor = 5 * time.Minute
	}
	return c
}

func merge(base, ov Config) Config {
	if ov.OpenAfter > 0 {
		base.OpenAfter = ov.OpenAfter
	}
	if ov.OpenFor > 0 {
		base.OpenFor = ov.OpenFor
	}
	if ov.HalfOpenSuccess > 0 {
		base.HalfOpenSuccess = ov.HalfOpenSuccess
	}
	if ov.AuthOpenFor > 0 {
		base.AuthOpenFor = ov.AuthOpenFor
	}
	return base
}
