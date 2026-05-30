// Package ratelimit provides a tiny Redis fixed-window limiter for the SDP offer
// endpoint (sessions per key per window). It sits purely on the control plane —
// media never touches it. If no Redis address is configured it allows everything.
package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	rdb    *redis.Client
	max    int
	window time.Duration
}

// New returns a limiter. addr="" disables limiting (Allow always true).
func New(addr string, max int, window time.Duration) *Limiter {
	if addr == "" {
		return &Limiter{}
	}
	return &Limiter{
		rdb:    redis.NewClient(&redis.Options{Addr: addr}),
		max:    max,
		window: window,
	}
}

// Allow reports whether key may start another session in the current window.
func (l *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	if l.rdb == nil {
		return true, nil
	}
	k := "rl:" + key
	n, err := l.rdb.Incr(ctx, k).Result()
	if err != nil {
		return false, err
	}
	if n == 1 {
		l.rdb.Expire(ctx, k, l.window)
	}
	return n <= int64(l.max), nil
}
