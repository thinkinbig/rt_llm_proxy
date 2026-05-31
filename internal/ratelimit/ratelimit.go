// Package ratelimit provides a tiny Redis fixed-window limiter for the SDP offer
// endpoint (sessions per key per window). It sits purely on the control plane —
// media never touches it. If no Redis address is configured it allows everything.
package ratelimit

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// incrExpire atomically increments the counter and, on the first hit of a new
// window, sets its TTL. Doing both in one script closes the crash window of a
// separate INCR + EXPIRE: a process death between the two would leave the key
// with no TTL, so the counter would never reset and the IP would be locked out
// permanently.
var incrExpire = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return n
`)

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
// On a Redis error it fails open (allows the session) rather than rejecting:
// rate limiting is a soft guard on the control plane, and a Redis blip should
// not take down the real-time service. The error is returned for logging only.
func (l *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	if l.rdb == nil {
		return true, nil
	}
	n, err := incrExpire.Run(ctx, l.rdb, []string{"rl:" + key}, int(l.window.Seconds())).Int64()
	if err != nil {
		log.Printf("ratelimit: redis error, failing open: %v", err)
		return true, err
	}
	return n <= int64(l.max), nil
}
