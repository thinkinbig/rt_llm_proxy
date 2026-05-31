package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestAllowRejectsAtMax(t *testing.T) {
	s := miniredis.RunT(t)
	l := New(s.Addr(), 3, time.Minute)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		ok, err := l.Allow(ctx, "1.2.3.4")
		if err != nil || !ok {
			t.Fatalf("hit %d: want allowed, got ok=%v err=%v", i, ok, err)
		}
	}
	ok, err := l.Allow(ctx, "1.2.3.4")
	if err != nil {
		t.Fatalf("4th hit: unexpected err=%v", err)
	}
	if ok {
		t.Fatal("4th hit: want rejected (over max), got allowed")
	}
}

func TestAllowResetsAfterWindow(t *testing.T) {
	s := miniredis.RunT(t)
	l := New(s.Addr(), 1, time.Minute)
	ctx := context.Background()

	if ok, _ := l.Allow(ctx, "ip"); !ok {
		t.Fatal("first hit should be allowed")
	}
	if ok, _ := l.Allow(ctx, "ip"); ok {
		t.Fatal("second hit in window should be rejected")
	}

	// The atomic script must have set a TTL; advancing past it resets the window.
	s.FastForward(time.Minute + time.Second)
	if ok, _ := l.Allow(ctx, "ip"); !ok {
		t.Fatal("hit after window should be allowed again")
	}
}

func TestAllowFailsOpenOnRedisError(t *testing.T) {
	s := miniredis.RunT(t)
	l := New(s.Addr(), 1, time.Minute)
	s.Close() // make Redis unreachable

	ok, err := l.Allow(context.Background(), "ip")
	if err == nil {
		t.Fatal("want a redis error to be surfaced for logging")
	}
	if !ok {
		t.Fatal("want fail-open (allowed) when Redis is unreachable")
	}
}

func TestDisabledLimiterAllowsAll(t *testing.T) {
	l := New("", 0, time.Minute) // no address = disabled
	for i := 0; i < 5; i++ {
		if ok, err := l.Allow(context.Background(), "ip"); !ok || err != nil {
			t.Fatalf("disabled limiter should always allow: ok=%v err=%v", ok, err)
		}
	}
}
