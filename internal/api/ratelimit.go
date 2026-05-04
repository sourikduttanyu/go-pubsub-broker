package api

import (
	"sync"
	"time"
)

// tokenBucket implements a standard token-bucket rate limiter.
// Tokens refill at refillRate per second up to max.
type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	max        float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

func newTokenBucket(rps float64) *tokenBucket {
	return &tokenBucket{
		tokens:     rps,
		max:        rps,
		refillRate: rps,
		lastRefill: time.Now(),
	}
}

func (tb *tokenBucket) allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.max {
		tb.tokens = tb.max
	}
	tb.lastRefill = now
	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

// rateLimiter manages one token bucket per API key.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	rps     float64
}

func newRateLimiter(rps float64) *rateLimiter {
	return &rateLimiter{
		buckets: make(map[string]*tokenBucket),
		rps:     rps,
	}
}

func (rl *rateLimiter) allow(key string) bool {
	rl.mu.Lock()
	b, ok := rl.buckets[key]
	if !ok {
		b = newTokenBucket(rl.rps)
		rl.buckets[key] = b
	}
	rl.mu.Unlock()
	return b.allow()
}
