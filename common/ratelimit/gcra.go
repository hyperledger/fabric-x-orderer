/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package ratelimit provides a lock-free token-bucket rate limiter suitable for
// hot paths that must admit or reject requests at very high rates with minimal
// contention.
package ratelimit

import (
	"sync/atomic"
	"time"
)

// Limiter is a token-bucket rate limiter implemented with the Generic Cell Rate
// Algorithm (GCRA). Its entire state is a single atomic int64 (the "theoretical
// arrival time", TAT, in monotonic nanoseconds), so an admission decision is one
// atomic load and, when admitted, one compare-and-swap. There is no mutex, no
// background goroutine and no ticker: contention is bounded by GOMAXPROCS rather
// than by the number of concurrent callers.
//
// A nil *Limiter is a valid value meaning "unlimited": callers gate the hot path
// with a single `lim != nil` check, so a disabled limiter costs nothing.
//
// A Limiter is safe for concurrent use by multiple goroutines.
type Limiter struct {
	tatNanos         atomic.Int64 // theoretical arrival time, in the nowFn timeline
	intervalNanos    int64        // nanoseconds per token = 1e9 / rate
	burstOffsetNanos int64        // intervalNanos * burst (how far TAT may lag "now")
	nowFn            func() int64 // monotonic clock; time.Since(base) in production
}

// New returns a Limiter admitting an average of rate requests per second with a
// bucket capacity of burst tokens. It returns nil (meaning "unlimited", i.e.
// throttling disabled) when rate <= 0, or when rate is so large that the
// per-token interval rounds down to zero nanoseconds. A burst < 1 is coerced to 1.
func New(rate float64, burst int) *Limiter {
	base := time.Now()
	return newWithClock(rate, burst, func() int64 { return int64(time.Since(base)) })
}

// newWithClock is New with an injectable monotonic clock, used by tests to make
// time deterministic. nowFn must return nanoseconds from a fixed, monotonic base.
func newWithClock(rate float64, burst int, nowFn func() int64) *Limiter {
	if rate <= 0 {
		return nil
	}
	interval := int64(float64(time.Second) / rate)
	if interval <= 0 {
		// Rate is effectively unlimited at nanosecond resolution.
		return nil
	}
	if burst < 1 {
		burst = 1
	}
	return &Limiter{
		intervalNanos:    interval,
		burstOffsetNanos: interval * int64(burst),
		nowFn:            nowFn,
	}
}

// Allow reports whether one request may proceed now. It never blocks: an
// over-budget request is rejected immediately (returning false). Rejections do
// not mutate state (no CAS), so they are cheaper than admissions.
func (l *Limiter) Allow() bool {
	now := l.nowFn()
	for {
		tat := l.tatNanos.Load()
		newTat := max(tat, now) + l.intervalNanos
		if now < newTat-l.burstOffsetNanos {
			return false // over budget
		}
		if l.tatNanos.CompareAndSwap(tat, newTat) {
			return true
		}
		// Lost the race with a concurrent admission; retry. Retries are bounded
		// by the number of goroutines actually running (GOMAXPROCS).
	}
}
