/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package ratelimit provides a lock-free token-bucket rate limiter suitable for
// hot paths that must admit or reject requests at very high rates with minimal
// contention. It implements the Generic Cell Rate Algorithm (GCRA).
package ratelimit

import (
	"math"
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
// Classical GCRA keeps a theoretical arrival time (TAT) and, for an emission
// interval T = 1/rate and a tolerance tau, admits a request arriving at "now"
// when now >= TAT - tau, then advances TAT to max(TAT, now) + T. tau is a
// duration — how far ahead of the theoretical schedule a request may run — so
// the number of requests admissible back to back is tau/T + 1.
//
// This implementation is parameterised instead by an integer burst — the bucket
// capacity, i.e. the number of requests admissible at a single instant — with
// tau = (burst-1)*T. It also folds the emission increment into the test: it
// computes newTAT = max(TAT, now) + T and rejects when now < newTAT - burstOffset,
// where burstOffset = burst*T = tau + T. The extra T inside newTAT and inside
// burstOffset cancel, so this is exactly the classical "now < TAT - tau" in the
// binding (TAT > now) case; the single max(...)+T form just lets the idle and
// bunched branches — and the CAS — share one value. So burst 1 => tau 0 => the
// steady rate with no bunching, burst N => N admitted at once, and burst < 1
// (which would be a negative tau, rejecting everything) is coerced to 1 in New.
//
// A nil *Limiter is a valid value meaning "unlimited": callers gate the hot path
// with a single `lim != nil` check, so a disabled limiter costs nothing.
//
// A Limiter is safe for concurrent use by multiple goroutines.
type Limiter struct {
	tatNanos         atomic.Int64 // theoretical arrival time, in the nowFn timeline
	intervalNanos    int64        // nanoseconds per token = 1e9 / rate
	burstOffsetNanos int64        // burst*interval; classical tolerance tau = (burst-1)*interval
	nowFn            func() int64 // monotonic clock; time.Since(base) in production
}

// New returns a Limiter admitting an average of rate requests per second with a
// bucket capacity of burst tokens. It returns nil (meaning "unlimited", i.e.
// throttling disabled) when rate <= 0, or when rate is so large that the
// per-token interval rounds down to zero nanoseconds. A rate below the slowest
// supported value (~one request per day) is clamped up to that minimum, so a
// tiny rate can't overflow the interval conversion and be mistaken for
// "unlimited".
//
// A burst < 1 is coerced to 1. This is only a defensive floor — it prevents a
// zero-capacity, reject-everything limiter (burst 0 => negative tolerance); it
// is not the intended "unset burst" default. That default belongs to the
// caller: the router defaults an omitted Burst to Rate in its local config (see
// config.applyNodeDefaults and the Router.Throttling.Burst docs), because a
// burst of 1 has zero bunching tolerance and would throttle bursty ingress well
// below the configured rate. New should therefore normally receive a burst >= 1.
func New(rate float64, burst int) *Limiter {
	base := time.Now()
	return newWithClock(rate, burst, func() int64 { return int64(time.Since(base)) })
}

// maxIntervalNanos caps the per-token interval so the slowest rate New supports
// is roughly one request per day. A smaller rate is clamped up to this, keeping
// the interval math well clear of int64 overflow.
const maxIntervalNanos = int64(24 * time.Hour)

// newWithClock is New with an injectable monotonic clock, used by tests to make
// time deterministic. nowFn must return nanoseconds from a fixed, monotonic base.
func newWithClock(rate float64, burst int, nowFn func() int64) *Limiter {
	if rate <= 0 {
		return nil
	}
	intervalFloat := float64(time.Second) / rate
	if intervalFloat > float64(maxIntervalNanos) {
		// Rate is below the slowest supported value (~once per day); clamp to that
		// minimum so a tiny rate can't overflow the int64 conversion and be
		// mistaken for "unlimited".
		intervalFloat = float64(maxIntervalNanos)
	}
	interval := int64(intervalFloat)
	if interval <= 0 {
		// Rate is effectively unlimited at nanosecond resolution.
		return nil
	}
	if burst < 1 {
		burst = 1
	}
	burstOffset := interval * int64(burst)
	if burstOffset/int64(burst) != interval {
		// Multiplication overflowed int64; treat as an effectively unbounded burst.
		burstOffset = math.MaxInt64
	}
	return &Limiter{
		intervalNanos:    interval,
		burstOffsetNanos: burstOffset,
		nowFn:            nowFn,
	}
}

// Allow reports whether one request may proceed now. It never blocks: an
// over-budget request is rejected immediately (returning false). Rejections do
// not mutate state (no CAS), so they are cheaper than admissions.
func (l *Limiter) Allow() bool {
	if l == nil {
		return true // a nil Limiter is "unlimited"
	}
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
