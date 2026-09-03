/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testClock is a deterministic, concurrency-safe monotonic clock for tests.
type testClock struct {
	nanos atomic.Int64
}

func (c *testClock) now() int64              { return c.nanos.Load() }
func (c *testClock) set(d time.Duration)     { c.nanos.Store(int64(d)) }
func (c *testClock) advance(d time.Duration) { c.nanos.Add(int64(d)) }

func TestNew_DisabledReturnsNil(t *testing.T) {
	require.Nil(t, New(0, 100), "rate 0 disables throttling")
	require.Nil(t, New(-1, 100), "negative rate disables throttling")
	// A rate so large that 1e9/rate rounds down to 0 ns/token is treated as unlimited.
	require.Nil(t, New(2e18, 1), "rate beyond nanosecond resolution disables throttling")
}

func TestAllow_BurstThenReject(t *testing.T) {
	clk := &testClock{}
	clk.set(time.Second) // start away from zero to exercise the cold-start catch-up
	const rate, burst = 1000, 5
	l := newWithClock(rate, burst, clk.now)

	// At a single instant, exactly `burst` requests are admitted.
	admitted := 0
	for i := 0; i < burst*3; i++ {
		if l.Allow() {
			admitted++
		}
	}
	require.Equal(t, burst, admitted, "should admit exactly the burst at a fixed instant")
	require.False(t, l.Allow(), "further requests at the same instant are rejected")
}

func TestAllow_RefillOverTime(t *testing.T) {
	clk := &testClock{}
	clk.set(time.Second)
	const rate, burst = 1000, 5
	interval := time.Second / rate
	l := newWithClock(rate, burst, clk.now)

	// Drain the burst.
	for i := 0; i < burst; i++ {
		require.True(t, l.Allow())
	}
	require.False(t, l.Allow())

	// One interval later, exactly one more token is available.
	clk.advance(interval)
	require.True(t, l.Allow())
	require.False(t, l.Allow())
}

func TestAllow_RateConvergence(t *testing.T) {
	clk := &testClock{}
	clk.set(time.Second)
	const rate, burst = 500, 1
	interval := time.Second / rate
	l := newWithClock(rate, burst, clk.now)

	// With burst 1, advancing exactly one interval admits exactly one request.
	admitted := 0
	const steps = 1000
	for i := 0; i < steps; i++ {
		if l.Allow() {
			admitted++
		}
		clk.advance(interval)
	}
	// steps admissions expected (one per interval); allow +/-1 for boundary effects.
	require.InDelta(t, steps, admitted, 1)
}

// TestAllow_ConcurrentNoOverAdmit runs many goroutines against a frozen clock and
// asserts that no more than the burst is ever admitted, proving Allow() is atomic.
// Run with -race.
func TestAllow_ConcurrentNoOverAdmit(t *testing.T) {
	clk := &testClock{}
	clk.set(time.Second)
	const rate, burst = 1000, 50
	l := newWithClock(rate, burst, clk.now)

	const goroutines, perG = 32, 1000
	var admitted atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if l.Allow() {
					admitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(burst), admitted.Load(), "frozen clock must admit exactly the burst, no over-admission under concurrency")
}

// BenchmarkAllow_Parallel measures Allow() on the real (time.Now-backed) clock at
// a rate high enough that virtually every call is admitted. Run with
// -cpu=1,4,8 to observe scaling and confirm the single-atomic hot line does not
// collapse throughput at the 500k/s target. This is the decision hook for keeping
// the direct clock read vs. switching to a coarse cached clock.
func BenchmarkAllow_Parallel(b *testing.B) {
	// rate 1e9 => interval 1ns (the max meaningful rate before New treats it as
	// unlimited); a huge burst guarantees the admit (CAS) path is never exhausted.
	l := New(1e9, 1<<40)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow()
		}
	})
}

// BenchmarkAllow_Contended measures the worst case: an over-budget limiter where
// almost every call is rejected (rejections don't CAS, so this isolates the load
// + clock cost under maximal concurrency).
func BenchmarkAllow_Contended(b *testing.B) {
	l := New(1, 1) // 1/s: essentially everything is rejected
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow()
		}
	})
}

// BenchmarkClockRead isolates the cost of the production clock read so its share
// of Allow() can be quantified.
func BenchmarkClockRead(b *testing.B) {
	base := time.Now()
	var sink int64
	for i := 0; i < b.N; i++ {
		sink = int64(time.Since(base))
	}
	_ = sink
}
