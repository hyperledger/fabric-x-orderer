/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon

import (
	"fmt"
	"sync"
	"time"
)

// RateLimiter controls the rate in which data can be sent and received.
// By such a rate limiter, txs are sent to routers at a specified constant rate that they can deal with in terms of memory and CPU.
type RateLimiter struct {
	tokens       float64        // Current number of tokens in the bucket
	capacity     int            // Maximum number of tokens in the bucket at a time
	fillInterval time.Duration  // The interval at which the bucket is filled in millisecond (e.g., every 10 milliseconds)
	fillQuota    float64        // Number of tokens to add per fillInterval interval
	mutex        sync.Mutex     // Mutex to synchronize access to the bucket
	cond         sync.Cond      // Condition variable to wait for tokens
	stopChan     chan bool      // Channel to stop the bucket
	stopped      bool           // Flag to indicate if the limiter is stopped
	wg           sync.WaitGroup // WaitGroup to wait for fillBucket goroutine
}

// NewRateLimiter creates a new bucket with a given capacity, fill interval (ms) and rate limit (tx/s).
func NewRateLimiter(rateLimit int, fillInterval time.Duration, capacity int) (*RateLimiter, error) {
	fillQuota := float64(rateLimit*int(fillInterval.Milliseconds())) / 1000.0

	if capacity <= 0 {
		return nil, fmt.Errorf("invalid capacity: (%d)", capacity)
	}

	if fillQuota <= 0 {
		return nil, fmt.Errorf("invalid fillQuota: (%.2f)", fillQuota)
	}

	if fillQuota > float64(capacity) {
		return nil, fmt.Errorf("fillQuota (%.2f) must be less than or equal to capacity (%d)", fillQuota, capacity)
	}

	rl := &RateLimiter{
		tokens:       float64(capacity),
		capacity:     capacity,
		fillInterval: fillInterval,
		fillQuota:    fillQuota,
		stopChan:     make(chan bool),
		stopped:      false,
	}
	rl.cond = sync.Cond{L: &rl.mutex}

	rl.wg.Add(1)
	go rl.fillBucket()
	return rl, nil
}

// fillBucket fills the bucket at the specified interval.
func (rl *RateLimiter) fillBucket() {
	defer rl.wg.Done()

	ticker := time.NewTicker(rl.fillInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rl.mutex.Lock()
			rl.tokens = rl.tokens + rl.fillQuota
			if rl.tokens > float64(rl.capacity) {
				rl.tokens = float64(rl.capacity)
			}
			rl.cond.Broadcast()
			rl.mutex.Unlock()
		case <-rl.stopChan:
			rl.mutex.Lock()
			rl.cond.Broadcast()
			rl.mutex.Unlock()
			return
		}
	}
}

// Stop stops the bucket.
func (rl *RateLimiter) Stop() {
	rl.mutex.Lock()

	if rl.stopped {
		rl.mutex.Unlock()
		return
	}

	rl.stopped = true
	close(rl.stopChan)
	rl.cond.Broadcast()
	rl.mutex.Unlock()

	rl.wg.Wait()
}

// GetToken attempts to obtain a token from the bucket.
// If a token was successfully obtained it returns true.
// If the bucket is empty it waits until a token becomes available.
// If the rate limiter was stopped it returns false.
func (rl *RateLimiter) GetToken() bool {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	for {
		if rl.stopped {
			return false
		}

		if rl.tokens >= 1.0 {
			rl.tokens--
			return true
		}

		rl.cond.Wait()
	}
}
