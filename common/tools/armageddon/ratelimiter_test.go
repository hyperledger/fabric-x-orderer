/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSingleTokenConsumption(t *testing.T) {
	capacity := 50
	fillInterval := 500 * time.Millisecond
	rateLimit := 100

	rl, err := NewRateLimiter(rateLimit, fillInterval, capacity)
	require.NoError(t, err)
	require.NotNil(t, rl)

	status := rl.GetToken()
	require.True(t, status)

	rl.Stop()
}

func TestStopBehavior(t *testing.T) {
	capacity := 50
	fillInterval := 500 * time.Millisecond
	rateLimit := 100

	rl, err := NewRateLimiter(rateLimit, fillInterval, capacity)
	require.NoError(t, err)
	require.NotNil(t, rl)

	require.True(t, rl.GetToken())
	rl.Stop()
	require.False(t, rl.GetToken())
}

func TestCapacityTokenConsumptionAndWaitForToken(t *testing.T) {
	capacity := 50
	fillInterval := 200 * time.Millisecond
	rateLimit := 2

	rl, err := NewRateLimiter(rateLimit, fillInterval, capacity)
	require.NoError(t, err)
	require.NotNil(t, rl)

	for i := 0; i < capacity; i++ {
		status := rl.GetToken()
		require.True(t, status)
	}

	// Eventually we should be able to get a token as the bucket is filled
	require.Eventually(t, func() bool {
		return rl.GetToken()
	}, 1000*time.Millisecond, 200*time.Millisecond)

	rl.Stop()
}

func TestConcurrentAccess(t *testing.T) {
	capacity := 20
	fillInterval := 200 * time.Millisecond
	rateLimit := 4

	rl, err := NewRateLimiter(rateLimit, fillInterval, capacity)
	require.NoError(t, err)

	var wg sync.WaitGroup
	numClients := 10
	tokensPerClient := 5

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < tokensPerClient; j++ {
				status := rl.GetToken()
				require.True(t, status)
			}
		}()
	}
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		rl.Stop()
		require.False(t, rl.GetToken())
	}()
	wg.Wait()
}

func TestTPSMeasureWithRateLimiter(t *testing.T) {
	transactions := 400
	rateLimit := 200
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	capacity := rateLimit / fillFrequency

	rl, err := NewRateLimiter(rateLimit, fillInterval, capacity)
	require.NoError(t, err)

	start := time.Now()

	for i := 0; i < transactions; i++ {
		status := rl.GetToken()
		require.True(t, status)
	}
	rl.Stop()

	elapsed := time.Since(start)
	TPS := float64(transactions) / float64(elapsed.Seconds())

	minLimit := float64(rateLimit) * 0.7
	maxLimit := float64(rateLimit) * 1.3

	require.Less(t, minLimit, TPS)
	require.Less(t, TPS, maxLimit)
}

// TestProtectedMapHelpers verifies the Size, Keys, and Contains methods added
// to protectedMap for the improved submit TX-verification path.
// This is a pure unit test — no network, no goroutines.
func TestProtectedMapHelpers(t *testing.T) {
	pm := &protectedMap{keyValMap: make(map[string]bool), mutex: sync.Mutex{}}

	// Empty map
	require.Equal(t, 0, pm.Size())
	require.Empty(t, pm.Keys())
	require.False(t, pm.Contains("0"))

	// Add three entries
	pm.Add("0")
	pm.Add("1")
	pm.Add("2")
	require.Equal(t, 3, pm.Size())
	require.True(t, pm.Contains("1"))
	require.False(t, pm.Contains("99"))

	keys := pm.Keys()
	require.Len(t, keys, 3)
	require.ElementsMatch(t, []string{"0", "1", "2"}, keys)

	// Remove one entry
	pm.Remove("1")
	require.Equal(t, 2, pm.Size())
	require.False(t, pm.Contains("1"))
	require.ElementsMatch(t, []string{"0", "2"}, pm.Keys())

	// Remove remaining entries — map should be empty
	pm.Remove("0")
	pm.Remove("2")
	require.Equal(t, 0, pm.Size())
	require.True(t, pm.IsEmpty())
}
