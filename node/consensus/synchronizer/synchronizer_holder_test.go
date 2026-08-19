/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer_test

import (
	"sync"
	"sync/atomic"
	"testing"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/synchronizer"
	"github.com/stretchr/testify/require"
)

// countingSynchronizer is a trivial SynchronizerWithStop that records how many times Sync and Stop
// were called. Its counters are atomic so they can be read after concurrent use without racing.
type countingSynchronizer struct {
	syncCalls atomic.Int64
	stopCalls atomic.Int64
}

func (c *countingSynchronizer) Sync() smartbft_types.SyncResponse {
	c.syncCalls.Add(1)
	return smartbft_types.SyncResponse{}
}

func (c *countingSynchronizer) Stop() {
	c.stopCalls.Add(1)
}

// TestHolderSwapDoesNotRaceSync spins up goroutines that call Sync() and Stop() on the holder while
// another goroutine swaps the inner synchronizer, reproducing the dynamic-reconfiguration race
// between SmartBFT's unlocked read of BFT.Synchronizer and the reconfiguration write. It must run
// clean under -race.
func TestHolderSwapDoesNotRaceSync(t *testing.T) {
	first := &countingSynchronizer{}
	holder := synchronizer.NewHolder(first)

	const iterations = 1000
	var wg sync.WaitGroup

	// Reader: continuously drives Sync() like SmartBFT's viewchanger/controller goroutines.
	wg.Go(func() {
		for range iterations {
			holder.Sync()
		}
	})

	// Reader: continuously drives Stop() like SoftStop/Stop.
	wg.Go(func() {
		for range iterations {
			holder.Stop()
		}
	})

	// Writer: continuously swaps the inner synchronizer like the reconfiguration goroutine.
	wg.Go(func() {
		for range iterations {
			holder.Swap(&countingSynchronizer{})
		}
	})

	wg.Wait()
}

func TestHolderSwapReplacesInner(t *testing.T) {
	first := &countingSynchronizer{}
	second := &countingSynchronizer{}
	holder := synchronizer.NewHolder(first)

	holder.Sync()
	holder.Stop()
	require.Equal(t, int64(1), first.syncCalls.Load())
	require.Equal(t, int64(1), first.stopCalls.Load())

	holder.Swap(second)

	holder.Sync()
	holder.Stop()
	// The first synchronizer is no longer touched after the swap.
	require.Equal(t, int64(1), first.syncCalls.Load())
	require.Equal(t, int64(1), first.stopCalls.Load())
	require.Equal(t, int64(1), second.syncCalls.Load())
	require.Equal(t, int64(1), second.stopCalls.Load())
}

var _ synchronizer.SynchronizerWithStop = (*countingSynchronizer)(nil)
