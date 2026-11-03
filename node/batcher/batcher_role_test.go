/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/operations"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/batcher/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

func TestPrimaryBatcherSimple(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	reqIDs := []string{"reused-req-id-1"}
	pool.NextRequestsReturnsOnCall(1, reqs, reqIDs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	batcher.Start()

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.RemoveRequestsCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()
	batcher.Stop()

	require.True(t, pool.RestartArgsForCall(0))
	require.NotZero(t, pool.NextRequestsCallCount())
	require.Equal(t, reqIDs, pool.RemoveRequestsArgsForCall(0))
}

func TestSecondaryBatcherSimple(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 1)
	reqs = append(reqs, req)

	batch := arma_types.NewSimpleBatch(0, 1, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	batcher.Start()

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batch = arma_types.NewSimpleBatch(0, 1, 1, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.RemoveRequestsCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.Zero(t, pool.NextRequestsCallCount())
}

// Scenario: a secondary batcher, running at config sequence K, pulls a batch that the primary stamped with the
// previous config sequence K-1. This models the timing skew where pulling batches from a
// primary and pulling configs from consensus are asynchronous. The secondary logs a warning about the config
// sequence mismatch but still verifies and appends the batch, and the BAF it emits
// carries the secondary's current config sequence K.
func TestSecondaryBatcherPullsLowerConfigSeqBatch(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := arma_types.PartyID(2)
	shardID := arma_types.ShardID(0)

	// the batcher's current config sequence (K) and the older config sequence the pulled batch is stamped with (K-1)
	const currentConfigSeq = arma_types.ConfigSequence(1)
	const batchConfigSeq = arma_types.ConfigSequence(0)

	logger := testutil.CreateLogger(t, int(batcherID))
	batcher := createBatcher(t, batcherID, shardID, batchers, N, logger)

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(currentConfigSeq)
	batcher.ConfigSequenceGetter = configSeqGet

	// the BAF creator stamps the batcher's current config sequence, mirroring the production;
	// this is what lets a fragment for a lower-seq batch still be counted at the current config sequence.
	bafCreator := &mocks.FakeBAFCreator{}
	bafCreator.CreateBAFCalls(func(seq arma_types.BatchSequence, primary arma_types.PartyID, si arma_types.ShardID, digest []byte, txCount uint64, primarySignature []byte) arma_types.BatchAttestationFragment {
		return arma_types.NewSimpleBatchAttestationFragment(si, primary, seq, digest, batcherID, configSeqGet.ConfigSequence(), txCount, primarySignature)
	})
	batcher.BAFCreator = bafCreator

	bafSender := &mocks.FakeBAFSender{}
	batcher.BAFSender = bafSender

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	// the pulled batch: primary 1, seq 0 (matching the secondary's initial ledger height), stamped with the older
	// config sequence K-1
	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := arma_types.BatchedRequests{req}
	batch := arma_types.NewSimpleBatch(shardID, 1, 0, reqs, batchConfigSeq, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()
	defer batcher.Stop()

	batchChan <- batch

	// verifyBatch logs a warning about the config-sequence mismatch but does not reject the batch; a successful
	// append is proof that the batch was verified and accepted despite the skew
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	// the batch is recorded under the secondary's current config sequence K, not the batch's K-1
	_, _, appendedConfigSeq, _, _, _ := ledger.AppendArgsForCall(0)
	require.Equal(t, currentConfigSeq, appendedConfigSeq)

	// the BAF the secondary emits carries the current config sequence K
	require.Eventually(t, func() bool {
		return bafSender.SendBAFCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	sentBAF, _ := bafSender.SendBAFArgsForCall(0)
	require.Equal(t, currentConfigSeq, sentBAF.ConfigSequence())
}

func TestPrimaryChangeToSecondary(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batch := arma_types.NewSimpleBatch(0, 2, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.NotZero(t, pool.NextRequestsCallCount())

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	})

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.False(t, pool.RestartArgsForCall(1))

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.True(t, pool.RestartArgsForCall(0))
	require.False(t, pool.RestartArgsForCall(1))
}

func TestSecondaryChangeToPrimary(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batch := arma_types.NewSimpleBatch(0, 1, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Zero(t, pool.NextRequestsCallCount())

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	})

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.True(t, pool.RestartArgsForCall(1))

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return batchPuller.StopCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.NotZero(t, pool.NextRequestsCallCount())

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.True(t, pool.RestartArgsForCall(1))

	require.Equal(t, arma_types.PartyID(1), ledger.HeightArgsForCall(0))
	require.Equal(t, arma_types.PartyID(2), ledger.HeightArgsForCall(1))
}

func TestSecondaryChangeToSecondary(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 3
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 1)
	reqs = append(reqs, req)

	batch := arma_types.NewSimpleBatch(0, 1, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return pool.RemoveRequestsCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	})

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.False(t, pool.RestartArgsForCall(1))

	require.Eventually(t, func() bool {
		return batchPuller.StopCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batch = arma_types.NewSimpleBatch(0, 2, 0, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.RemoveRequestsCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.False(t, pool.RestartArgsForCall(1))
	require.Zero(t, pool.NextRequestsCallCount())

	require.Eventually(t, func() bool {
		return batchPuller.StopCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)
}

func TestPrimaryChangeToPrimary(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.NotZero(t, pool.NextRequestsCallCount())

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  4,
			},
		},
	})

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.True(t, pool.RestartArgsForCall(1))

	batcher.Stop()

	require.True(t, pool.RestartArgsForCall(0))
	require.True(t, pool.RestartArgsForCall(1))
}

func TestPrimaryWaiting(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturns(reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	batcher.Start()

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.NextRequestsCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.Equal(t, 10, ledger.AppendCallCount())
	require.Equal(t, 10, pool.NextRequestsCallCount())
}

func TestPrimaryWaitingAndTermChange(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturns(reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batch := arma_types.NewSimpleBatch(0, 2, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.NextRequestsCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	})

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.False(t, pool.RestartArgsForCall(1))

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 11
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.Equal(t, 11, ledger.AppendCallCount())
	require.Equal(t, 10, pool.NextRequestsCallCount())
}

func TestResubmitPending(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs, nil)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	batcher.ConfigSequenceGetter = configSeqGet

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	batcher.TermProvider = termProvider

	batch := arma_types.NewSimpleBatch(0, 1, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	})

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Zero(t, pool.NextRequestsCallCount())

	require.Zero(t, pool.SubmitCallCount())

	ledger.RetrieveBatchByNumberReturns(batch)

	myBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), arma_types.PartyID(batcherID), 0, 0, nil)
	notMyBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), arma_types.PartyID(batcherID+1), 0, 0, nil)
	myBAFWithOtherPrimary := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary()+1, batch.Seq(), batch.Digest(), arma_types.PartyID(batcherID), 0, 0, nil)

	termProvider.setState(batcher, &state.State{
		Shards: []state.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
		Pending: []arma_types.BatchAttestationFragment{myBAF, notMyBAF, myBAFWithOtherPrimary},
	})

	require.Eventually(t, func() bool {
		return pool.SubmitCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.True(t, pool.RestartArgsForCall(1))

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return batchPuller.StopCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.NotZero(t, pool.NextRequestsCallCount())

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.True(t, pool.RestartArgsForCall(1))

	require.Equal(t, arma_types.PartyID(1), ledger.HeightArgsForCall(0))
	require.Equal(t, arma_types.PartyID(2), ledger.HeightArgsForCall(1))
}

func TestVerifyBatch(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0
	logger := testutil.CreateLogger(t, batcherID)
	secondaryBatcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)
	verifier := &mocks.FakeBatchedRequestsVerifier{}
	reqIDs := []string{"reused-req-id-1"}
	verifier.VerifyBatchedRequestsReturns(reqIDs, nil)
	secondaryBatcher.BatchedRequestsVerifier = verifier
	complainer := &mocks.FakeComplainer{}
	secondaryBatcher.Complainer = complainer

	pool := &mocks.FakeMemPool{}
	secondaryBatcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 1)
	reqs = append(reqs, req)

	batch := arma_types.NewSimpleBatch(0, 1, 0, reqs, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	secondaryBatcher.BatchPuller = batchPuller

	ledger := &mocks.FakeBatchLedger{}
	secondaryBatcher.Ledger = ledger

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	secondaryBatcher.ConfigSequenceGetter = configSeqGet

	sigVerifier := &mocks.FakeSigVerifier{}
	sigVerifier.VerifySignatureReturns(nil)
	secondaryBatcher.SigVerifier = sigVerifier

	secondaryBatcher.Start()
	defer secondaryBatcher.Stop()

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.RemoveRequestsCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, reqIDs, pool.RemoveRequestsArgsForCall(0))

	batch = arma_types.NewSimpleBatch(0, 2, 0, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batch = arma_types.NewSimpleBatch(1, 1, 0, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	batch = arma_types.NewSimpleBatch(0, 1, 2, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 3
	}, 10*time.Second, 10*time.Millisecond)

	batch = arma_types.NewSimpleBatch(0, 1, 0, nil, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 4
	}, 10*time.Second, 10*time.Millisecond)

	verifier.VerifyBatchedRequestsReturns(nil, errors.New(""))
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 5
	}, 10*time.Second, 10*time.Millisecond)
	verifier.VerifyBatchedRequestsReturns(nil, nil)

	batch = arma_types.NewSimpleBatch(0, 1, 1, reqs, 1, nil) // config seq mismatch, log as warning but append anyway
	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	sigVerifier.VerifySignatureReturns(errors.New(""))
	batch = arma_types.NewSimpleBatch(0, 1, 2, reqs, 0, nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 6
	}, 10*time.Second, 10*time.Millisecond)
	sigVerifier.VerifySignatureReturns(nil)

	require.Equal(t, 2, ledger.AppendCallCount())
}

// TestResubmitPendingBAFsReverifiesOnConfigChange covers the reconfiguration case:
// requests taken from a BAF created under an older config are re-verified under the
// current policy before being resubmitted to the pool, and any request that fails
// the current policy is dropped rather than reinserted (so it never becomes eligible
// for the secondary pull-path verification shortcut).
// TestComplainInTermFaultSeenIn checks that a secondary complains in the term it saw the faulty batch
// in, even if the term changes before the complaint is sent.
func TestComplainInTermFaultSeenIn(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 3
	shardID := 0
	logger := testutil.CreateLogger(t, batcherID)
	secondaryBatcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	secondaryBatcher.TermProvider = termProvider

	// the verification of the pulled batch blocks until the term changed, and then fails
	verifying := make(chan struct{})
	termChanged := make(chan struct{})
	verifier := &mocks.FakeBatchedRequestsVerifier{}
	verifier.VerifyBatchedRequestsCalls(func(arma_types.BatchedRequests) ([]string, error) {
		close(verifying)
		<-termChanged
		return nil, errors.New("invalid request")
	})
	secondaryBatcher.BatchedRequestsVerifier = verifier
	complainer := &mocks.FakeComplainer{}
	secondaryBatcher.Complainer = complainer

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	batch := arma_types.NewSimpleBatch(0, 1, 0, arma_types.BatchedRequests{req}, 0, nil)

	batchPuller := &mocks.FakeBatchesPuller{}
	batchChan := make(chan arma_types.Batch, 1)
	batchPuller.PullBatchesReturns(batchChan)
	secondaryBatcher.BatchPuller = batchPuller

	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(0)
	secondaryBatcher.ConfigSequenceGetter = configSeqGet

	secondaryBatcher.Start()
	defer secondaryBatcher.Stop()

	// pull a batch from the primary of term 0
	batchChan <- batch
	<-verifying

	// the term changes while the batch is verified
	termProvider.setState(secondaryBatcher, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 1}}})
	close(termChanged)

	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	term, _ := complainer.ComplainArgsForCall(0)
	require.Equal(t, uint64(0), term)
}

// TestRoleTermIsTheTermItActsIn checks that the role's term, which the second-strike complaint is sent in,
// is the term the role acts in: it does not move with the batcher's term until the role restarts in it.
func TestRoleTermIsTheTermItActsIn(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 3
	shardID := 0
	logger := testutil.CreateLogger(t, batcherID)
	secondaryBatcher := createBatcher(t, arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	termProvider := newTestTermProvider(batchers, arma_types.ShardID(shardID), N)
	secondaryBatcher.TermProvider = termProvider

	batchPuller := &mocks.FakeBatchesPuller{}
	batchPuller.PullBatchesReturns(make(chan arma_types.Batch))
	secondaryBatcher.BatchPuller = batchPuller

	pool := &mocks.FakeMemPool{}
	secondaryBatcher.MemPool = pool

	secondaryBatcher.Start()
	defer secondaryBatcher.Stop()

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(0), secondaryBatcher.Term())

	// the batcher's term moves on, but the role is not told yet, so it still acts in term 0
	termProvider.setTerm(1)
	require.Never(t, func() bool {
		return secondaryBatcher.Term() != 0
	}, 500*time.Millisecond, 10*time.Millisecond)

	// once the role is told, it restarts in term 1
	termProvider.setState(secondaryBatcher, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 2}}})
	require.Eventually(t, func() bool {
		return secondaryBatcher.Term() == 2 && pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)
}

func TestResubmitPendingBAFsReverifiesOnConfigChange(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := arma_types.PartyID(2)
	shardID := arma_types.ShardID(0)
	logger := testutil.CreateLogger(t, int(batcherID))

	b := createBatcher(t, batcherID, shardID, batchers, N, logger)

	validReq := []byte("valid-request")
	invalidReq := []byte("invalid-request")
	reqs := arma_types.BatchedRequests{validReq, invalidReq}

	// The pending BAF and its ledger batch were created under config seq 0.
	batch := arma_types.NewSimpleBatch(shardID, 1, 0, reqs, 0, nil)
	ledger := &mocks.FakeBatchLedger{}
	ledger.RetrieveBatchByNumberReturns(batch)
	b.Ledger = ledger

	// The batcher's current config seq is newer than the BAF's, so its requests must
	// be re-verified under the current policy.
	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(1)
	b.ConfigSequenceGetter = configSeqGet

	// Under the current policy invalidReq no longer verifies.
	verifier := &mocks.FakeBatchedRequestsVerifier{}
	verifier.VerifyRequestStub = func(req []byte) error {
		if string(req) == string(invalidReq) {
			return errors.New("fails current policy")
		}
		return nil
	}
	b.BatchedRequestsVerifier = verifier

	pool := &mocks.FakeMemPool{}
	b.MemPool = pool

	myBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), batcherID, 0, 0, nil)
	st := &state.State{
		Shards:  []state.ShardTerm{{Shard: shardID, Term: 1}},
		Pending: []arma_types.BatchAttestationFragment{myBAF},
	}

	b.ResubmitPendingBAFs(st, 0, true)

	// Both requests were re-verified, but only the valid one was resubmitted to the pool.
	require.Equal(t, 2, verifier.VerifyRequestCallCount())
	require.Equal(t, 1, pool.SubmitCallCount())
	require.Equal(t, validReq, pool.SubmitArgsForCall(0))
}

// TestResubmitStaleConfigBAFs covers the stale-config revive path: a BAF that consensus surfaced as
// behind the current config (in State.StaleConfigBAFs) has its batch's requests re-read from the ledger,
// re-verified under the current config, and resubmitted to the pool. BAFs signed by other batchers
// are ignored.
func TestResubmitStaleConfigBAFs(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := arma_types.PartyID(2)
	shardID := arma_types.ShardID(0)
	logger := testutil.CreateLogger(t, int(batcherID))

	b := createBatcher(t, batcherID, shardID, batchers, N, logger)

	validReq := []byte("valid-request")
	invalidReq := []byte("invalid-request")
	reqs := arma_types.BatchedRequests{validReq, invalidReq}

	// The stale BAF and its ledger batch were created under config seq 0.
	batch := arma_types.NewSimpleBatch(shardID, 1, 0, reqs, 0, nil)
	ledger := &mocks.FakeBatchLedger{}
	ledger.RetrieveBatchByNumberReturns(batch)
	b.Ledger = ledger

	// Current config seq (1) is newer than the BAF's (0), so its requests are re-verified.
	configSeqGet := &mocks.FakeConfigSequenceGetter{}
	configSeqGet.ConfigSequenceReturns(1)
	b.ConfigSequenceGetter = configSeqGet

	verifier := &mocks.FakeBatchedRequestsVerifier{}
	verifier.VerifyRequestStub = func(req []byte) error {
		if string(req) == string(invalidReq) {
			return errors.New("fails current policy")
		}
		return nil
	}
	b.BatchedRequestsVerifier = verifier

	pool := &mocks.FakeMemPool{}
	b.MemPool = pool

	myBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), batcherID, 0, 0, nil)
	notMyBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), batcherID+1, 0, 0, nil)
	st := &state.State{
		Shards:          []state.ShardTerm{{Shard: shardID, Term: 1}},
		StaleConfigBAFs: []arma_types.BatchAttestationFragment{myBAF, notMyBAF},
	}

	b.ResubmitStaleConfigBAFs(st)

	// Only my BAF's requests are revived: both re-verified, only the valid one resubmitted; the other
	// batcher's BAF is ignored (no extra ledger read / verify).
	require.Equal(t, 2, verifier.VerifyRequestCallCount())
	require.Equal(t, 1, pool.SubmitCallCount())
	require.Equal(t, validReq, pool.SubmitArgsForCall(0))
}

func createBatcher(t *testing.T, batcherID arma_types.PartyID, shardID arma_types.ShardID, batchers []arma_types.PartyID, N uint16, logger *flogging.FabricLogger) *batcher.BatcherRole {
	bafCreator := &mocks.FakeBAFCreator{}
	bafCreator.CreateBAFCalls(func(seq arma_types.BatchSequence, primary arma_types.PartyID, si arma_types.ShardID, digest []byte, txCount uint64, primarySignature []byte) arma_types.BatchAttestationFragment {
		return arma_types.NewSimpleBatchAttestationFragment(shardID, primary, seq, digest, batcherID, 0, txCount, nil)
	})

	batchersInfo := make([]config.BatcherInfo, len(batchers))
	for i, id := range batchers {
		batchersInfo[i] = config.BatcherInfo{
			PartyID: id,
		}
	}

	ledger := &mocks.FakeBatchLedger{}

	batcher := &batcher.BatcherRole{
		BatchTimeout:            time.Millisecond * 500,
		RequestInspector:        &mocks.FakeRequestInspector{},
		ID:                      batcherID,
		Shard:                   shardID,
		Threshold:               2,
		N:                       N,
		Logger:                  logger,
		Ledger:                  ledger,
		BatchPuller:             &mocks.FakeBatchesPuller{},
		TermProvider:            newTestTermProvider(batchers, shardID, N),
		BAFCreator:              bafCreator,
		BAFSender:               &mocks.FakeBAFSender{},
		BatchAcker:              &mocks.FakeBatchAcker{},
		MemPool:                 &mocks.FakeMemPool{},
		BatchedRequestsVerifier: &mocks.FakeBatchedRequestsVerifier{},
		SigVerifier:             &mocks.FakeSigVerifier{},
		BatchSequenceGap:        arma_types.BatchSequence(10),
		Metrics: batcher.NewBatcherMetrics(&config.BatcherNodeConfig{
			PartyId: batcherID,
			ShardId: shardID,
			Operations: &operations.Operations{
				ListenAddress: testutil.AllocateLocalhostAddress(t),
			},
			Metrics: &operations.Metrics{
				Provider:           "disabled",
				MetricsLogInterval: 10 * time.Second,
			},
		}, batchersInfo, &node_ledger.BatchLedgerMetrics{}, ledger, logger),
	}

	return batcher
}

// testTermProvider stands in for the batcher as the source of the shard's primary and term.
type testTermProvider struct {
	lock     sync.Mutex
	batchers []arma_types.PartyID
	shard    arma_types.ShardID
	n        uint16
	term     uint64
}

func newTestTermProvider(batchers []arma_types.PartyID, shard arma_types.ShardID, n uint16) *testTermProvider {
	return &testTermProvider{batchers: batchers, shard: shard, n: n}
}

func (tp *testTermProvider) CurrentPrimaryAndTerm() (arma_types.PartyID, uint64) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	return tp.primary(tp.term), tp.term
}

// setState moves the provider to the term of its shard in s and, if the term changed, notifies role, as
// the batcher's decision loop does.
func (tp *testTermProvider) setState(role *batcher.BatcherRole, s *state.State) {
	var term uint64
	for _, shard := range s.Shards {
		if shard.Shard == tp.shard {
			term = shard.Term
		}
	}
	if prevPrimary, changed := tp.setTerm(term); changed {
		role.NotifyTermChange(s, prevPrimary)
	}
}

// setTerm moves the provider to term without notifying the role, and returns the primary of the
// previous term and whether the term changed.
func (tp *testTermProvider) setTerm(term uint64) (arma_types.PartyID, bool) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	prevPrimary, prevTerm := tp.primary(tp.term), tp.term
	tp.term = term
	return prevPrimary, term != prevTerm
}

func (tp *testTermProvider) primary(term uint64) arma_types.PartyID {
	return tp.batchers[(uint64(tp.shard)+term)%uint64(tp.n)]
}

// TestPrimaryBAFContextPerRole checks that the primary reuses one BAF context and cancels it on a term change.
func TestPrimaryBAFContextPerRole(t *testing.T) {
	b := createBatcher(t, 1, 0, []arma_types.PartyID{1, 2, 3, 4}, 4, testutil.CreateLogger(t, 1))
	pool := &mocks.FakeMemPool{}
	pool.NextRequestsReturns(arma_types.BatchedRequests{[]byte{1}}, nil)
	b.MemPool = pool
	bafSender := &mocks.FakeBAFSender{}
	// The last BAF blocks until its context is cancelled.
	inFlightDone := make(chan struct{})
	bafSender.SendBAFCalls(func(baf arma_types.BatchAttestationFragment, ctx context.Context) {
		if baf.Seq() == 9 {
			<-ctx.Done()
			close(inFlightDone)
		}
	})
	b.BAFSender = bafSender
	b.ConfigSequenceGetter = &mocks.FakeConfigSequenceGetter{}
	termProvider := newTestTermProvider([]arma_types.PartyID{1, 2, 3, 4}, 0, 4)
	b.TermProvider = termProvider

	b.Start()
	defer b.Stop()

	// With no acks, the primary sends BatchSequenceGap (10) batches and then waits.
	require.Eventually(t, func() bool {
		return bafSender.SendBAFCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	_, ctx := bafSender.SendBAFArgsForCall(0)
	for i := 1; i < 10; i++ {
		_, next := bafSender.SendBAFArgsForCall(i)
		require.Truef(t, next == ctx, "batch %d got a new BAF context", i)
	}
	require.NoError(t, ctx.Err())

	termProvider.setState(b, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 1}}})
	select {
	case <-inFlightDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the in-flight SendBAF did not get a cancelled context")
	}
}

// TestSecondaryBAFContextPerRole checks that the secondary reuses one BAF context and cancels it on a term change.
func TestSecondaryBAFContextPerRole(t *testing.T) {
	b := createBatcher(t, 2, 0, []arma_types.PartyID{1, 2, 3, 4}, 4, testutil.CreateLogger(t, 2))
	batchChan := make(chan arma_types.Batch)
	puller := &mocks.FakeBatchesPuller{}
	puller.PullBatchesReturns(batchChan)
	b.BatchPuller = puller
	bafSender := &mocks.FakeBAFSender{}
	// The last BAF blocks until its context is cancelled.
	inFlightDone := make(chan struct{})
	bafSender.SendBAFCalls(func(baf arma_types.BatchAttestationFragment, ctx context.Context) {
		if baf.Seq() == 9 {
			<-ctx.Done()
			close(inFlightDone)
		}
	})
	b.BAFSender = bafSender
	b.ConfigSequenceGetter = &mocks.FakeConfigSequenceGetter{}
	termProvider := newTestTermProvider([]arma_types.PartyID{1, 2, 3, 4}, 0, 4)
	b.TermProvider = termProvider

	b.Start()
	defer b.Stop()

	for seq := 0; seq < 10; seq++ {
		batchChan <- arma_types.NewSimpleBatch(0, 1, arma_types.BatchSequence(seq), arma_types.BatchedRequests{[]byte{1}}, 0, nil)
	}
	require.Eventually(t, func() bool {
		return bafSender.SendBAFCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	_, ctx := bafSender.SendBAFArgsForCall(0)
	for i := 1; i < 10; i++ {
		_, next := bafSender.SendBAFArgsForCall(i)
		require.Truef(t, next == ctx, "batch %d got a new BAF context", i)
	}
	require.NoError(t, ctx.Err())

	termProvider.setState(b, &state.State{Shards: []state.ShardTerm{{Shard: 0, Term: 1}}})
	select {
	case <-inFlightDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the in-flight SendBAF did not get a cancelled context")
	}
}
