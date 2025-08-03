/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package core_test

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	arma_types_mocks "github.com/hyperledger/fabric-x-orderer/common/types/mocks"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/core/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

func TestPrimaryBatcherSimple(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	stateProvider := &mocks.FakeStateProvider{}
	batcher.StateProvider = stateProvider

	batcher.Start()

	require.Eventually(t, func() bool {
		return stateProvider.GetLatestStateChanCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()
	batcher.Stop()

	require.True(t, pool.RestartArgsForCall(0))
	require.NotZero(t, pool.NextRequestsCallCount())
}

func TestSecondaryBatcherSimple(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 1)
	reqs = append(reqs, req)

	batch := &arma_types_mocks.FakeBatch{}
	batch.PrimaryReturns(1)
	batch.RequestsReturns(reqs)
	batch.DigestReturns(reqs.Digest())

	batchPuller := &mocks.FakeBatchPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	stateProvider := &mocks.FakeStateProvider{}
	batcher.StateProvider = stateProvider

	batcher.Start()

	require.Eventually(t, func() bool {
		return stateProvider.GetLatestStateChanCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batch.SeqReturns(0)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batch.SeqReturns(1)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.Equal(t, 2, pool.RemoveRequestsCallCount())
	require.Zero(t, pool.NextRequestsCallCount())
}

func TestPrimaryChangeToSecondary(t *testing.T) {
	N := uint16(4)
	batchers := []arma_types.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	stateProvider := &mocks.FakeStateProvider{}
	stateChan := make(chan *core.State)
	stateProvider.GetLatestStateChanReturns(stateChan)
	batcher.StateProvider = stateProvider

	batch := &arma_types_mocks.FakeBatch{}
	batch.PrimaryReturns(2)
	batch.SeqReturns(0)
	batch.RequestsReturns(reqs)
	batch.DigestReturns(reqs.Digest())

	batchPuller := &mocks.FakeBatchPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	require.Eventually(t, func() bool {
		return stateProvider.GetLatestStateChanCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	}

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.NotZero(t, pool.NextRequestsCallCount())

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	}

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

	batcher := createBatcher(arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	stateProvider := &mocks.FakeStateProvider{}
	stateChan := make(chan *core.State)
	stateProvider.GetLatestStateChanReturns(stateChan)
	batcher.StateProvider = stateProvider

	batch := &arma_types_mocks.FakeBatch{}
	batch.PrimaryReturns(1)
	batch.RequestsReturns(reqs)
	batch.DigestReturns(reqs.Digest())

	batchPuller := &mocks.FakeBatchPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	require.Eventually(t, func() bool {
		return stateProvider.GetLatestStateChanCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	}

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Zero(t, pool.NextRequestsCallCount())

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	}

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

	batcher := createBatcher(arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 1)
	reqs = append(reqs, req)

	batch := &arma_types_mocks.FakeBatch{}
	batch.PrimaryReturns(1)
	batch.RequestsReturns(reqs)
	batch.DigestReturns(reqs.Digest())

	batchPuller := &mocks.FakeBatchPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	stateProvider := &mocks.FakeStateProvider{}
	stateChan := make(chan *core.State)
	stateProvider.GetLatestStateChanReturns(stateChan)
	batcher.StateProvider = stateProvider

	batcher.Start()

	require.Eventually(t, func() bool {
		return stateProvider.GetLatestStateChanCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	}

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	}

	require.Eventually(t, func() bool {
		return pool.RestartCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.False(t, pool.RestartArgsForCall(1))

	require.Eventually(t, func() bool {
		return batchPuller.StopCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batch.PrimaryReturns(2)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.False(t, pool.RestartArgsForCall(1))
	require.Equal(t, 2, pool.RemoveRequestsCallCount())
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

	batcher := createBatcher(arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	stateProvider := &mocks.FakeStateProvider{}
	stateChan := make(chan *core.State)
	stateProvider.GetLatestStateChanReturns(stateChan)
	batcher.StateProvider = stateProvider

	batcher.Start()

	require.Eventually(t, func() bool {
		return stateProvider.GetLatestStateChanCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	}

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.NotZero(t, pool.NextRequestsCallCount())

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  4,
			},
		},
	}

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

	batcher := createBatcher(arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturns(reqs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

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

	batcher := createBatcher(arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturns(reqs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	stateProvider := &mocks.FakeStateProvider{}
	stateChan := make(chan *core.State)
	stateProvider.GetLatestStateChanReturns(stateChan)
	batcher.StateProvider = stateProvider

	batch := &arma_types_mocks.FakeBatch{}
	batch.PrimaryReturns(2)
	batch.RequestsReturns(reqs)
	batch.DigestReturns(reqs.Digest())

	batchPuller := &mocks.FakeBatchPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	}

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return pool.NextRequestsCallCount() == 10
	}, 10*time.Second, 10*time.Millisecond)

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	}

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

	batcher := createBatcher(arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	stateProvider := &mocks.FakeStateProvider{}
	stateChan := make(chan *core.State)
	stateProvider.GetLatestStateChanReturns(stateChan)
	batcher.StateProvider = stateProvider

	batch := &arma_types_mocks.FakeBatch{}
	batch.PrimaryReturns(1)
	batch.RequestsReturns(reqs)
	batch.DigestReturns(reqs.Digest())

	batchPuller := &mocks.FakeBatchPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	require.Eventually(t, func() bool {
		return stateProvider.GetLatestStateChanCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  0,
			},
		},
	}

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Zero(t, pool.NextRequestsCallCount())

	require.Zero(t, pool.SubmitCallCount())

	ledger.RetrieveBatchByNumberReturns(batch)

	myBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), arma_types.PartyID(batcherID), nil, 0, nil)
	notMyBAF := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary(), batch.Seq(), batch.Digest(), arma_types.PartyID(batcherID+1), nil, 0, nil)
	myBAFWithOtherPrimary := arma_types.NewSimpleBatchAttestationFragment(batch.Shard(), batch.Primary()+1, batch.Seq(), batch.Digest(), arma_types.PartyID(batcherID), nil, 0, nil)

	stateChan <- &core.State{
		Shards: []core.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
		Pending: []arma_types.BatchAttestationFragment{myBAF, notMyBAF, myBAFWithOtherPrimary},
	}

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
	secondaryBatcher := createBatcher(arma_types.PartyID(batcherID), arma_types.ShardID(shardID), batchers, N, logger)
	verifier := &mocks.FakeBatchedRequestsVerifier{}
	verifier.VerifyBatchedRequestsReturns(nil)
	secondaryBatcher.BatchedRequestsVerifier = verifier
	complainer := &mocks.FakeComplainer{}
	secondaryBatcher.Complainer = complainer

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma_types.BatchedRequests, 1)
	reqs = append(reqs, req)

	batch := &arma_types_mocks.FakeBatch{}
	batch.PrimaryReturns(1)
	batch.RequestsReturns(reqs)
	batch.DigestReturns(reqs.Digest())

	batchPuller := &mocks.FakeBatchPuller{}
	batchChan := make(chan arma_types.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	secondaryBatcher.BatchPuller = batchPuller

	ledger := &mocks.FakeBatchLedger{}
	secondaryBatcher.Ledger = ledger

	secondaryBatcher.Start()
	defer secondaryBatcher.Stop()

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	batch.PrimaryReturns(2)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)
	batch.PrimaryReturns(1)

	batch.ShardReturns(1)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)
	batch.ShardReturns(0)

	batch.SeqReturns(2)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 3
	}, 10*time.Second, 10*time.Millisecond)
	batch.SeqReturns(1)

	batch.RequestsReturns(nil)
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 4
	}, 10*time.Second, 10*time.Millisecond)
	batch.RequestsReturns(reqs)

	batch.DigestReturns([]byte{1})
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 5
	}, 10*time.Second, 10*time.Millisecond)
	batch.DigestReturns(reqs.Digest())

	verifier.VerifyBatchedRequestsReturns(errors.New(""))
	batchChan <- batch
	require.Eventually(t, func() bool {
		return complainer.ComplainCallCount() == 6
	}, 10*time.Second, 10*time.Millisecond)
	verifier.VerifyBatchedRequestsReturns(nil)

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)
}

func createBatcher(batcherID arma_types.PartyID, shardID arma_types.ShardID, batchers []arma_types.PartyID, N uint16, logger arma_types.Logger) *core.Batcher {
	bafCreator := &mocks.FakeBAFCreator{}
	bafCreator.CreateBAFCalls(func(seq arma_types.BatchSequence, primary arma_types.PartyID, si arma_types.ShardID, digest []byte) arma_types.BatchAttestationFragment {
		return arma_types.NewSimpleBatchAttestationFragment(shardID, primary, seq, digest, batcherID, nil, 0, nil)
	})

	batcher := &core.Batcher{
		Batchers:                batchers,
		BatchTimeout:            time.Millisecond * 500,
		RequestInspector:        &mocks.FakeRequestInspector{},
		ID:                      batcherID,
		Shard:                   shardID,
		Threshold:               2,
		N:                       N,
		Logger:                  logger,
		Ledger:                  &mocks.FakeBatchLedger{},
		BatchPuller:             &mocks.FakeBatchPuller{},
		StateProvider:           &mocks.FakeStateProvider{},
		BAFCreator:              bafCreator,
		BAFSender:               &mocks.FakeBAFSender{},
		BatchAcker:              &mocks.FakeBatchAcker{},
		MemPool:                 &mocks.FakeMemPool{},
		BatchedRequestsVerifier: &mocks.FakeBatchedRequestsVerifier{},
		BatchSequenceGap:        arma_types.BatchSequence(10),
	}

	return batcher
}
