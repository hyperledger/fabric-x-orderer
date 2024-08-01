package arma_test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"arma/testutil"

	arma_types "arma/common/types"
	arma "arma/core"
	"arma/core/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchBytes(t *testing.T) {
	var b arma.BatchedRequests
	for i := 0; i < 10; i++ {
		req := make([]byte, 100)
		rand.Read(req)
		b = append(b, req)
	}

	b2 := arma.BatchFromRaw(b.ToBytes())
	assert.Equal(t, b, b2)
}

func TestPrimaryBatcherSimple(t *testing.T) {
	N := uint16(4)
	batchers := []arma.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(arma.PartyID(batcherID), arma.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma.BatchedRequests, 0, 1)
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

	require.True(t, pool.RestartArgsForCall(0))
	require.NotZero(t, pool.NextRequestsCallCount())
}

func TestSecondaryBatcherSimple(t *testing.T) {
	N := uint16(4)
	batchers := []arma.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(arma.PartyID(batcherID), arma.ShardID(shardID), batchers, N, logger)

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma.BatchedRequests, 1)
	reqs = append(reqs, req)

	batch := &mocks.FakeBatch{}
	batch.PartyReturns(1)
	batch.RequestsReturns(reqs)

	batchPuller := &mocks.FakeBatchPuller{}
	batchChan := make(chan arma.Batch)
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

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

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
	batchers := []arma.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(arma.PartyID(batcherID), arma.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	stateProvider := &mocks.FakeStateProvider{}
	stateChan := make(chan *arma.State)
	stateProvider.GetLatestStateChanReturns(stateChan)
	batcher.StateProvider = stateProvider

	batch := &mocks.FakeBatch{}
	batch.PartyReturns(1)
	batch.RequestsReturns(reqs)

	batchPuller := &mocks.FakeBatchPuller{}
	batchChan := make(chan arma.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	require.Eventually(t, func() bool {
		return stateProvider.GetLatestStateChanCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	stateChan <- &arma.State{
		Shards: []arma.ShardTerm{
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

	stateChan <- &arma.State{
		Shards: []arma.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	}

	require.Eventually(t, func() bool {
		return pool.RestartArgsForCall(1) == false
	}, 10*time.Second, 10*time.Millisecond)

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
	batchers := []arma.PartyID{1, 2, 3, 4}
	batcherID := 2
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(arma.PartyID(batcherID), arma.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	stateProvider := &mocks.FakeStateProvider{}
	stateChan := make(chan *arma.State)
	stateProvider.GetLatestStateChanReturns(stateChan)
	batcher.StateProvider = stateProvider

	batch := &mocks.FakeBatch{}
	batch.PartyReturns(1)
	batch.RequestsReturns(reqs)

	batchPuller := &mocks.FakeBatchPuller{}
	batchChan := make(chan arma.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	batcher.Start()

	require.Eventually(t, func() bool {
		return stateProvider.GetLatestStateChanCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	stateChan <- &arma.State{
		Shards: []arma.ShardTerm{
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

	stateChan <- &arma.State{
		Shards: []arma.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	}

	require.Eventually(t, func() bool {
		return pool.RestartArgsForCall(1) == true
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	require.NotZero(t, pool.NextRequestsCallCount())

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.True(t, pool.RestartArgsForCall(1))

	require.Equal(t, arma.PartyID(1), ledger.HeightArgsForCall(0))
	require.Equal(t, arma.PartyID(2), ledger.HeightArgsForCall(1))
}

func TestSecondaryChangeToSecondary(t *testing.T) {
	N := uint16(4)
	batchers := []arma.PartyID{1, 2, 3, 4}
	batcherID := 3
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(arma.PartyID(batcherID), arma.ShardID(shardID), batchers, N, logger)

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma.BatchedRequests, 1)
	reqs = append(reqs, req)

	batch := &mocks.FakeBatch{}
	batch.PartyReturns(1)
	batch.RequestsReturns(reqs)

	batchPuller := &mocks.FakeBatchPuller{}
	batchChan := make(chan arma.Batch)
	batchPuller.PullBatchesReturns(batchChan)
	batcher.BatchPuller = batchPuller

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	stateProvider := &mocks.FakeStateProvider{}
	stateChan := make(chan *arma.State)
	stateProvider.GetLatestStateChanReturns(stateChan)
	batcher.StateProvider = stateProvider

	batcher.Start()

	require.Eventually(t, func() bool {
		return stateProvider.GetLatestStateChanCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	stateChan <- &arma.State{
		Shards: []arma.ShardTerm{
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

	stateChan <- &arma.State{
		Shards: []arma.ShardTerm{
			{
				Shard: 0,
				Term:  1,
			},
		},
	}

	require.Eventually(t, func() bool {
		return pool.RestartArgsForCall(1) == false
	}, 10*time.Second, 10*time.Millisecond)

	batchChan <- batch
	require.Eventually(t, func() bool {
		return ledger.AppendCallCount() == 2
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.False(t, pool.RestartArgsForCall(0))
	require.False(t, pool.RestartArgsForCall(1))
	require.Equal(t, 2, pool.RemoveRequestsCallCount())
	require.Zero(t, pool.NextRequestsCallCount())
}

func TestPrimaryChangeToPrimary(t *testing.T) {
	N := uint16(4)
	batchers := []arma.PartyID{1, 2, 3, 4}
	batcherID := 1
	shardID := 0

	logger := testutil.CreateLogger(t, batcherID)

	batcher := createBatcher(arma.PartyID(batcherID), arma.ShardID(shardID), batchers, N, logger)

	pool := &mocks.FakeMemPool{}
	batcher.MemPool = pool

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	reqs := make(arma.BatchedRequests, 0, 1)
	reqs = append(reqs, req)

	pool.NextRequestsReturnsOnCall(1, reqs)

	ledger := &mocks.FakeBatchLedger{}
	batcher.Ledger = ledger

	stateProvider := &mocks.FakeStateProvider{}
	stateChan := make(chan *arma.State)
	stateProvider.GetLatestStateChanReturns(stateChan)
	batcher.StateProvider = stateProvider

	batcher.Start()

	require.Eventually(t, func() bool {
		return stateProvider.GetLatestStateChanCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	stateChan <- &arma.State{
		Shards: []arma.ShardTerm{
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

	stateChan <- &arma.State{
		Shards: []arma.ShardTerm{
			{
				Shard: 0,
				Term:  4,
			},
		},
	}

	require.Eventually(t, func() bool {
		return pool.RestartArgsForCall(1) == true
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.True(t, pool.RestartArgsForCall(0))
	require.True(t, pool.RestartArgsForCall(1))
}

func createBatcher(batcherID arma.PartyID, shardID arma.ShardID, batchers []arma.PartyID, N uint16, logger arma.Logger) *arma.Batcher {
	digestFunc := func(data [][]byte) []byte {
		h := sha256.New()
		for _, d := range data {
			h.Write(d)
		}
		return h.Sum(nil)
	}

	batcher := &arma.Batcher{
		Batchers:         batchers,
		BatchTimeout:     0,
		Digest:           digestFunc,
		RequestInspector: &mocks.FakeRequestInspector{},
		ID:               arma.PartyID(batcherID),
		Shard:            arma.ShardID(shardID),
		Threshold:        1,
		N:                N,
		Logger:           logger,
		Ledger:           &mocks.FakeBatchLedger{},
		BatchPuller:      &mocks.FakeBatchPuller{},
		StateProvider:    &mocks.FakeStateProvider{},
		AttestBatch: func(seq uint64, primary arma.PartyID, shard arma.ShardID, digest []byte) arma.BatchAttestationFragment {
			return &arma_types.SimpleBatchAttestationFragment{
				Dig: digest,
				Sh:  int(shardID),
				Si:  int(batcherID),
				P:   int(primary),
				Se:  int(seq),
			}
		},
		TotalOrderBAF: func(arma.BatchAttestationFragment) {},
		AckBAF:        func(seq uint64, to arma.PartyID) {},
		MemPool:       &mocks.FakeMemPool{},
	}

	return batcher
}
