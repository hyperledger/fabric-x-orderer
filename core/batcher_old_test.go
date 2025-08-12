/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package core_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/core/mocks"
	"github.com/hyperledger/fabric-x-orderer/request"
	request_mocks "github.com/hyperledger/fabric-x-orderer/request/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

type reqInspector struct{}

func (ri *reqInspector) RequestID(req []byte) string {
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}

type noopLedger struct{}

func (*noopLedger) Append(partyID arma_types.PartyID, batchSeq arma_types.BatchSequence, batchedRequests arma_types.BatchedRequests) {
}

func (*noopLedger) Height(partyID arma_types.PartyID) uint64 {
	return 0
}

func (*noopLedger) RetrieveBatchByNumber(partyID arma_types.PartyID, seq uint64) arma_types.Batch {
	return nil
}

type naiveReplication struct {
	subscribers []chan arma_types.Batch
	i           uint32
	stopped     int32
}

func (r *naiveReplication) Replicate(_ arma_types.ShardID) <-chan arma_types.Batch {
	j := atomic.AddUint32(&r.i, 1)
	return r.subscribers[j-1]
}

func (r *naiveReplication) PullBatches(_ arma_types.PartyID) <-chan arma_types.Batch {
	j := atomic.AddUint32(&r.i, 1)
	return r.subscribers[j-1]
}

func (r *naiveReplication) Stop() {
	atomic.StoreInt32(&r.stopped, 0x1)
}

func (r *naiveReplication) Append(partyID arma_types.PartyID, batchSeq arma_types.BatchSequence, batchedRequests arma_types.BatchedRequests) {
	for _, s := range r.subscribers {
		s <- arma_types.NewSimpleBatch(batchSeq, 0, partyID, batchedRequests)
	}
}

func (r *naiveReplication) Height(partyID arma_types.PartyID) uint64 {
	// TODO use in test
	return 0
}

func (r *naiveReplication) RetrieveBatchByNumber(partyID arma_types.PartyID, seq uint64) arma_types.Batch {
	// TODO use in test
	return nil
}

type acker struct {
	from     arma_types.PartyID
	batchers []*core.Batcher
}

func (a *acker) Ack(seq arma_types.BatchSequence, to arma_types.PartyID) {
	a.batchers[to].HandleAck(seq, arma_types.PartyID(a.from))
}

func BenchmarkBatcherNetwork(b *testing.B) {
	n := 4
	batchers, commit := createBenchBatchers(b, n)
	for _, b := range batchers {
		b.Start()
	}

	go func() {
		for worker := 0; worker < 100; worker++ {
			go func(worker uint64) {
				var i int
				for {
					req := make([]byte, 512)
					binary.BigEndian.PutUint64(req, uint64(i))
					i++
					binary.BigEndian.PutUint64(req[500:], worker)
					for node := 0; node < n; node++ {
						batchers[node].Submit(req)
					}
					time.Sleep(time.Millisecond)
				}
			}(uint64(worker))
		}
	}()

	var committedRequestCount uint32

	var fastBatchesCommitted uint64

	go func() {
		for {
			time.Sleep(time.Second * 5)
			tps := atomic.LoadUint32(&committedRequestCount) / 5
			if tps > 50*1000 {
				b.Log("Fast batch committed; TPS:", tps)
				atomic.AddUint64(&fastBatchesCommitted, 1)
			} else {
				b.Log("TPS:", tps)
			}
			atomic.StoreUint32(&committedRequestCount, 0)
		}
	}()

	var committedRequests sync.Map

	for atomic.LoadUint64(&fastBatchesCommitted) < 5 {
		batch := <-commit
		requests := batch.Requests()
		atomic.AddUint32(&committedRequestCount, uint32(len(requests)))
		for _, req := range requests {
			_, loaded := committedRequests.LoadOrStore(string(req), struct{}{})
			if loaded {
				panic("request was delivered twice")
			}
		}
	}
}

func createBenchBatchers(b *testing.B, n int) ([]*core.Batcher, <-chan arma_types.Batch) {
	var batcherConf []arma_types.PartyID
	for i := 0; i < n; i++ {
		batcherConf = append(batcherConf, arma_types.PartyID(i))
	}

	var batchers []*core.Batcher
	for i := 0; i < n; i++ {
		b := createBenchBatcher(b, 0, arma_types.PartyID(i), batcherConf)
		batchers = append(batchers, b)
	}

	r := &naiveReplication{}

	for i := 1; i < n; i++ {
		r.subscribers = append(r.subscribers, make(chan arma_types.Batch, 100))
	}

	r.subscribers = append(r.subscribers, make(chan arma_types.Batch, 100))
	commit := r.PullBatches(0)

	batchers[0].Ledger = r
	for i := 1; i < n; i++ {
		batchers[i].BatchPuller = r
	}

	for i := 0; i < n; i++ {
		batchers[i].BAFSender = &mocks.FakeBAFSender{}
		acker := &acker{from: arma_types.PartyID(i), batchers: batchers}
		batchers[i].BatchAcker = acker
	}
	return batchers, commit
}

func createBenchBatcher(b *testing.B, shardID arma_types.ShardID, nodeID arma_types.PartyID, batchers []arma_types.PartyID) *core.Batcher {
	sugaredLogger := testutil.CreateBenchmarkLogger(b, int(nodeID))

	requestInspector := &reqInspector{}

	striker := &request_mocks.FakeStriker{}
	striker.OnFirstStrikeTimeoutStub = func(b []byte) {
		sugaredLogger.Info("OnFirstStrikeTimeout")
	}
	striker.OnSecondStrikeTimeoutStub = func() {
		sugaredLogger.Warn("OnSecondStrikeTimeout")
	}

	pool := request.NewPool(sugaredLogger, requestInspector, request.PoolOptions{
		MaxSize:               1000 * 100,
		BatchMaxSize:          10000,
		BatchMaxSizeBytes:     100000,
		RequestMaxBytes:       1024 * 100,
		SubmitTimeout:         time.Second * 10,
		FirstStrikeThreshold:  time.Second * 10,
		SecondStrikeThreshold: time.Minute / 2,
		AutoRemoveTimeout:     time.Minute / 2,
	}, striker)

	bafCreator := &mocks.FakeBAFCreator{}
	bafCreator.CreateBAFCalls(func(seq arma_types.BatchSequence, primary arma_types.PartyID, si arma_types.ShardID, digest []byte) arma_types.BatchAttestationFragment {
		return arma_types.NewSimpleBatchAttestationFragment(shardID, primary, seq, digest, nodeID, nil, 0, nil)
	})

	batcher := &core.Batcher{
		N:                       uint16(len(batchers)),
		Batchers:                batchers,
		BatchTimeout:            time.Millisecond * 500,
		Shard:                   arma_types.ShardID(shardID),
		BAFCreator:              bafCreator,
		RequestInspector:        requestInspector,
		Logger:                  sugaredLogger,
		MemPool:                 pool,
		ID:                      arma_types.PartyID(nodeID),
		Threshold:               2,
		Ledger:                  &noopLedger{},
		StateProvider:           &mocks.FakeStateProvider{},
		BatchedRequestsVerifier: &mocks.FakeBatchedRequestsVerifier{},
		BatchSequenceGap:        arma_types.BatchSequence(10),
	}

	return batcher
}

func TestBatchersStopSecondaries(t *testing.T) {
	n := 4

	var secondStrikeCount uint32

	striker := &request_mocks.FakeStriker{}
	striker.OnFirstStrikeTimeoutStub = func(b []byte) {
	}
	striker.OnSecondStrikeTimeoutStub = func() {
		atomic.AddUint32(&secondStrikeCount, 1)
	}

	batchers, _ := createBatchers(t, n)
	for _, b := range batchers {
		b.BatchAcker = &mocks.FakeBatchAcker{} // no ack will be received by the primary
		pool := request.NewPool(b.Logger, b.RequestInspector, request.PoolOptions{
			MaxSize:               100 * 1000,
			BatchMaxSize:          100, // batch can't include all requests
			BatchMaxSizeBytes:     100000,
			RequestMaxBytes:       1024 * 100,
			SubmitTimeout:         time.Second * 10,
			FirstStrikeThreshold:  time.Second * 1,
			SecondStrikeThreshold: time.Second * 2,
			AutoRemoveTimeout:     time.Minute / 2,
		}, striker)
		b.MemPool = pool
		b.Start()
	}

	var submits sync.WaitGroup
	submits.Add(100)

	go func() {
		for worker := 0; worker < 100; worker++ {
			go func(worker uint64) {
				defer submits.Done()
				var i int
				for j := 0; j < 1000; j++ {
					req := make([]byte, 512)
					binary.BigEndian.PutUint64(req, uint64(i))
					i++
					binary.BigEndian.PutUint64(req[500:], worker)
					for node := 0; node < n; node++ {
						batchers[node].Submit(req)
					}
					time.Sleep(time.Millisecond)
				}
			}(uint64(worker))
		}
	}()

	submits.Wait()
	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&secondStrikeCount) >= uint32(3)
	}, 1*time.Minute, 1*time.Second)

	for _, b := range batchers {
		b.Stop()
	}
}

func createBatchers(t *testing.T, n int) ([]*core.Batcher, <-chan arma_types.Batch) {
	var batcherConf []arma_types.PartyID
	for i := 0; i < n; i++ {
		batcherConf = append(batcherConf, arma_types.PartyID(i))
	}

	var batchers []*core.Batcher
	for i := 0; i < n; i++ {
		b := createTestBatcher(t, 0, arma_types.PartyID(i), batcherConf)
		batchers = append(batchers, b)
	}

	r := &naiveReplication{}

	for i := 1; i < n; i++ {
		r.subscribers = append(r.subscribers, make(chan arma_types.Batch, 100))
	}

	r.subscribers = append(r.subscribers, make(chan arma_types.Batch, 100))
	commit := r.PullBatches(0)

	batchers[0].Ledger = r
	for i := 1; i < n; i++ {
		batchers[i].BatchPuller = r
	}

	for i := 0; i < n; i++ {
		batchers[i].BAFSender = &mocks.FakeBAFSender{}
		acker := &acker{from: arma_types.PartyID(i), batchers: batchers}
		batchers[i].BatchAcker = acker
	}
	return batchers, commit
}

func createTestBatcher(t *testing.T, shardID arma_types.ShardID, nodeID arma_types.PartyID, batchers []arma_types.PartyID) *core.Batcher {
	sugaredLogger := testutil.CreateLogger(t, int(nodeID))

	striker := &request_mocks.FakeStriker{}
	striker.OnFirstStrikeTimeoutStub = func(b []byte) {
		sugaredLogger.Info("OnFirstStrikeTimeout")
	}
	striker.OnSecondStrikeTimeoutStub = func() {
		sugaredLogger.Warn("OnSecondStrikeTimeout")
	}

	requestInspector := &reqInspector{}
	pool := request.NewPool(sugaredLogger, requestInspector, request.PoolOptions{
		MaxSize:               1000 * 100,
		BatchMaxSize:          10000,
		BatchMaxSizeBytes:     100000,
		RequestMaxBytes:       1024 * 100,
		SubmitTimeout:         time.Second * 10,
		FirstStrikeThreshold:  time.Second * 10,
		SecondStrikeThreshold: time.Minute / 2,
		AutoRemoveTimeout:     time.Minute / 2,
	}, striker)

	bafCreator := &mocks.FakeBAFCreator{}
	bafCreator.CreateBAFCalls(func(seq arma_types.BatchSequence, primary arma_types.PartyID, si arma_types.ShardID, digest []byte) arma_types.BatchAttestationFragment {
		return arma_types.NewSimpleBatchAttestationFragment(shardID, primary, seq, digest, nodeID, nil, 0, nil)
	})

	b := &core.Batcher{
		N:                       uint16(len(batchers)),
		Batchers:                batchers,
		BatchTimeout:            time.Millisecond * 500,
		Shard:                   shardID,
		BAFCreator:              bafCreator,
		RequestInspector:        requestInspector,
		Logger:                  sugaredLogger,
		MemPool:                 pool,
		ID:                      nodeID,
		Threshold:               2,
		Ledger:                  &noopLedger{},
		StateProvider:           &mocks.FakeStateProvider{},
		Complainer:              &mocks.FakeComplainer{},
		BatchedRequestsVerifier: &mocks.FakeBatchedRequestsVerifier{},
		BatchSequenceGap:        arma_types.BatchSequence(10),
	}

	return b
}
