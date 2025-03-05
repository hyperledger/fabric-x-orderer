package core_test

import (
	"encoding/binary"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	arma_types "github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/core/mocks"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.com/stretchr/testify/assert"
)

type mockBatchAttestationDB struct{}

func (m *mockBatchAttestationDB) Exists(digest []byte) bool {
	return false
}

func (m *mockBatchAttestationDB) Put(digest [][]byte, epoch []uint64) {
}

func (m *mockBatchAttestationDB) Clean(epoch uint64) {
}

type naiveTotalOrder chan []byte

func (n naiveTotalOrder) SubmitRequest(req []byte) error {
	n <- req
	return nil
}

type naiveblock struct {
	seq         uint64
	batch       core.Batch
	attestation core.BatchAttestation
}

type naiveBlockLedger chan naiveblock

func (n naiveBlockLedger) Append(batch core.Batch, orderingInfo interface{}) {
	n <- naiveblock{
		seq:   orderingInfo.(uint64),
		batch: batch,
		attestation: &naiveBatchAttestation{
			primary: batch.Primary(),
			seq:     batch.Seq(),
			shard:   batch.Shard(),
			digest:  batch.Digest(),
		},
	}
}

func (n naiveBlockLedger) Close() {}

type shardCommitter struct {
	sr      *shardReplicator
	shardID uint16
}

func (s *shardCommitter) Append(party arma_types.PartyID, _ uint64, rawBatch []byte) {
	var reqs arma_types.BatchedRequests
	reqs.Deserialize(rawBatch)
	nb := &naiveBatch{
		requests: reqs,
		node:     party,
	}

	s.sr.subscribers[s.shardID] <- nb
}

func (r *shardCommitter) Height(partyID arma_types.PartyID) uint64 {
	return 0
}

func (r *shardCommitter) RetrieveBatchByNumber(partyID arma_types.PartyID, seq uint64) core.Batch {
	return nil
}

type shardReplicator struct {
	subscribers []chan core.Batch
}

func (s *shardReplicator) Replicate(shard arma_types.ShardID) <-chan core.Batch {
	return s.subscribers[shard]
}

type stateProvider struct {
	s *core.State
}

func (s *stateProvider) GetLatestStateChan() <-chan *core.State {
	stateChan := make(chan *core.State, 1)
	stateChan <- s.s
	return stateChan
}

type BAFSimpleDeserializer struct{}

func (bafd *BAFSimpleDeserializer) Deserialize(bytes []byte) (core.BatchAttestationFragment, error) {
	var baf arma_types.SimpleBatchAttestationFragment
	if err := baf.Deserialize(bytes); err != nil {
		return nil, err
	}
	return &baf, nil
}

func TestAssemblerBatcherConsenter(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	shardCount := 10

	_, _, baReplicator, assembler := createAssembler(t, shardCount)
	blockLedger := make(naiveBlockLedger, 1000)
	assembler.Ledger = blockLedger

	replicator := &shardReplicator{}
	for i := 0; i < shardCount; i++ {
		replicator.subscribers = append(replicator.subscribers, make(chan core.Batch, 1000))
	}
	assembler.Replicator = replicator
	assembler.Logger = logger

	totalOrder := make(naiveTotalOrder, 1000)

	initialState := &core.State{
		Threshold:  1,
		N:          1,
		ShardCount: uint16(shardCount),
	}

	for shardID := uint16(0); shardID < initialState.ShardCount; shardID++ {
		initialState.Shards = append(initialState.Shards, core.ShardTerm{Shard: arma_types.ShardID(shardID), Term: 1})
	}

	consenter := &core.Consenter{
		State:           initialState,
		DB:              &mockBatchAttestationDB{},
		BAFDeserializer: &BAFSimpleDeserializer{},
		Logger:          logger,
	}

	go func() {
		var events [][]byte
		state := consenter.State.Clone()
		num := uint64(0)
		for {
			select {
			case <-time.After(time.Millisecond * 100):
				if len(events) == 0 {
					continue
				}
				consenter.Commit(events)
				newState, aggregatedBAFs := consenter.SimulateStateTransition(state, events)
				state = newState
				for _, bafs := range aggregatedBAFs {
					ba := &mocks.FakeBatchAttestation{}
					ba.DigestReturns(bafs[0].Digest())
					ba.FragmentsReturns(bafs)
					oba := &naiveOrderedBatchAttestation{
						ba:           ba,
						orderingInfo: num,
					}
					baReplicator <- oba
					num++
				}
				events = nil

			case event := <-totalOrder:
				events = append(events, event)
			}
		}
	}()

	var batchers []*core.Batcher

	state := core.State{N: uint16(shardCount)}
	for shard := 0; shard < shardCount; shard++ {
		state.Shards = append(state.Shards, core.ShardTerm{Shard: arma_types.ShardID(shard)})
	}

	var parties []arma_types.PartyID
	for shardID := 0; shardID < shardCount; shardID++ {
		parties = append(parties, arma_types.PartyID(shardID))
	}

	for shardID := 0; shardID < shardCount; shardID++ {
		batcher := createTestBatcher(t, arma_types.ShardID(shardID), arma_types.PartyID(shardID), parties)
		batcher.Logger = logger
		batcher.TotalOrderBAF = func(baf core.BatchAttestationFragment) {
			ba := arma_types.NewSimpleBatchAttestationFragment(baf.Shard(), baf.Primary(), baf.Seq(), baf.Digest(), baf.Signer(), nil, 0, nil)
			totalOrder.SubmitRequest((&core.ControlEvent{BAF: ba}).Bytes())
		}
		batcher.Threshold = 1
		batchers = append(batchers, batcher)
	}

	sp := &stateProvider{s: &state}

	for i := 0; i < shardCount; i++ {
		sc := &shardCommitter{
			shardID: uint16(i),
			sr:      replicator,
		}
		acker := &acker{from: arma_types.PartyID(i), batchers: batchers}
		batchers[i].BatchAcker = acker
		batchers[i].Ledger = sc
		batchers[i].BatchPuller = nil
		batchers[i].N = state.N
		batchers[i].StateProvider = sp
		batchers[i].ID = arma_types.PartyID(i)
		batchers[i].Start()
		defer batchers[i].Stop()
	}

	time.Sleep(100 * time.Millisecond)

	assembler.Run()

	router := &core.Router{
		Logger:     logger,
		ShardCount: uint16(shardCount),
	}

	var submittedRequests sync.Map
	var submittedCount uint32
	var committedReqCount int

	workerNum := runtime.NumCPU()
	workerPerWorker := 20000

	var wg sync.WaitGroup
	wg.Add(workerNum)

	t1 := time.Now()

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workerPerWorker; i++ {
				req := make([]byte, 8)
				binary.BigEndian.PutUint32(req, uint32(worker))
				binary.BigEndian.PutUint32(req[4:], uint32(i))
				submittedRequests.Store(binary.BigEndian.Uint64(req), struct{}{})
				atomic.AddUint32(&submittedCount, 1)
				shardID, _ := router.Map(req)
				err := batchers[shardID].Submit(req)
				if err != nil {
					panic(err)
				}
			}
		}(worker)
	}

	num := uint64(0)
	for committedReqCount < workerNum*workerPerWorker {
		block := <-blockLedger
		assert.Equal(t, num, block.seq)
		num++
		requests := block.batch.Requests()
		committedReqCount += len(requests)
		for _, req := range requests {
			submittedRequests.Delete(binary.BigEndian.Uint64(req))
		}
		t.Log("committed:", committedReqCount, "submitted:", atomic.LoadUint32(&submittedCount))
	}

	wg.Wait()

	var remainingRequests int
	submittedRequests.Range(func(_, _ interface{}) bool {
		remainingRequests++
		return true
	})

	assert.Equal(t, 0, remainingRequests)
	t.Log(committedReqCount, time.Since(t1))
}
