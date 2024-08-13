package core_test

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	arma_types "arma/common/types"
	"arma/core"
	"arma/core/mocks"
	"arma/testutil"

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

func (n naiveBlockLedger) Append(seq uint64, batch core.Batch, attestation core.BatchAttestation) {
	n <- naiveblock{
		seq:         seq,
		batch:       batch,
		attestation: attestation,
	}
}

type shardCommitter struct {
	sr      *shardReplicator
	shardID uint16
}

func (s *shardCommitter) Append(party core.PartyID, _ uint64, rawBatch []byte) {
	nb := &naiveBatch{
		requests: core.BatchFromRaw(rawBatch),
		node:     party,
	}

	s.sr.subscribers[s.shardID] <- nb
}

func (r *shardCommitter) Height(partyID core.PartyID) uint64 {
	return 0
}

func (r *shardCommitter) RetrieveBatchByNumber(partyID core.PartyID, seq uint64) core.Batch {
	return nil
}

type shardReplicator struct {
	subscribers []chan core.Batch
}

func (s *shardReplicator) Replicate(shard core.ShardID) <-chan core.Batch {
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

	for shardID := uint16(1); shardID <= initialState.ShardCount; shardID++ {
		initialState.Shards = append(initialState.Shards, core.ShardTerm{Shard: core.ShardID(shardID), Term: 1})
	}

	consenter := &core.Consenter{
		State:           initialState,
		DB:              &mockBatchAttestationDB{},
		BAFDeserializer: &BAFSimpleDeserializer{},
		Logger:          logger,
		TotalOrder:      &totalOrder,
	}

	go func() {
		var events [][]byte
		state := consenter.State.Clone()
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
					baReplicator <- ba
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
		state.Shards = append(state.Shards, core.ShardTerm{Shard: core.ShardID(shard)})
	}

	var parties []core.PartyID
	for shardID := 0; shardID < shardCount; shardID++ {
		parties = append(parties, core.PartyID(shardID))
	}

	for shardID := 0; shardID < shardCount; shardID++ {
		batcher := createTestBatcher(t, shardID, shardID, parties)
		batcher.Logger = logger
		batcher.TotalOrderBAF = func(baf core.BatchAttestationFragment) {
			ba := &arma_types.SimpleBatchAttestationFragment{
				Dig: baf.Digest(),
				Se:  int(baf.Seq()),
				Sh:  int(baf.Shard()),
				Si:  int(baf.Signer()),
				P:   int(baf.Primary()),
			}
			consenter.Submit((&core.ControlEvent{
				BAF: ba,
			}).Bytes())
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
		batcher := batchers[i]
		batcher.AckBAF = func(seq core.BatchSequence, to core.PartyID) {
			batchers[to].HandleAck(seq, batcher.ID)
		}
		batchers[i].Ledger = sc
		batchers[i].BatchPuller = nil
		batchers[i].N = state.N
		batchers[i].StateProvider = sp
		batchers[i].ID = core.PartyID(i)
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

	for committedReqCount < workerNum*workerPerWorker {
		block := <-blockLedger
		requests := block.batch.Requests()
		committedReqCount += len(requests)
		for _, req := range requests {
			submittedRequests.Delete(binary.BigEndian.Uint64(req))
		}
		fmt.Println("committed:", committedReqCount, "submitted:", atomic.LoadUint32(&submittedCount))
	}

	wg.Wait()

	var remainingRequests int
	submittedRequests.Range(func(_, _ interface{}) bool {
		remainingRequests++
		return true
	})

	assert.Equal(t, 0, remainingRequests)
	fmt.Println(committedReqCount, time.Since(t1))
}
