package arma

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type naiveConsensusLedger chan BatchAttestation

func (n naiveConsensusLedger) Append(_ uint64, blockHeaders BatchAttestation) {
	n <- blockHeaders
}

type naiveTotalOrder struct {
	input    chan [][]byte
	feedback func([]BatchAttestationFragment, []byte)
}

func (n *naiveTotalOrder) Deliver() ([]ControlEvent, func([]BatchAttestationFragment, []byte)) {
	batch := <-n.input

	bafs := make([]ControlEvent, 0, len(batch))

	for i := 0; i < len(batch); i++ {
		ba := &SimpleBatchAttestationFragment{}
		if err := ba.Deserialize(batch[i]); err != nil {
			panic(err)
		}

		bafs = append(bafs, ControlEvent{BAF: ba})
	}

	return bafs, n.feedback
}

func (n naiveTotalOrder) SubmitRequest(req []byte) error {
	n.input <- [][]byte{req}
	return nil
}

type naiveblock struct {
	seq         uint64
	batch       Batch
	attestation BatchAttestation
}

type naiveBlockLedger chan naiveblock

func (n naiveBlockLedger) Append(seq uint64, batch Batch, attestation BatchAttestation) {
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

func (s *shardCommitter) Append(party uint16, _ uint64, rawBatch []byte) {
	nb := &naiveBatch{
		requests: BatchFromRaw(rawBatch),
		node:     party,
	}

	s.sr.subscribers[s.shardID] <- nb
}

type shardReplicator struct {
	subscribers []chan Batch
}

func (s *shardReplicator) Replicate(shard uint16, _ uint64) <-chan Batch {
	return s.subscribers[shard]
}

func TestAssemblerBatcherConsenter(t *testing.T) {
	logger := createLogger(t, 0)
	shardCount := 10

	_, _, baReplicator, assembler := createAssembler(t, shardCount)
	blockLedger := make(naiveBlockLedger, 1000)
	assembler.Ledger = blockLedger

	replicator := &shardReplicator{}
	for i := 0; i < shardCount; i++ {
		replicator.subscribers = append(replicator.subscribers, make(chan Batch, 1000))
	}
	assembler.Replicator = replicator
	assembler.Logger = logger

	consenterLedger := make(naiveConsensusLedger)

	var totalOrder naiveTotalOrder
	totalOrder.input = make(chan [][]byte, 1000)
	totalOrder.feedback = func(bafs []BatchAttestationFragment, _ []byte) {
		if len(bafs) == 0 {
			fmt.Println("Empty fragments")
			return
		}
		ba := SimpleBatchAttestation{F: make([]SimpleBatchAttestationFragment, len(bafs))}
		for i := 0; i < len(bafs); i++ {
			ba.F[i] = SimpleBatchAttestationFragment{}
			ba.F[i].Deserialize(bafs[i].Serialize())
		}
		consenterLedger.Append(0, &ba)
	}

	consenter := &Consenter{
		State: State{
			Threshold:  1,
			N:          1,
			ShardCount: 1,
			Shards:     []ShardTerm{{Shard: 1, Term: 1}},
		},
		Logger:     logger,
		TotalOrder: &totalOrder,
	}

	go func() {
		for ba := range consenterLedger {
			baReplicator <- ba
		}
	}()

	var batchers []*Batcher

	for shardID := 0; shardID < shardCount; shardID++ {
		batcher := createBatcher(t, shardID, 0)
		batcher.Logger = logger
		batcher.TotalOrderBAF = func(baf BatchAttestationFragment) {
			ba := &SimpleBatchAttestationFragment{
				Dig: baf.Digest(),
				Se:  int(baf.Seq()),
			}
			consenter.Submit(ba.Serialize())
		}
		batcher.Threshold = 1
		batchers = append(batchers, batcher)
	}

	for i := 0; i < shardCount; i++ {
		sc := &shardCommitter{
			shardID: uint16(i),
			sr:      replicator,
		}
		batcher := batchers[i]
		batcher.AckBAF = func(seq uint64, to uint16) {
			batchers[to].HandleAck(seq, batcher.ID)
		}
		batchers[i].Ledger = sc
		batchers[i].Replicator = nil
		batchers[i].Primary = uint16(i)
		batchers[i].ID = uint16(i)
		batchers[i].Run()
	}

	time.Sleep(100 * time.Millisecond)

	assembler.Run()
	consenter.Run()

	router := &Router{
		Logger:         logger,
		RequestToShard: CRC32RequestToShard(uint16(shardCount)),
		Forward: func(shard uint16, request []byte) (BackendError, error) {
			err := batchers[shard].Submit(request)
			if err != nil {
				return fmt.Errorf("%s", err.Error()), nil
			}
			return nil, nil
		},
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
				submittedRequests.Store(binary.BigEndian.Uint32(req), struct{}{})
				atomic.AddUint32(&submittedCount, 1)
				router.Submit(req)
			}
		}(worker)
	}

	for committedReqCount < workerNum*workerPerWorker {
		block := <-blockLedger
		requests := block.batch.Requests()
		committedReqCount += len(requests)
		for _, req := range requests {
			submittedRequests.Delete(binary.BigEndian.Uint32(req))
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
