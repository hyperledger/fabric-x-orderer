package core_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	arma_types "arma/common/types"
	arma "arma/core"
	"arma/request"
	"arma/testutil"
)

type reqInspector struct{}

func (ri *reqInspector) RequestID(req []byte) string {
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}

type noopLedger struct{}

func (*noopLedger) Append(_ arma.PartyID, _ uint64, _ []byte) {
}

func (*noopLedger) Height(partyID arma.PartyID) uint64 {
	return 0
}

func (*noopLedger) RetrieveBatchByNumber(partyID arma.PartyID, seq uint64) arma.Batch {
	return nil
}

type naiveReplication struct {
	subscribers []chan arma.Batch
	i           uint32
}

func (r *naiveReplication) Replicate(_ arma.ShardID) <-chan arma.Batch {
	j := atomic.AddUint32(&r.i, 1)
	return r.subscribers[j-1]
}

func (r *naiveReplication) PullBatches(_ arma.PartyID) <-chan arma.Batch {
	j := atomic.AddUint32(&r.i, 1)
	return r.subscribers[j-1]
}

func (r *naiveReplication) Append(_ arma.PartyID, _ uint64, bytes []byte) {
	for _, s := range r.subscribers {
		s <- &naiveBatch{
			requests: arma.BatchFromRaw(bytes),
		}
	}
}

func (r *naiveReplication) Height(partyID arma.PartyID) uint64 {
	// TODO use in test
	return 0
}

func (r *naiveReplication) RetrieveBatchByNumber(partyID arma.PartyID, seq uint64) arma.Batch {
	// TODO use in test
	return nil
}

type naiveBatch struct {
	shardID  arma.ShardID
	node     arma.PartyID
	seq      uint64 // TODO use BatchSequence
	requests [][]byte
}

func (nb *naiveBatch) Party() arma.PartyID {
	return nb.node
}

func (nb *naiveBatch) Digest() []byte {
	h := sha256.New()
	for _, req := range nb.Requests() {
		h.Write(req)
	}
	return h.Sum(nil)
}

func (nb *naiveBatch) Shard() arma.ShardID {
	return nb.shardID
}

func (nb *naiveBatch) Seq() uint64 {
	return nb.seq
}

func (nb *naiveBatch) Requests() arma.BatchedRequests {
	return nb.requests
}

func TestBatcherNetwork(t *testing.T) {
	t.Skip()
	n := 3

	batchers, commit := createBatchers(t, n)

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
				atomic.AddUint64(&fastBatchesCommitted, 1)
			} else {
				fmt.Println("TPS:", tps)
			}
			atomic.StoreUint32(&committedRequestCount, 0)
		}
	}()

	var committedRequests sync.Map

	for atomic.LoadUint64(&fastBatchesCommitted) < 4 {
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

func TestBatchersStopSecondaries(t *testing.T) {
	t.Skip()
	n := 3

	var stopped sync.WaitGroup
	stopped.Add(n)

	state := arma.State{}
	for shard := 0; shard < 1; shard++ {
		state.Shards = append(state.Shards, arma.ShardTerm{Shard: arma.ShardID(shard), Term: 5})
	}

	batchers, _ := createBatchers(t, n)
	for _, b := range batchers {
		b := b
		b.AckBAF = func(_ uint64, _ arma.PartyID) {}
		pool := request.NewPool(b.Logger, b.RequestInspector, request.PoolOptions{
			FirstStrikeThreshold:  time.Second * 1,
			SecondStrikeThreshold: time.Second * 5,
			BatchMaxSize:          10000,
			MaxSize:               1000 * 100,
			AutoRemoveTimeout:     time.Minute / 2,
			SubmitTimeout:         time.Second * 10,
			OnFirstStrikeTimeout:  func([]byte) {},
			OnSecondStrikeTimeout: func() {
				go func() {
					fmt.Println("Stopping batcher", b.ID)
					defer stopped.Done()
					b.Stop()
				}()
			},
		})
		b.MemPool.Close()
		b.MemPool = pool
	}

	var submits sync.WaitGroup
	submits.Add(100)

	go func() {
		for worker := 0; worker < 100; worker++ {
			go func(worker uint64) {
				defer submits.Done()
				var i int
				for j := 0; j < 100; j++ {
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
	stopped.Wait()
}

func createBatchers(t *testing.T, n int) ([]*arma.Batcher, <-chan arma.Batch) {
	var batchers []*arma.Batcher

	var batcherConf []arma.PartyID
	for i := 0; i < n; i++ {
		batchers[i].Batchers = batcherConf
	}

	for i := 0; i < n; i++ {
		b := createTestBatcher(t, 0, i, batcherConf)
		batchers = append(batchers, b)
	}

	r := &naiveReplication{}

	for i := 1; i < n; i++ {
		r.subscribers = append(r.subscribers, make(chan arma.Batch, 100))
	}

	r.subscribers = append(r.subscribers, make(chan arma.Batch, 100))
	commit := r.PullBatches(0)

	batchers[0].Ledger = r
	for i := 1; i < n; i++ {
		batchers[i].BatchPuller = r
	}

	for i := 0; i < n; i++ {
		from := i
		batchers[i].TotalOrderBAF = func(arma.BatchAttestationFragment) {}
		batchers[i].AckBAF = func(seq uint64, to arma.PartyID) {
			batchers[to].HandleAck(seq, arma.PartyID(from))
		}
	}

	for i := 0; i < n; i++ {
		batchers[i].Start()
	}
	return batchers, commit
}

func createTestBatcher(t *testing.T, shardID int, nodeID int, batchers []arma.PartyID) *arma.Batcher {
	sugaredLogger := testutil.CreateLogger(t, nodeID)

	requestInspector := &reqInspector{}
	pool := request.NewPool(sugaredLogger, requestInspector, request.PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          10000,
		MaxSize:               1000 * 100,
		AutoRemoveTimeout:     time.Minute / 2,
		SubmitTimeout:         time.Second * 10,
	})

	b := &arma.Batcher{
		Batchers: batchers,
		Shard:    arma.ShardID(shardID),
		AttestBatch: func(seq uint64, primary arma.PartyID, shard arma.ShardID, digest []byte) arma.BatchAttestationFragment {
			return &arma_types.SimpleBatchAttestationFragment{
				Dig: digest,
				Sh:  shardID,
				Si:  nodeID,
				P:   int(primary),
				Se:  int(seq),
			}
		},
		Digest: func(data [][]byte) []byte {
			h := sha256.New()
			for _, d := range data {
				h.Write(d)
			}
			return h.Sum(nil)
		},
		RequestInspector: requestInspector,
		Logger:           sugaredLogger,
		MemPool:          pool,
		ID:               arma.PartyID(nodeID),
		Threshold:        2,
	}

	b.Ledger = &noopLedger{}

	return b
}
