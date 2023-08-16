package arma

import (
	"arma/request"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestBatchBytes(t *testing.T) {
	var b BatchedRequests
	for i := 0; i < 10; i++ {
		req := make([]byte, 100)
		rand.Read(req)
		b = append(b, req)
	}

	b2 := BatchFromRaw(b.ToBytes())
	assert.Equal(t, b, b2)
}

type reqInspector struct {
}

func (ri *reqInspector) RequestID(req []byte) string {
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}

type noopLedger struct {
}

func (noopLedger) Append(_ uint16, _ uint64, _ []byte) {

}

type naiveReplication struct {
	subscribers []chan Batch
	i           uint32
}

func (r *naiveReplication) Replicate(_ uint16, _ uint64) <-chan Batch {
	j := atomic.AddUint32(&r.i, 1)
	return r.subscribers[j-1]
}

func (r *naiveReplication) Append(_ uint16, seq uint64, bytes []byte) {
	for _, s := range r.subscribers {
		s <- &naiveBatch{
			requests: BatchFromRaw(bytes),
		}
	}
}

type naiveBatch struct {
	node     uint16
	requests [][]byte
}

func (nb *naiveBatch) Party() uint16 {
	return nb.node
}

func (nb *naiveBatch) Digest() []byte {
	h := sha256.New()
	for _, req := range nb.Requests() {
		h.Write(req)
	}
	return h.Sum(nil)
}

func (nb *naiveBatch) Requests() BatchedRequests {
	return nb.requests
}

func TestBatcherNetwork(t *testing.T) {
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
	n := 3

	var stopped sync.WaitGroup
	stopped.Add(n)

	batchers, _ := createBatchers(t, n)
	for _, b := range batchers {
		b := b
		b.Primary = 99 // No one is primary
		pool := request.NewPool(b.Logger, b.RequestInspector, request.PoolOptions{
			FirstStrikeThreshold:  time.Second * 1,
			SecondStrikeThreshold: time.Second * 5,
			BatchMaxSize:          10000,
			MaxSize:               1000 * 100,
			AutoRemoveTimeout:     time.Minute / 2,
			SubmitTimeout:         time.Second * 10,
			OnFirstStrikeTimeout:  func([]byte) {},
			SecondStrikeCallback: func() {
				go func() {
					fmt.Println("Stopping batcher", b.ID)
					defer stopped.Done()
					b.Stop()
				}()
			},
		})
		b.MemPool.Stop()
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

	buf := make([]byte, 1024*10)
	runtime.Stack(buf, true)
	fmt.Println(string(buf))

}

func createBatchers(t *testing.T, n int) ([]*Batcher, <-chan Batch) {
	var batchers []*Batcher

	for i := 0; i < n; i++ {
		b := createBatcher(t, 0, i)
		batchers = append(batchers, b)
	}

	r := &naiveReplication{}

	for i := 1; i < n; i++ {
		r.subscribers = append(r.subscribers, make(chan Batch, 100))
	}

	r.subscribers = append(r.subscribers, make(chan Batch, 100))
	commit := r.Replicate(0, 0)

	batchers[0].Ledger = r
	for i := 1; i < n; i++ {
		batchers[i].Replicator = r
	}

	for i := 0; i < n; i++ {
		from := i
		batchers[i].Send = func(msg []byte) {
			for i := 0; i < n; i++ {
				batchers[i].HandleMessage(msg, uint16(from))
			}
		}
	}

	for i := 0; i < n; i++ {
		batchers[i].Run()
	}
	return batchers, commit
}

func createBatcher(t *testing.T, shardID int, nodeID int) *Batcher {
	sugaredLogger := createLogger(t, nodeID)

	requestInspector := &reqInspector{}
	pool := request.NewPool(sugaredLogger, requestInspector, request.PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          10000,
		MaxSize:               1000 * 100,
		AutoRemoveTimeout:     time.Minute / 2,
		SubmitTimeout:         time.Second * 10,
	})

	b := &Batcher{
		Shard: uint16(shardID),
		AttestationFromBytes: func(bytes []byte) (BatchAttestationFragment, error) {
			baf := &SimpleBatchAttestationFragment{}
			err := baf.Deserialize(bytes)
			return baf, err
		},
		AttestBatch: func(seq uint64, primary uint16, shard uint16, digest []byte) BatchAttestationFragment {
			return &SimpleBatchAttestationFragment{
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
		OnCollectedAttestation: func(BatchAttestationFragment) {},
		RequestInspector:       requestInspector,
		Logger:                 sugaredLogger,
		MemPool:                pool,
		ID:                     uint16(nodeID),
		Quorum:                 2,
		confirmedSequences:     make(map[uint64]map[uint16]struct{}),
		seq2digest:             make(map[uint64][]byte),
	}

	b.Ledger = &noopLedger{}

	return b
}

func createLogger(t *testing.T, i int) *zap.SugaredLogger {
	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zapcore.WarnLevel)
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("t", t.Name())).With(zap.Int64("id", int64(i)))
	sugaredLogger := logger.Sugar()
	return sugaredLogger
}
