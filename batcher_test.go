package arma

import (
	"arma/request"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

	var batchers []*Batcher

	for i := 0; i < n; i++ {
		b := createBatcher(t, i)
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
		batchers[i].Send = func(to uint16, msg []byte) {
			batchers[to].HandleMessage(msg, uint16(from))
		}
	}

	for i := 0; i < n; i++ {
		batchers[i].run()
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
			if tps > 60*1000 {
				atomic.AddUint64(&fastBatchesCommitted, 1)
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

func createBatcher(t *testing.T, i int) *Batcher {
	sugaredLogger := createLogger(t, i)

	requestInspector := &reqInspector{}
	pool := request.NewPool(sugaredLogger, requestInspector, request.PoolOptions{
		BatchMaxSize:      10000,
		MaxSize:           1000 * 100,
		AutoRemoveTimeout: time.Minute / 2,
		SubmitTimeout:     time.Second * 10,
	})

	b := &Batcher{
		Digest: func(data [][]byte) []byte {
			h := sha256.New()
			for _, d := range data {
				h.Write(d)
			}
			return h.Sum(nil)
		},
		OnCollectAttestations: func(uint642 uint64, _ []byte, m map[uint16][]byte) {},
		RequestInspector:      requestInspector,
		Logger:                sugaredLogger,
		memPool:               pool,
		ID:                    uint16(i),
		Quorum:                2,
		Sign: func(uint64, []byte) []byte {
			return nil
		},
		confirmedSequences: make(map[uint64]map[uint16][]byte),
		seq2digest:         make(map[uint64][]byte),
	}

	b.Ledger = &noopLedger{}

	b.signal = sync.Cond{L: &b.lock}
	return b
}

func createLogger(t *testing.T, i int) *zap.SugaredLogger {
	logConfig := zap.NewDevelopmentConfig()
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("t", t.Name())).With(zap.Int64("id", int64(i)))
	sugaredLogger := logger.Sugar()
	return sugaredLogger
}
