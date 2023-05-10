package arma

import (
	"arma/request"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatchBytes(t *testing.T) {
	var b Batch
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

func (ri *reqInspector) RequestID(req []byte) types.RequestInfo {
	digest := sha256.Sum256(req)
	return types.RequestInfo{
		ID: hex.EncodeToString(digest[:]),
	}
}

type noopLedger struct {
}

func (noopLedger) Append(_ uint16, _ uint64, _ []byte) {

}

type replication struct {
	subscribers []chan []byte
	i           uint32
}

func (r *replication) Replicate(_ uint16, _ uint64) <-chan []byte {
	j := atomic.AddUint32(&r.i, 1)
	return r.subscribers[j-1]
}

func (r *replication) Append(_ uint16, seq uint64, bytes []byte) {
	t1 := time.Now()
	for _, s := range r.subscribers {
		s <- bytes
	}
	fmt.Println("Appended request to subscribers in", time.Since(t1))
}

func TestBatcherNetwork(t *testing.T) {
	n := 3

	var batchers []*Batcher

	for i := 0; i < n; i++ {

		logConfig := zap.NewDevelopmentConfig()
		logger, _ := logConfig.Build()
		logger = logger.With(zap.String("t", t.Name())).With(zap.Int64("id", int64(i)))
		sugaredLogger := logger.Sugar()

		requestInspector := &reqInspector{}
		pool := request.NewPool(sugaredLogger, requestInspector, request.PoolOptions{
			BatchMaxSize:      10000,
			MaxSize:           1000 * 100,
			AutoRemoveTimeout: time.Minute / 2,
			SubmitTimeout:     time.Second * 10,
		})

		b := &Batcher{
			RequestInspector: requestInspector,
			Logger:           sugaredLogger,
			memPool:          pool,
			ID:               uint16(i),
			Quorum:           2,
			Sign: func(uint64, []byte) []byte {
				return nil
			},
			confirmedSequences: make(map[uint64]map[uint16][]byte),
		}

		b.signal = sync.Cond{L: &b.lock}

		batchers = append(batchers, b)
	}

	r := &replication{}

	for i := 1; i < n; i++ {
		r.subscribers = append(r.subscribers, make(chan []byte, 100))
	}

	r.subscribers = append(r.subscribers, make(chan []byte, 100))
	commit := r.Replicate(0, 0)

	batchers[0].Ledger = r
	for i := 1; i < n; i++ {
		batchers[i].Ledger = &noopLedger{}
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
					batchers[0].Submit(req)
					time.Sleep(time.Millisecond)
				}
			}(uint64(worker))
		}
	}()

	var committedRequestCount uint32

	go func() {
		for {
			time.Sleep(time.Second * 5)
			fmt.Println(">>>>>>>>>>>", atomic.LoadUint32(&committedRequestCount)/5)
			atomic.StoreUint32(&committedRequestCount, 0)
		}
	}()

	var committedRequests sync.Map

	for {
		rawBatch := <-commit
		b := BatchFromRaw(rawBatch)
		atomic.AddUint32(&committedRequestCount, uint32(len(b)))
		for _, req := range b {
			_, loaded := committedRequests.LoadOrStore(string(req), struct{}{})
			if loaded {
				panic("request was delivered twice")
			}
		}
	}
}

func Stack() []byte {
	buf := make([]byte, 1024)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}
