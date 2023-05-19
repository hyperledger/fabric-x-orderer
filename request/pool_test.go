package request

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type reqInspector struct {
}

func (ri *reqInspector) RequestID(req []byte) string {
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}

func TestRequestPool(t *testing.T) {
	sugaredLogger := createLogger(t, 0)

	requestInspector := &reqInspector{}

	pool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		BatchMaxSize:      10000,
		MaxSize:           1000 * 100,
		AutoRemoveTimeout: time.Minute / 2,
		SubmitTimeout:     time.Second * 10,
	})

	var submittedCount uint32
	var committedReqCount int

	workerNum := runtime.NumCPU()
	workerPerWorker := 100000

	var wg sync.WaitGroup
	wg.Add(workerNum)

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workerPerWorker; i++ {
				req := make([]byte, 4)
				binary.BigEndian.PutUint16(req, uint16(worker))
				binary.BigEndian.PutUint16(req[2:], uint16(i))
				atomic.AddUint32(&submittedCount, 1)
				pool.Submit(req)
			}
		}(worker)
	}

	for {
		batch := pool.NextRequests(context.Background())
		committedReqCount += len(batch)
		fmt.Println(committedReqCount, workerPerWorker*workerNum)
	}
}

func createLogger(t *testing.T, i int) *zap.SugaredLogger {
	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zapcore.WarnLevel)
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("t", t.Name())).With(zap.Int64("id", int64(i)))
	sugaredLogger := logger.Sugar()
	return sugaredLogger
}
