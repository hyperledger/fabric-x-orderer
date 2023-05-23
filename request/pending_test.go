package request

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"
)

func TestPending(t *testing.T) {
	sugaredLogger := createLogger(t, 0)
	requestInspector := &reqInspector{}

	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 100)

	ps := &PendingStore{
		ReqIDLifetime:   time.Second * 10,
		ReqIDGCInterval: time.Second,
		Logger:          sugaredLogger,
		SecondStrikeCallback: func() {

		},
		StartTime: start,
		Time:      ticker.C,
		FirstStrikeCallback: func([]byte) {

		},
		Epoch:                 time.Millisecond * 200,
		FirstStrikeThreshold:  time.Second * 10,
		Inspector:             requestInspector,
		Semaphore:             semaphore.NewWeighted(10000),
		SecondStrikeThreshold: time.Second,
	}

	ps.Init()
	ps.Restart()

	workerNum := runtime.NumCPU()
	workPerWorker := 100000

	var submittedCount uint32

	reqIDsSent := make(chan string, workerNum*workPerWorker)

	var wg sync.WaitGroup
	wg.Add(workerNum)

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workPerWorker; i++ {
				req := make([]byte, 8)
				binary.BigEndian.PutUint32(req, uint32(worker))
				binary.BigEndian.PutUint32(req[4:], uint32(i))

				reqID := requestInspector.RequestID(req)

				reqIDsSent <- reqID

				atomic.AddUint32(&submittedCount, 1)
				if err := ps.Submit(req, context.Background()); err != nil {
					panic(err)
				}
			}
		}(worker)
	}

	go func() {
		wg.Wait()
		close(reqIDsSent)
	}()

	virtualBlock := make([]string, 0, workerNum*workPerWorker)

	var end bool

	for !end {
		select {
		case reqID := <-reqIDsSent:
			end = reqID == ""
			virtualBlock = append(virtualBlock, reqID)
		default:
			ps.RemoveRequests(virtualBlock...)
			virtualBlock = make([]string, 0, workerNum*workPerWorker)
		}
	}

	assert.Equal(t, uint32(workerNum*workPerWorker), atomic.LoadUint32(&submittedCount))
	fmt.Println(len(ps.buckets))
}
