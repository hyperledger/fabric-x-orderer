/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package request

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/assert"
)

type reqInspector struct{}

func (ri *reqInspector) RequestID(req []byte) string {
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}

type striker struct{}

func (s *striker) OnFirstStrikeTimeout([]byte) {
	panic("timed out on request")
}

func (s *striker) OnSecondStrikeTimeout() {
	panic("timed out on request")
}

func BenchmarkRequestPool(b *testing.B) {
	sugaredLogger := testutil.CreateBenchmarkLogger(b, 0)

	requestInspector := &reqInspector{}

	primaryPool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          10000,
		BatchMaxSizeBytes:     10000 * 32,
		MaxSize:               1000 * 100,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
	}, &striker{})

	secondaryPool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          10000,
		BatchMaxSizeBytes:     10000 * 32,
		MaxSize:               1000 * 100,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
	}, &striker{})

	primaryPool.Restart(true)
	secondaryPool.Restart(false)

	var submittedCount uint32
	var committedReqCount int

	workerNum := runtime.NumCPU()
	workerPerWorker := 100000

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			for i := 0; i < workerPerWorker; i++ {
				req := make([]byte, 8)
				binary.BigEndian.PutUint32(req, uint32(worker))
				binary.BigEndian.PutUint32(req[4:], uint32(i))
				atomic.AddUint32(&submittedCount, 1)

				if err := secondaryPool.Submit(req); err != nil {
					panic(err)
				}

				if err := primaryPool.Submit(req); err != nil {
					panic(err)
				}
			}
		}(worker)
	}

	for committedReqCount < workerPerWorker*workerNum {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		batch := primaryPool.NextRequests(ctx)
		cancel()
		committedReqCount += len(batch)

		workerNum := runtime.NumCPU()

		removeRequests(workerNum, batch, requestInspector, primaryPool)
		removeRequests(workerNum, batch, requestInspector, secondaryPool)

	}
}

func removeRequests(workerNum int, batch [][]byte, requestInspector *reqInspector, pool *Pool) {
	var wg sync.WaitGroup
	wg.Add(workerNum)

	for workerID := 0; workerID < workerNum; workerID++ {
		go func(workerID int) {
			defer wg.Done()
			reqInfos := make([]string, 0, len(batch))
			for i, req := range batch {
				if i%workerNum != workerID {
					continue
				}
				reqInfos = append(reqInfos, requestInspector.RequestID(req))
			}

			pool.RemoveRequests(reqInfos...)
		}(workerID)
	}

	wg.Wait()
}

func TestRestartPool(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)

	requestInspector := &reqInspector{}

	pool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          10,
		BatchMaxSizeBytes:     10 * 32,
		MaxSize:               1000,
		RequestMaxBytes:       100 * 1024,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
	}, &striker{})

	assert.Equal(t, 5*time.Second, pool.pending.FirstStrikeThreshold)

	pool.Restart(true)

	count := 100

	for i := 0; i < count; i++ {
		req := make([]byte, 8)
		binary.BigEndian.PutUint64(req, uint64(i))
		if err := pool.Submit(req); err != nil {
			panic(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	batch1 := pool.NextRequests(ctx)

	assert.Equal(t, 10, len(batch1))

	batch2 := pool.NextRequests(ctx)

	assert.Equal(t, 10, len(batch2))

	pool.Restart(true)

	batch3 := pool.NextRequests(ctx)

	assert.Equal(t, 10, len(batch3))

	pool.Restart(false)

	assert.Nil(t, pool.NextRequests(ctx))

	pool.Restart(false)

	assert.Nil(t, pool.NextRequests(ctx))

	pool.Restart(true)

	batch4 := pool.NextRequests(ctx)

	assert.Equal(t, 10, len(batch4))

	batch5 := pool.NextRequests(ctx)

	assert.Equal(t, 10, len(batch5))
}

func TestBasicBatching(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)

	byteReq1 := makeTestRequest("1", "foo")
	byteReq2 := makeTestRequest("2", "foo-bar")
	byteReq3 := makeTestRequest("3", "foo-bar-foo")
	byteReq4 := makeTestRequest("4", "foo-bar-foo-bar")
	byteReq5 := makeTestRequest("5", "foo-bar-foo-bar-foo")

	pool := NewPool(sugaredLogger, &testRequestInspector{}, PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          1,
		BatchMaxSizeBytes:     2048,
		MaxSize:               3,
		RequestMaxBytes:       100 * 1024,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
	}, &striker{})

	assert.Equal(t, 5*time.Second, pool.pending.FirstStrikeThreshold)

	pool.Restart(true)

	ctx, cancel1 := context.WithTimeout(context.Background(), time.Second)
	defer cancel1()

	assert.NoError(t, pool.Submit(byteReq1))
	assert.Error(t, pool.Submit(byteReq1))
	assert.Len(t, pool.NextRequests(ctx), 1)

	pool.RemoveRequests("1")
	assert.Len(t, pool.NextRequests(ctx), 0) // after timeout

	assert.NoError(t, pool.Submit(byteReq2))
	assert.NoError(t, pool.Submit(byteReq3))

	ctx, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()

	res := pool.NextRequests(ctx)
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq2, res[0])

	res = pool.NextRequests(ctx)
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq3, res[0])

	pool.Close()

	// change count limit

	pool = NewPool(sugaredLogger, &testRequestInspector{}, PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          2,
		BatchMaxSizeBytes:     2048,
		MaxSize:               3,
		RequestMaxBytes:       100 * 1024,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
		BatchTimeout:          time.Second,
	}, &striker{})

	t.Logf("First strike with random is %f seconds\n", pool.pending.FirstStrikeThreshold.Seconds())
	assert.GreaterOrEqual(t, 7*time.Second, pool.pending.FirstStrikeThreshold)
	assert.LessOrEqual(t, 3*time.Second, pool.pending.FirstStrikeThreshold)

	pool.Restart(true)

	assert.NoError(t, pool.Submit(byteReq4))
	assert.NoError(t, pool.Submit(byteReq5))

	res = pool.NextRequests(ctx)
	assert.Len(t, res, 2)

	pool.Close()

	// change size limit

	pool = NewPool(sugaredLogger, &testRequestInspector{}, PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          3,
		BatchMaxSizeBytes:     uint32(len(byteReq3) + len(byteReq4)),
		MaxSize:               3,
		RequestMaxBytes:       100 * 1024,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
	}, &striker{})

	pool.Restart(true)

	assert.NoError(t, pool.Submit(byteReq3))
	assert.NoError(t, pool.Submit(byteReq4))
	assert.NoError(t, pool.Submit(byteReq5))

	res = pool.NextRequests(ctx)
	assert.Len(t, res, 2)

	pool.Close()
}

func TestBasicBatchingWhileSubmitting(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)

	pool := NewPool(sugaredLogger, &testRequestInspector{}, PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          100,
		BatchMaxSizeBytes:     5000,
		MaxSize:               300,
		RequestMaxBytes:       100 * 1024,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
	}, &striker{})

	pool.Restart(true)

	submitted := 300

	go func() {
		for i := 0; i < submitted; i++ {
			iStr := fmt.Sprintf("%d", i)
			byteReq := makeTestRequest(iStr, "foo")
			err := pool.Submit(byteReq)
			assert.NoError(t, err)
		}
	}()

	requests := 0

	timer := time.NewTimer(30 * time.Second)
	for requests < submitted {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		res := pool.NextRequests(ctx)
		requests += len(res)
		cancel()
		select {
		case <-timer.C:
			t.Logf("timeout")
			t.Fail()
		default:
		}
	}

	pool.Close()
}

func TestBasicBatchingTimeout(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)

	pool := NewPool(sugaredLogger, &testRequestInspector{}, PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          100,
		BatchMaxSizeBytes:     5000,
		MaxSize:               200,
		RequestMaxBytes:       100 * 1024,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
	}, &striker{})

	pool.Restart(true)

	byteReq := makeTestRequest("1", "foo")
	assert.NoError(t, pool.Submit(byteReq))
	assert.Error(t, pool.Submit(byteReq))

	go func() {
		pool.Close()
	}()

	t1 := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pool.NextRequests(ctx)
	assert.True(t, time.Since(t1) < 5*time.Second)
}

func TestBasicPrune(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)
	insp := &testRequestInspector{}
	pool := NewPool(sugaredLogger, insp, PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          10,
		BatchMaxSizeBytes:     1000,
		MaxSize:               10,
		RequestMaxBytes:       100 * 1024,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
	}, &striker{})

	pool.Restart(true)

	for i := 0; i < 5; i++ {
		iStr := fmt.Sprintf("%d", i)
		byteReq := makeTestRequest(iStr, "foo")
		err := pool.Submit(byteReq)
		assert.NoError(t, err)
	}

	pool.Prune(func(req []byte) error {
		ID := insp.RequestID(req)
		if ID == "3" {
			return errors.New("remove")
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res := pool.NextRequests(ctx)
	assert.Len(t, res, 4)

	pool.Restart(false)

	for i := 5; i < 10; i++ {
		iStr := fmt.Sprintf("%d", i)
		byteReq := makeTestRequest(iStr, "foo")
		err := pool.Submit(byteReq)
		assert.NoError(t, err)
	}

	pool.Prune(func(req []byte) error {
		ID := insp.RequestID(req)
		if ID == "1" || ID == "2" || ID == "7" || ID == "8" || ID == "9" {
			return errors.New("remove")
		}
		return nil
	})

	pool.Restart(true)

	res = pool.NextRequests(ctx)
	assert.Len(t, res, 2)
}

func makeTestRequest(txID, data string) []byte {
	buffLen := make([]byte, 4)
	buff := make([]byte, 12)

	binary.LittleEndian.PutUint32(buffLen, uint32(len(txID)))
	buff = append(buff[0:0], buffLen...)
	buff = append(buff, []byte(txID)...)

	binary.LittleEndian.PutUint32(buffLen, uint32(len(data)))
	buff = append(buff, buffLen...)
	buff = append(buff, []byte(data)...)

	return buff
}

func parseTestRequest(request []byte) (txID, data string) {
	l := binary.LittleEndian.Uint32(request)
	buff := request[4:]

	txID = string(buff[:l])
	buff = buff[l:]

	l = binary.LittleEndian.Uint32(buff)
	buff = buff[4:]

	data = string(buff[:l])

	return txID, data
}

type testRequestInspector struct{}

func (ins *testRequestInspector) RequestID(req []byte) string {
	ID, _ := parseTestRequest(req)
	return ID
}
