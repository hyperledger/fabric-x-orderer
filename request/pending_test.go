/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package request_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/request"
	"github.com/hyperledger/fabric-x-orderer/request/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type reqInspector struct{}

func (ri *reqInspector) RequestID(req []byte) string {
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}

func TestPending(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)
	requestInspector := &reqInspector{}

	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 100)

	ps := &request.PendingStore{
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
		OnDelete:              func(key string) {},
		SecondStrikeThreshold: time.Second,
	}

	ps.Init()
	ps.Start()

	workerNum := runtime.NumCPU()
	workPerWorker := 1000

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
				ps.Submit(req)
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
}

func TestGetAll(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)
	requestInspector := &reqInspector{}

	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 100)

	ps := &request.PendingStore{
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
		OnDelete:              func(key string) {},
		SecondStrikeThreshold: time.Second,
	}

	ps.Init()
	ps.Start()

	count := 100

	for i := 0; i < count; i++ {
		req := make([]byte, 8)
		binary.BigEndian.PutUint64(req, uint64(i))
		if err := ps.Submit(req); err != nil {
			panic(err)
		}
	}

	ps.Close()
	all := ps.GetAllRequests(uint64(count))
	assert.Equal(t, count, len(all))
}

func TestBasicStrikes(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)
	requestInspector := &reqInspector{}

	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 100)

	striker := &mocks.FakeStriker{}

	ps := &request.PendingStore{
		ReqIDLifetime:   time.Second * 10,
		ReqIDGCInterval: time.Second,
		Logger:          sugaredLogger,
		StartTime:       start,
		Time:            ticker.C,
		Epoch:           time.Millisecond * 200,
		Inspector:       requestInspector,
		OnDelete:        func(key string) {},
		FirstStrikeCallback: func(b []byte) {
			striker.OnFirstStrikeTimeout(b)
		},
		FirstStrikeThreshold: 500 * time.Millisecond,
		SecondStrikeCallback: func() {
			striker.OnSecondStrikeTimeout()
		},
		SecondStrikeThreshold: time.Second,
	}

	ps.Init()
	ps.Start()

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	require.NoError(t, ps.Submit(req))

	require.Eventually(t, func() bool {
		return striker.OnFirstStrikeTimeoutCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return striker.OnSecondStrikeTimeoutCallCount() == 1
	}, 10*time.Second, 10*time.Millisecond)

	ps.Close()
	all := ps.GetAllRequests(10)
	require.Equal(t, 1, len(all))
}

func TestMultipleStopCalls(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)
	requestInspector := &reqInspector{}

	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	ps := &request.PendingStore{
		ReqIDLifetime:         time.Second * 10,
		ReqIDGCInterval:       time.Second,
		Logger:                sugaredLogger,
		SecondStrikeCallback:  func() {},
		StartTime:             start,
		Time:                  ticker.C,
		FirstStrikeCallback:   func([]byte) {},
		Epoch:                 time.Millisecond * 200,
		FirstStrikeThreshold:  time.Second * 10,
		Inspector:             requestInspector,
		OnDelete:              func(key string) {},
		SecondStrikeThreshold: time.Second,
	}

	// Check that Stop can be called before Start
	ps.Stop()

	ps.Init()
	ps.Start()

	// Submit a request
	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	require.NoError(t, ps.Submit(req))

	// Call Stop multiple times - should not panic or deadlock
	for i := 0; i < 5; i++ {
		ps.Stop()
	}

	// Verify the store is still functional after multiple stops
	ps.Close()
	all := ps.GetAllRequests(10)
	assert.Equal(t, 1, len(all))
}

func TestStopFollowedByReset(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)
	requestInspector := &reqInspector{}

	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	var firstStrikeCount atomic.Int32
	var secondStrikeCount atomic.Int32

	ps := &request.PendingStore{
		ReqIDLifetime:   time.Second * 10,
		ReqIDGCInterval: time.Second,
		Logger:          sugaredLogger,
		SecondStrikeCallback: func() {
			secondStrikeCount.Add(1)
		},
		StartTime: start,
		Time:      ticker.C,
		FirstStrikeCallback: func([]byte) {
			firstStrikeCount.Add(1)
		},
		Epoch:                 time.Millisecond * 200,
		FirstStrikeThreshold:  300 * time.Millisecond,
		Inspector:             requestInspector,
		OnDelete:              func(key string) {},
		SecondStrikeThreshold: 500 * time.Millisecond,
	}

	ps.Init()
	ps.Start()

	// Submit a request
	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	require.NoError(t, ps.Submit(req))

	// Wait for first strike to occur
	require.Eventually(t, func() bool {
		return firstStrikeCount.Load() == 1
	}, 5*time.Second, 50*time.Millisecond)

	// Stop the store
	ps.Stop()

	// Ensure that a second strike does not occur while the store is stopped.
	// Wait for longer than SecondStrikeThreshold + Epoch, polling for any second strike.
	assert.Never(t, func() bool {
		return secondStrikeCount.Load() > 0
	}, ps.SecondStrikeThreshold+ps.Epoch+50*time.Millisecond, 10*time.Millisecond, "Second strike should not occur while stopped")

	// Reset timestamps - this should resume the store
	ps.ResetTimestamps()

	// Verify the store is working again by submitting a new request
	req2 := make([]byte, 8)
	binary.BigEndian.PutUint64(req2, uint64(2))
	require.NoError(t, ps.Submit(req2))

	ps.Close()
	all := ps.GetAllRequests(10)
	assert.Equal(t, 2, len(all))
}

func TestConcurrentStopAndClose(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)
	requestInspector := &reqInspector{}

	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	ps := &request.PendingStore{
		ReqIDLifetime:         time.Second * 10,
		ReqIDGCInterval:       time.Second,
		Logger:                sugaredLogger,
		SecondStrikeCallback:  func() {},
		StartTime:             start,
		Time:                  ticker.C,
		FirstStrikeCallback:   func([]byte) {},
		Epoch:                 time.Millisecond * 200,
		FirstStrikeThreshold:  time.Second * 10,
		Inspector:             requestInspector,
		OnDelete:              func(key string) {},
		SecondStrikeThreshold: time.Second,
	}

	ps.Init()
	ps.Start()

	// Submit some requests
	for i := 0; i < 10; i++ {
		req := make([]byte, 8)
		binary.BigEndian.PutUint64(req, uint64(i))
		require.NoError(t, ps.Submit(req))
	}

	// Concurrently call Stop and Close - should not deadlock or panic
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			ps.Stop()
			time.Sleep(time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		ps.Close()
	}()

	// Wait with timeout to detect deadlocks
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - no deadlock
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out - possible deadlock")
	}

	// Verify store handled concurrent operations without panic
	// (if we got here, the test passed)
}

func TestStopPreventsEpochProcessing(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)
	requestInspector := &reqInspector{}

	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()

	var firstStrikeCount atomic.Int32

	ps := &request.PendingStore{
		ReqIDLifetime:   time.Second * 10,
		ReqIDGCInterval: time.Second,
		Logger:          sugaredLogger,
		SecondStrikeCallback: func() {
			t.Error("Second strike should not be called while stopped")
		},
		StartTime: start,
		Time:      ticker.C,
		FirstStrikeCallback: func([]byte) {
			firstStrikeCount.Add(1)
		},
		Epoch:                 time.Millisecond * 100,
		FirstStrikeThreshold:  200 * time.Millisecond,
		Inspector:             requestInspector,
		OnDelete:              func(key string) {},
		SecondStrikeThreshold: 400 * time.Millisecond,
	}

	ps.Init()
	ps.Start()

	// Submit a request
	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	require.NoError(t, ps.Submit(req))

	// Stop immediately
	ps.Stop()

	// Wait longer than first strike threshold
	time.Sleep(500 * time.Millisecond)

	// First strike should not have occurred because store is stopped
	assert.Equal(t, int32(0), firstStrikeCount.Load(), "First strike should not occur while stopped")

	ps.Close()
}

func TestStopAfterClose(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)
	requestInspector := &reqInspector{}

	start := time.Now()
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	ps := &request.PendingStore{
		ReqIDLifetime:         time.Second * 10,
		ReqIDGCInterval:       time.Second,
		Logger:                sugaredLogger,
		SecondStrikeCallback:  func() {},
		StartTime:             start,
		Time:                  ticker.C,
		FirstStrikeCallback:   func([]byte) {},
		Epoch:                 time.Millisecond * 200,
		FirstStrikeThreshold:  time.Second * 10,
		Inspector:             requestInspector,
		OnDelete:              func(key string) {},
		SecondStrikeThreshold: time.Second,
	}

	ps.Init()
	ps.Start()

	// Close the store
	ps.Close()

	// Calling Stop after Close should not panic or block
	ps.Stop()

	// Multiple stops after close should also be safe
	for i := 0; i < 3; i++ {
		ps.Stop()
	}

	// If we got here without panic or deadlock, test passed
}
