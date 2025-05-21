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

	"github.ibm.com/decentralized-trust-research/arma/request"
	"github.ibm.com/decentralized-trust-research/arma/request/mocks"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

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
