/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package request

import (
	"context"
	"encoding/binary"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/assert"
)

func TestBatchStore(t *testing.T) {
	max := uint32(100)
	lenByte := uint32(8)
	var removed uint32

	sugaredLogger := testutil.CreateLogger(t, 0)

	bs := NewBatchStore(max, max*lenByte, func(string) {
		atomic.AddUint32(&removed, 1)
	}, sugaredLogger)
	assert.NotNil(t, bs)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fetched := bs.Fetch(ctx)
	assert.Len(t, fetched, 0)

	requestInspector := &reqInspector{}

	workerNum := runtime.NumCPU()
	workPerWorker := 1000

	loaded := make(chan string, workerNum*workPerWorker)

	var wg sync.WaitGroup
	wg.Add(workerNum)
	var inserted uint32

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workPerWorker; i++ {
				key := make([]byte, lenByte)
				binary.BigEndian.PutUint32(key, uint32(worker))
				binary.BigEndian.PutUint32(key[4:], uint32(i))
				keyID := requestInspector.RequestID(key)
				if bs.Insert(keyID, key, uint32(len(key))) {
					atomic.AddUint32(&inserted, 1)
				}
				loaded <- keyID
			}
		}(worker)
	}

	wg.Wait()
	close(loaded)

	assert.Equal(t, workerNum*workPerWorker, int(inserted))

	for i := 0; i < 10; i++ {
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fetched = bs.Fetch(ctx)
		assert.Len(t, fetched, int(max))
	}

	wg.Add(workerNum)

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workPerWorker; i++ {
				key := make([]byte, lenByte)
				binary.BigEndian.PutUint32(key, uint32(worker))
				binary.BigEndian.PutUint32(key[4:], uint32(i))
				keyID := requestInspector.RequestID(key)
				bs.Remove(keyID)
			}
		}(worker)
	}

	wg.Wait()

	assert.Equal(t, workerNum*workPerWorker, int(removed))
}
