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
	"github.com/stretchr/testify/require"
)

// requireMatchingBatchIDs asserts that Fetch returned exactly one id per
// request, and that each id is the RequestID of the request at the same index.
func requireMatchingBatchIDs(t *testing.T, requestID func([]byte) string, batch, ids []interface{}) {
	t.Helper()
	require.Len(t, ids, len(batch), "expected one id per request")
	for i := range batch {
		require.Equal(t, requestID(batch[i].([]byte)), ids[i].(string), "id at index %d does not match its request", i)
	}
}

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
	fetched, ids := bs.Fetch(ctx)
	assert.Len(t, fetched, 0)
	assert.Len(t, ids, 0)

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
		fetched, ids = bs.Fetch(ctx)
		assert.Len(t, fetched, int(max))
		assert.Len(t, ids, int(max))
		requireMatchingBatchIDs(t, requestInspector.RequestID, fetched, ids)
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

func TestBatchStoreRemoveRequests(t *testing.T) {
	max := uint32(10000)
	lenByte := uint32(8)
	var removed uint32

	sugaredLogger := testutil.CreateLogger(t, 0)

	bs := NewBatchStore(max, max*lenByte, func(string) {
		atomic.AddUint32(&removed, 1)
	}, sugaredLogger)

	requestInspector := &reqInspector{}

	// Insert n keys and record their ids.
	const n = 5000
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := make([]byte, lenByte)
		binary.BigEndian.PutUint32(key[4:], uint32(i))
		keyID := requestInspector.RequestID(key)
		require.True(t, bs.Insert(keyID, key, uint32(len(key))))
		keys = append(keys, keyID)
	}

	// Remove them all in parallel via the new fan-out API.
	bs.RemoveRequests(keys...)

	// onDelete must have fired exactly once per key, and every key must be gone.
	assert.Equal(t, n, int(atomic.LoadUint32(&removed)))
	for _, keyID := range keys {
		_, exists := bs.Lookup(keyID)
		assert.False(t, exists, "key %s should have been removed", keyID)
	}

	// Removing again (now-absent keys) plus keys that never existed must be a
	// no-op: LoadAndDelete misses, so onDelete does not fire again.
	bs.RemoveRequests(append(keys, "does-not-exist-1", "does-not-exist-2")...)
	assert.Equal(t, n, int(atomic.LoadUint32(&removed)))

	// An empty call must not panic.
	bs.RemoveRequests()
	assert.Equal(t, n, int(atomic.LoadUint32(&removed)))
}
