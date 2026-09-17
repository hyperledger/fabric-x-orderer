/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

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
	// Size the store so inserting n keys rotates the current batch into many
	// distinct readyBatches (a batch fills at batchMaxSize keys; batchMaxSizeBytes
	// is set large so the count limit is what drives rotation). This spreads the
	// keys across several *batch instances, exercising the multi-batch concurrent
	// removal path rather than removing everything from a single currentBatch.
	const batchMaxSize = uint32(100)
	lenByte := uint32(8)
	var removed uint32

	sugaredLogger := testutil.CreateLogger(t, 0)

	bs := NewBatchStore(batchMaxSize, 1<<30, func(string) {
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

	// Sanity-check that the keys really did spread across multiple batches, so
	// the removal below genuinely covers the multi-batch path.
	require.Greater(t, len(bs.readyBatches), 1, "expected keys to span several rotated batches")

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

// BenchmarkBatchStoreRemoveRequests compares the shipped parallel fan-out
// (RemoveRequests) against a plain serial Remove loop across representative
// batch sizes, documenting the tradeoff the fan-out makes on the commit hot path.
//
// Observed behaviour (results are machine/NumCPU-dependent): fan-out wins on
// large, near-max batches (the common full-batch commit, MaxMessageCount defaults
// to 10000) and loses on small ones, with the crossover around ~1000 keys. Below
// runtime.NumCPU() keys RemoveRequests falls back to a serial pass; between that
// and the crossover the goroutine-spawn/join overhead makes fan-out slower than
// the serial loop, but only by microseconds in absolute terms. Run with:
//
//	go test -run '^$' -bench BenchmarkBatchStoreRemoveRequests ./request/...
func BenchmarkBatchStoreRemoveRequests(b *testing.B) {
	const lenByte = uint32(8)
	requestInspector := &reqInspector{}
	logger := testutil.CreateBenchmarkLogger(b, 0)

	// populate returns a fresh store pre-loaded with size keys plus their ids.
	// Removal is destructive, so each iteration repopulates; that setup is kept
	// out of the timed region via Stop/StartTimer. batchMaxSize is large so no
	// rotation happens and the removal cost, not batching, is what is measured.
	populate := func(size int) (*BatchStore, []string) {
		bs := NewBatchStore(1<<20, 1<<30, func(string) {}, logger)
		keys := make([]string, 0, size)
		for i := 0; i < size; i++ {
			key := make([]byte, lenByte)
			binary.BigEndian.PutUint32(key[4:], uint32(i))
			keyID := requestInspector.RequestID(key)
			bs.Insert(keyID, key, lenByte)
			keys = append(keys, keyID)
		}
		return bs, keys
	}

	for _, size := range []int{1, 10, 100, 1000, 5000} {
		b.Run(fmt.Sprintf("fanout/size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				bs, keys := populate(size)
				b.StartTimer()
				bs.RemoveRequests(keys...)
			}
		})

		b.Run(fmt.Sprintf("serial/size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				bs, keys := populate(size)
				b.StartTimer()
				for _, key := range keys {
					bs.Remove(key)
				}
			}
		})
	}
}
