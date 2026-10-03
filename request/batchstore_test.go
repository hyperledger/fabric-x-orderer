/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package request

import (
	"context"
	"encoding/binary"
	"errors"
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

// TestFetchCancelWhileWaiting is a regression test for a reported deadlock. A
// batcher stop cancels the context of an in-flight Fetch that is parked in
// signal.Wait() with no batch ready. The cancellation is the only thing that can
// wake that parked Fetch; if nothing does, Fetch stays parked forever inside
// Pool.NextRequests — which holds the pool's RLock — deadlocking Pool.Close,
// which blocks on the pool's Lock.
//
// Each iteration parks a Fetch on an empty store under a live context, gives it a
// moment to reach signal.Wait(), then cancels; Fetch must be woken by the
// cancellation and return an empty batch promptly. Removing the wakeup
// (context.AfterFunc) makes this deadlock on the watchdog, which the earlier
// pre-canceled variant did not catch because an already-done context
// short-circuits the wait loop before Fetch ever parks.
//
// Note: the wakeup being delivered under bs.lock — which prevents a lost wakeup
// should cancellation ever race Fetch's parking — is correct by construction with
// AfterFunc (it schedules its func only after cancel, and the check→park window
// is too narrow for that func to interleave), so that specific property is not
// exercised here; this test guards that a parked Fetch is woken at all.
func TestFetchCancelWhileWaiting(t *testing.T) {
	sugaredLogger := testutil.CreateLogger(t, 0)

	// The wake path is deterministic once Fetch is parked, so a modest count with
	// a comfortable watchdog suffices; the extra iterations only guard against rare
	// scheduling where Fetch had not parked yet when cancel fired.
	const iterations = 500
	// A correct canceled Fetch returns in microseconds; a regressed Fetch never
	// returns, so a stuck iteration waits out this watchdog before t.Fatalf ends
	// the test (the parked Fetch is then torn down with the process). The bound
	// only affects how long a genuine regression takes to report.
	const watchdog = 10 * time.Second

	for i := 0; i < iterations; i++ {
		bs := NewBatchStore(100, 100*8, func(string) {}, sugaredLogger)

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})
		go func() {
			// Empty store + live context => Fetch parks in signal.Wait() until the
			// cancellation below wakes it; it must then return an empty batch.
			batch, ids := bs.Fetch(ctx)
			assert.Empty(t, batch, "canceled Fetch must return an empty batch")
			assert.Empty(t, ids, "canceled Fetch must return no ids")
			close(done)
		}()

		time.Sleep(time.Millisecond) // let Fetch reach signal.Wait()
		cancel()                     // the cancellation must wake the parked Fetch

		select {
		case <-done:
		case <-time.After(watchdog):
			t.Fatalf("Fetch deadlocked when its context was canceled while parked (iteration %d)", i)
		}
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

// TestBatchStorePrune is a regression test for the following issue: batch.Prune only
// deleted matching keys from the batch's own map, without routing them through
// the same cleanup as Remove. As a result pruned ids stayed in keys2Batches (so
// re-submissions were rejected as duplicates) and onDelete never fired (so the
// pool's capacity/semaphore permits leaked).
//
// The predicate signals "prune this key" by returning a non-nil error.
func TestBatchStorePrune(t *testing.T) {
	const batchMaxSize = uint32(100)
	const lenByte = uint32(8)
	var removed uint32

	sugaredLogger := testutil.CreateLogger(t, 0)

	bs := NewBatchStore(batchMaxSize, 1<<30, func(string) {
		atomic.AddUint32(&removed, 1)
	}, sugaredLogger)

	requestInspector := &reqInspector{}

	makeKey := func(i int) []byte {
		key := make([]byte, lenByte)
		binary.BigEndian.PutUint32(key[4:], uint32(i))
		return key
	}

	// Insert enough keys to rotate the current batch into several readyBatches, so
	// the prune below covers the multi-batch path rather than a single currentBatch.
	const n = 300
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := makeKey(i)
		keyID := requestInspector.RequestID(key)
		require.True(t, bs.Insert(keyID, key, uint32(len(key))))
		ids = append(ids, keyID)
	}
	require.Greater(t, len(bs.readyBatches), 1, "expected keys to span several rotated batches")

	// Prune every even-indexed key.
	pruned := make(map[string]bool)
	for i := 0; i < n; i += 2 {
		pruned[ids[i]] = true
	}
	bs.Prune(func(k, _ interface{}) error {
		key, _ := k.(string)
		if pruned[key] {
			return errors.New("prune")
		}
		return nil
	})

	// onDelete must have fired exactly once per pruned key (reclaimed capacity).
	assert.Equal(t, len(pruned), int(atomic.LoadUint32(&removed)))

	// Pruned keys must be gone and unpruned keys must remain.
	for i := 0; i < n; i++ {
		_, exists := bs.Lookup(ids[i])
		if pruned[ids[i]] {
			assert.False(t, exists, "pruned key %s should be gone", ids[i])
		} else {
			assert.True(t, exists, "unpruned key %s should remain", ids[i])
		}
	}

	// Lookup only consults keys2Batches, so it cannot catch a request still sitting
	// in a batch map that Fetch would propose. Check the batch maps directly: no
	// pruned id may survive in readyBatches or currentBatch.
	inBatchMaps := func(id string) bool {
		for _, b := range bs.readyBatches {
			if _, ok := b.Load(id); ok {
				return true
			}
		}
		_, ok := bs.currentBatch.Load(id)
		return ok
	}
	for i := 0; i < n; i += 2 {
		assert.False(t, inBatchMaps(ids[i]), "pruned key %s still present in a batch map", ids[i])
	}

	// A pruned key must no longer be treated as a duplicate: re-inserting it must
	// succeed (its keys2Batches entry was cleared).
	for i := 0; i < n; i += 2 {
		key := makeKey(i)
		assert.True(t, bs.Insert(ids[i], key, uint32(len(key))), "pruned key %s should be re-insertable", ids[i])
	}
}

// TestBatchStorePruneConcurrentInsert is a regression test for a race in Prune.
// Prune routes each removal through Remove, which is not synchronized by bs.lock
// and works in two steps (keys2Batches.LoadAndDelete, then the batch delete). While
// Prune held only a read lock, a concurrent Insert could interleave between those
// two steps: its LoadOrStore re-created the index entry, Remove then cleared the
// batch map, and the key was left present in keys2Batches but absent from every
// batch -- so Fetch never proposed it, onDelete leaked a permit, and a later Insert
// of the same id was rejected as a duplicate.
//
// The fix is to take the write lock in Prune so it serializes against Insert. This
// test fails on the read-lock version (a dangling index entry surfaces as a key
// that is absent from the store yet rejected as a duplicate) and passes once Prune
// holds the write lock.
func TestBatchStorePruneConcurrentInsert(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	insp := &reqInspector{}

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		bs := NewBatchStore(100, 1<<30, func(string) {}, logger)

		key := make([]byte, 8)
		binary.BigEndian.PutUint32(key[4:], uint32(i))
		id := insp.RequestID(key)

		// Pre-populate K so the concurrent re-Insert below collides with Prune's
		// Remove of the same key -- the only way the two-step Remove can interleave.
		require.True(t, bs.Insert(id, key, uint32(len(key))))

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			bs.Prune(func(interface{}, interface{}) error {
				return errors.New("prune")
			})
		}()
		go func() {
			defer wg.Done()
			bs.Insert(id, key, uint32(len(key)))
		}()
		wg.Wait()

		// If the key is absent from the store it must be cleanly absent: a dangling
		// keys2Batches entry shows up as Lookup==false while a re-Insert is still
		// rejected as a duplicate.
		if _, present := bs.Lookup(id); !present {
			require.True(t, bs.Insert(id, key, uint32(len(key))),
				"key absent from store but rejected as duplicate: dangling keys2Batches entry (iteration %d)", i)
		}
	}
}

// TestBatchStorePruneConcurrentRemove asserts onDelete fires exactly once per key
// when a Prune races RemoveRequests over the same keys. Prune holds the write lock
// but Remove is unsynchronized, so the two removals still run concurrently; Remove's
// LoadAndDelete makes exactly one of them win each key and fire onDelete.
func TestBatchStorePruneConcurrentRemove(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	insp := &reqInspector{}

	const iterations = 2000
	const n = 50
	for it := 0; it < iterations; it++ {
		var mu sync.Mutex
		counts := make(map[string]int)
		bs := NewBatchStore(1000, 1<<30, func(k string) {
			mu.Lock()
			counts[k]++
			mu.Unlock()
		}, logger)

		ids := make([]string, 0, n)
		for i := 0; i < n; i++ {
			key := make([]byte, 8)
			binary.BigEndian.PutUint32(key[0:], uint32(it))
			binary.BigEndian.PutUint32(key[4:], uint32(i))
			id := insp.RequestID(key)
			require.True(t, bs.Insert(id, key, uint32(len(key))))
			ids = append(ids, id)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			bs.Prune(func(interface{}, interface{}) error { return errors.New("prune") })
		}()
		go func() {
			defer wg.Done()
			bs.RemoveRequests(ids...)
		}()
		wg.Wait()

		for _, id := range ids {
			mu.Lock()
			c := counts[id]
			mu.Unlock()
			require.Equal(t, 1, c, "onDelete for %s fired %d times (iteration %d)", id, c, it)
		}
	}
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
