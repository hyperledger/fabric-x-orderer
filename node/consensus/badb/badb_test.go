/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package badb

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchAttestationDB(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	assert.NoError(t, err)

	defer os.RemoveAll(dir)

	logger := testutil.CreateLogger(t, 0)
	db, err := NewBatchAttestationDB(dir, logger)
	assert.NoError(t, err)

	// Insert test data, indexed over three decisions
	ids := []types.BatchID{
		state.NewAvailableBatch(1, 0, 0, []byte{1}),
		state.NewAvailableBatch(1, 0, 1, []byte{2}),
		state.NewAvailableBatch(2, 0, 0, []byte{3}),
	}
	for i, id := range ids {
		db.Put([]types.BatchID{id}, types.DecisionNum(i+1))
	}

	// Test that each digest exists in the DB
	for _, id := range ids {
		assert.True(t, db.Exists(id.Digest()))
	}

	// Test retrieval of digests and their decisions from the DB
	storedDigests, storedDecisions := db.List()
	assert.Len(t, storedDigests, len(ids))
	assert.Len(t, storedDecisions, len(ids))

	for i, id := range ids {
		assert.Contains(t, storedDigests, id.Digest())
		assert.Contains(t, storedDecisions, uint64(i+1))
	}

	// Test cleaning of the DB: digests indexed below decision 3 are removed
	db.Clean(3)
	assert.False(t, db.Exists(ids[0].Digest()))
	assert.False(t, db.Exists(ids[1].Digest()))
	assert.True(t, db.Exists(ids[2].Digest()))

	storedDigests, storedDecisions = db.List()
	assert.Len(t, storedDigests, 1)
	assert.Len(t, storedDecisions, 1)

	// Ensure that the DB cannot be accessed after closing
	db.Close()
	assert.False(t, db.Exists(ids[2].Digest()))
}

// TestPutWritesBatchIDKey verifies that Put indexes every batch by <shard, primary, seq, digest>, with the
// decision that committed it as the value.
func TestPutWritesBatchIDKey(t *testing.T) {
	db, err := NewBatchAttestationDB(t.TempDir(), testutil.CreateLogger(t, 0))
	require.NoError(t, err)
	defer db.Close()

	ids := []types.BatchID{
		state.NewAvailableBatch(1, 0, 7, []byte{1}),
		state.NewAvailableBatch(3, 2, 7, []byte{2}),
		// an equivocating primary: a second digest for <shard 0, primary 1, seq 7>
		state.NewAvailableBatch(1, 0, 7, []byte{3}),
	}
	db.Put(ids, 5)

	for _, id := range ids {
		value, closer, err := db.db.Get(makeBatchIDKey(id))
		require.NoError(t, err)
		assert.Equal(t, uint64(5), binary.BigEndian.Uint64(value))
		require.NoError(t, closer.Close())
	}

	// <shard, primary, seq> of the first batch with a digest that was never indexed
	_, _, err = db.db.Get(makeBatchIDKey(state.NewAvailableBatch(1, 0, 7, []byte{4})))
	assert.ErrorIs(t, err, pebble.ErrNotFound)
}

// TestBatchIDKeyLayout pins the bytes of a batch ID key, which readers of the index depend on: prefix 2,
// then the shard and the primary as 2 big-endian bytes each, the seq as 8 big-endian bytes, then the digest.
func TestBatchIDKeyLayout(t *testing.T) {
	id := state.NewAvailableBatch(0x0304, 0x0102, 0x05060708090a0b0c, []byte{0xab, 0xcd})
	want := []byte{0x02, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0xab, 0xcd}
	assert.Equal(t, want, makeBatchIDKey(id))
}

// TestBatchIDKeyOrder verifies that batch ID keys sort by shard, then primary, then seq, so that the
// batches of one <shard, primary> are contiguous and in sequence order.
func TestBatchIDKeyOrder(t *testing.T) {
	// batchID builds a batch ID with its fields in key order; NewAvailableBatch takes the primary first.
	batchID := func(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, digest byte) types.BatchID {
		return state.NewAvailableBatch(primary, shard, seq, []byte{digest})
	}
	ordered := []types.BatchID{
		batchID(0, 2, math.MaxUint64, 9),
		batchID(0, 256, 0, 0),
		batchID(1, 1, 0, 9),
		batchID(1, 1, 1, 0),
		batchID(1, 1, 256, 0),
		batchID(1, 1, 1<<56, 0),
		batchID(1, math.MaxUint16, 0, 0),
		batchID(2, 0, 0, 0),
		batchID(256, 0, 0, 0),
		batchID(math.MaxUint16, 0, 0, 0),
	}
	for i := 1; i < len(ordered); i++ {
		assert.Negative(t, bytes.Compare(makeBatchIDKey(ordered[i-1]), makeBatchIDKey(ordered[i])),
			"key %d should sort before key %d", i-1, i)
	}
}

// Scenario:
//  1. Store a digest, close the database and reopen it.
//  2. Expect the digest to exist and to be listed with its epoch.
func TestReopenKeepsDigests(t *testing.T) {
	dir := t.TempDir()
	logger := testutil.CreateLogger(t, 0)

	db, err := NewBatchAttestationDB(dir, logger)
	require.NoError(t, err)
	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{5})}, 9)
	db.Close()

	db, err = NewBatchAttestationDB(dir, logger)
	require.NoError(t, err)
	defer db.Close()

	assert.True(t, db.Exists([]byte{5}))
	digests, epochs := db.List()
	assert.Equal(t, [][]byte{{5}}, digests)
	assert.Equal(t, []uint64{9}, epochs)
}

// TestExistsNotFound verifies that Exists returns false (and does not panic)
// for a digest that was never stored, i.e. pebble.ErrNotFound is treated as absence.
func TestExistsNotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := testutil.CreateLogger(t, 0)
	db, err := NewBatchAttestationDB(dir, logger)
	require.NoError(t, err)
	defer db.Close()

	assert.False(t, db.Exists([]byte{9, 9, 9}))
}

// TestExistsAfterClose verifies that Exists treats a closed DB as benign
// and returns false. The consenter's SoftStop path closes the BADB while the
// BFT engine is still running and may drive Exists via SimulateStateTransition.
func TestExistsAfterClose(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := testutil.CreateLogger(t, 0)
	db, err := NewBatchAttestationDB(dir, logger)
	require.NoError(t, err)

	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{1})}, 1)
	db.Close()

	assert.NotPanics(t, func() {
		assert.False(t, db.Exists([]byte{1}))
	})
}

// Scenario:
//  1. Store a digest and close the database.
//  2. Expect List to return nothing, and closing again not to panic.
func TestListAndCloseAfterClose(t *testing.T) {
	db, err := NewBatchAttestationDB(t.TempDir(), testutil.CreateLogger(t, 0))
	require.NoError(t, err)

	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{1})}, 1)
	db.Close()

	assert.NotPanics(t, func() {
		digests, epochs := db.List()
		assert.Empty(t, digests)
		assert.Empty(t, epochs)
		db.Close()
	})
}

// Scenario:
//  1. Close the database.
//  2. Expect Put to panic.
func TestPutAfterClosePanics(t *testing.T) {
	db, err := NewBatchAttestationDB(t.TempDir(), testutil.CreateLogger(t, 0))
	require.NoError(t, err)
	db.Close()

	assert.Panics(t, func() {
		db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{1})}, 1)
	})
}

// Scenario:
//  1. Store a digest.
//  2. Call Exists from several goroutines while another goroutine closes the database.
//  3. Expect no call to panic.
func TestExistsConcurrentWithClose(t *testing.T) {
	db, err := NewBatchAttestationDB(t.TempDir(), testutil.CreateLogger(t, 0))
	require.NoError(t, err)
	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{1})}, 1)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 1000 {
				db.Exists([]byte{1})
			}
		}()
	}
	close(start)
	db.Close()
	wg.Wait()
}

// TestExistsFailingReadPanics verifies that Exists fail-stops (panics) on a
// genuine read error that is neither ErrNotFound nor a closed DB, matching Put's
// behavior of not silently discarding pebble errors.
func TestExistsFailingReadPanics(t *testing.T) {
	db, fs := openFaultyDB(t)
	defer db.Close()

	// Store a digest and flush it to an on-disk table so reads hit the file system.
	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{7})}, 1)
	require.NoError(t, db.db.Flush())

	// Arm the fault: subsequent table reads fail with an IO error.
	fs.failReads.Store(true)

	assert.Panics(t, func() {
		db.Exists([]byte{7})
	})
}

// Scenario:
//  1. Store a digest.
//  2. Make syncing a file fail.
//  3. Expect Put of another digest to panic.
func TestPutFailingWritePanics(t *testing.T) {
	db, fs := openFaultyDB(t)
	defer db.Close()

	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{1})}, 1)
	fs.failSyncs.Store(true)

	assert.Panics(t, func() {
		db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{2})}, 1)
	})
}

// Scenario:
//  1. Store a digest.
//  2. Make syncing a file fail.
//  3. Expect Clean of that digest to panic.
func TestCleanFailingWritePanics(t *testing.T) {
	db, fs := openFaultyDB(t)
	defer db.Close()

	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{1})}, 1)
	fs.failSyncs.Store(true)

	assert.Panics(t, func() {
		db.Clean(2)
	})
}

// TestCleanAfterClosePanics verifies that Clean fail-stops (panics) when the
// database is closed, rather than silently skipping the deletion.
func TestCleanAfterClosePanics(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := testutil.CreateLogger(t, 0)
	db, err := NewBatchAttestationDB(dir, logger)
	require.NoError(t, err)

	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{1})}, 1)

	db.Close()

	assert.Panics(t, func() {
		db.Clean(2)
	})
}

// TestCleanFailingIterationPanics verifies that Clean fail-stops (panics) when
// the iterator encounters an error (surfaced by iter.Close()), rather than
// silently writing a partial/empty delete batch and masking the fault.
func TestCleanFailingIterationPanics(t *testing.T) {
	db, fs := openFaultyDB(t)
	defer db.Close()

	// Store a digest and flush it to an on-disk table so iteration hits the file system.
	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{7})}, 1)
	require.NoError(t, db.db.Flush())

	// Arm the fault: subsequent table reads fail with an IO error, so the
	// iterator in Clean cannot read the table and accumulates an error.
	fs.failReads.Store(true)

	assert.Panics(t, func() {
		db.Clean(2)
	})
}

// openFaultyDB opens a database on an in-memory file system whose reads can be made to fail. The block
// cache is kept minimal so that reads reach the file system.
func openFaultyDB(t *testing.T) (*BatchAttestationDB, *faultyFS) {
	fs := &faultyFS{FS: vfs.NewMem()}
	db, err := openBatchAttestationDB("db", &pebble.Options{FS: fs, CacheSize: 1}, testutil.CreateLogger(t, 0))
	require.NoError(t, err)
	return db, fs
}

// faultyFS wraps an in-memory file system and, once armed, fails every read of a
// file opened through it with an IO error that is neither ErrNotFound nor ErrClosed.
type faultyFS struct {
	vfs.FS
	failReads atomic.Bool
	failSyncs atomic.Bool
}

func (fs *faultyFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	return fs.wrap(f, err)
}

func (fs *faultyFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	return fs.wrap(f, err)
}

func (fs *faultyFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname, category)
	return fs.wrap(f, err)
}

func (fs *faultyFS) wrap(f vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	return &faultyFile{File: f, fs: fs}, nil
}

type faultyFile struct {
	vfs.File
	fs *faultyFS
}

func (f *faultyFile) ReadAt(p []byte, off int64) (int, error) {
	if f.fs.failReads.Load() {
		return 0, errors.New("injected IO fault")
	}
	return f.File.ReadAt(p, off)
}

func (f *faultyFile) Sync() error {
	if f.fs.failSyncs.Load() {
		return errors.New("injected IO fault")
	}
	return f.File.Sync()
}

func (f *faultyFile) SyncData() error {
	if f.fs.failSyncs.Load() {
		return errors.New("injected IO fault")
	}
	return f.File.SyncData()
}

func (f *faultyFile) SyncTo(length int64) (bool, error) {
	if f.fs.failSyncs.Load() {
		return false, errors.New("injected IO fault")
	}
	return f.File.SyncTo(length)
}
