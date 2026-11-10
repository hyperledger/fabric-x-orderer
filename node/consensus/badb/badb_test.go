/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package badb

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
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

	// Insert test data
	digests := [][]byte{{1}, {2}, {3}}
	epochs := []uint64{1, 2, 3}
	db.Put(digests, epochs)

	// Test that each digest exists in the DB
	for _, digest := range digests {
		assert.True(t, db.Exists(digest))
	}

	// Test retrieval of digests and epochs from the DB
	storedDigests, storedEpochs := db.List()
	assert.Equal(t, len(digests), len(storedDigests))
	assert.Equal(t, len(epochs), len(storedEpochs))

	for i, digest := range digests {
		assert.Contains(t, storedDigests, digest)
		assert.Contains(t, storedEpochs, epochs[i])
	}

	// Test cleaning of the DB
	db.Clean(3)
	assert.False(t, db.Exists(digests[0]))
	assert.False(t, db.Exists(digests[1]))
	assert.True(t, db.Exists(digests[2]))

	storedDigests, storedEpochs = db.List()
	assert.Len(t, storedDigests, 1)
	assert.Len(t, storedEpochs, 1)

	// Ensure that the DB cannot be accessed after closing
	db.Close()
	assert.False(t, db.Exists(digests[2]))
}

// Scenario:
//  1. Store a digest, close the database and reopen it.
//  2. Expect the digest to exist and to be listed with its epoch.
func TestReopenKeepsDigests(t *testing.T) {
	dir := t.TempDir()
	logger := testutil.CreateLogger(t, 0)

	db, err := NewBatchAttestationDB(dir, logger)
	require.NoError(t, err)
	db.Put([][]byte{{5}}, []uint64{9})
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

	db.Put([][]byte{{1}}, []uint64{1})
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

	db.Put([][]byte{{1}}, []uint64{1})
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
		db.Put([][]byte{{1}}, []uint64{1})
	})
}

// Scenario:
//  1. Store a digest.
//  2. Call Exists from several goroutines while another goroutine closes the database.
//  3. Expect no call to panic.
func TestExistsConcurrentWithClose(t *testing.T) {
	db, err := NewBatchAttestationDB(t.TempDir(), testutil.CreateLogger(t, 0))
	require.NoError(t, err)
	db.Put([][]byte{{1}}, []uint64{1})

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
	db.Put([][]byte{{7}}, []uint64{1})
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

	db.Put([][]byte{{1}}, []uint64{1})
	fs.failSyncs.Store(true)

	assert.Panics(t, func() {
		db.Put([][]byte{{2}}, []uint64{1})
	})
}

// Scenario:
//  1. Store a digest.
//  2. Make syncing a file fail.
//  3. Expect Clean of that digest to panic.
func TestCleanFailingWritePanics(t *testing.T) {
	db, fs := openFaultyDB(t)
	defer db.Close()

	db.Put([][]byte{{1}}, []uint64{1})
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

	db.Put([][]byte{{1}}, []uint64{1})

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
	db.Put([][]byte{{7}}, []uint64{1})
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
