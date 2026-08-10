/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package badb

import (
	"os"
	"sync/atomic"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
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
	_, err = db.db.Get(makeDigestKey(digests[2]), nil)
	assert.Error(t, err)
}

// TestExistsNotFound verifies that Exists returns false (and does not panic)
// for a digest that was never stored, i.e. leveldb.ErrNotFound is treated as absence.
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

// TestExistsAfterClose verifies that Exists treats leveldb.ErrClosed as benign
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

// TestExistsFailingReadPanics verifies that Exists fail-stops (panics) on a
// genuine read error that is neither ErrNotFound nor ErrClosed, matching Put's
// behavior of not silently discarding leveldb errors.
func TestExistsFailingReadPanics(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	stor := newFaultyStorage()

	// Disable the open-files cache so every table read re-opens the file
	// through the (faulty) storage layer.
	ldb, err := leveldb.Open(stor, &opt.Options{OpenFilesCacheCapacity: -1})
	require.NoError(t, err)
	db := &BatchAttestationDB{db: ldb, logger: logger}
	defer db.Close()

	// Store a digest and flush it to an on-disk table so reads hit storage.Open.
	db.Put([][]byte{{7}}, []uint64{1})
	require.NoError(t, ldb.CompactRange(util.Range{}))

	// Arm the fault: subsequent table opens fail with an IO error.
	stor.failOpen.Store(true)

	assert.Panics(t, func() {
		db.Exists([]byte{7})
	})
}

// faultyStorage wraps an in-memory leveldb storage and, once armed, fails all
// Open calls with a non-ErrNotFound, non-ErrClosed error to simulate an IO fault.
type faultyStorage struct {
	storage.Storage
	failOpen atomic.Bool
}

func newFaultyStorage() *faultyStorage {
	return &faultyStorage{Storage: storage.NewMemStorage()}
}

func (fs *faultyStorage) Open(fd storage.FileDesc) (storage.Reader, error) {
	if fs.failOpen.Load() {
		return nil, errors.New("injected IO fault")
	}
	return fs.Storage.Open(fd)
}

// TestCleanFailingWritePanics verifies that Clean fail-stops (panics) when the
// underlying db.Write fails, rather than silently discarding the error.
func TestCleanFailingWritePanics(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	logger := testutil.CreateLogger(t, 0)
	db, err := NewBatchAttestationDB(dir, logger)
	require.NoError(t, err)

	db.Put([][]byte{{1}}, []uint64{1})

	// Closing the DB makes the subsequent Write in Clean fail with ErrClosed.
	db.Close()

	assert.Panics(t, func() {
		db.Clean(2)
	})
}
