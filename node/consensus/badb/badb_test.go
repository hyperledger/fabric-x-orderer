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
	"sync/atomic"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
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
	_, err = db.db.Get(makeDigestKey(ids[2].Digest()), nil)
	assert.Error(t, err)
}

// TestPutWritesBatchIDKey verifies that Put indexes every batch by <shard, primary, seq, digest>, with the
// decision that committed it as the value.
func TestPutWritesBatchIDKey(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	db, err := NewBatchAttestationDB(t.TempDir(), logger)
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
		value, err := db.db.Get(makeBatchIDKey(id), nil)
		require.NoError(t, err)
		assert.Equal(t, uint64(5), binary.BigEndian.Uint64(value))
	}

	// <shard, primary, seq> of the first batch with a digest that was never indexed
	_, err = db.db.Get(makeBatchIDKey(state.NewAvailableBatch(1, 0, 7, []byte{4})), nil)
	assert.ErrorIs(t, err, leveldb.ErrNotFound)
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

	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{1})}, 1)
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
	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{7})}, 1)
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

	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{1})}, 1)

	// Closing the DB makes the subsequent Write in Clean fail with ErrClosed.
	db.Close()

	assert.Panics(t, func() {
		db.Clean(2)
	})
}

// TestCleanFailingIterationPanics verifies that Clean fail-stops (panics) when
// the iterator encounters an error (surfaced via iter.Error()), rather than
// silently writing a partial/empty delete batch and masking the fault.
func TestCleanFailingIterationPanics(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	stor := newFaultyStorage()

	// Disable the open-files cache so every table read re-opens the file
	// through the (faulty) storage layer.
	ldb, err := leveldb.Open(stor, &opt.Options{OpenFilesCacheCapacity: -1})
	require.NoError(t, err)
	db := &BatchAttestationDB{db: ldb, logger: logger}
	defer db.Close()

	// Store a digest and flush it to an on-disk table so iteration hits storage.Open.
	db.Put([]types.BatchID{state.NewAvailableBatch(1, 0, 0, []byte{7})}, 1)
	require.NoError(t, ldb.CompactRange(util.Range{}))

	// Arm the fault: subsequent table opens fail with an IO error, so the
	// iterator in Clean cannot read the table and accumulates an error.
	stor.failOpen.Store(true)

	assert.Panics(t, func() {
		db.Clean(2)
	})
}
