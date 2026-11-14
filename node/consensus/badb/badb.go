/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package badb

import (
	"encoding/binary"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/pebbledb"
	"github.com/hyperledger/fabric-x-orderer/common/types"
)

// The first byte of a key names its keyspace.
const (
	digestKeyPrefix  = 0 // digest -> decision
	epochKeyPrefix   = 1 // decision | digest -> empty
	batchIDKeyPrefix = 2 // shard | primary | seq | digest -> decision
)

// BatchAttestationDB records the batches that consensus has ordered. Their digests let the consenter
// reject a batch attestation fragment for a batch that is already ordered, and their batch IDs record the
// decision that ordered each batch of a <shard, primary>, in sequence order. It is safe for concurrent use.
type BatchAttestationDB struct {
	// lock guards closed. Pebble panics when a closed database is used, so every method holds it for
	// reading while it uses db, and Close holds it for writing.
	lock   sync.RWMutex
	closed bool
	db     *pebble.DB
	logger *flogging.FabricLogger
}

// NewBatchAttestationDB opens the database in the directory at path, creating it if it does not exist.
func NewBatchAttestationDB(path string, logger *flogging.FabricLogger) (*BatchAttestationDB, error) {
	return openBatchAttestationDB(path, nil, logger)
}

// openBatchAttestationDB is NewBatchAttestationDB with Pebble options, such as a file system for tests.
func openBatchAttestationDB(path string, opts *pebble.Options, logger *flogging.FabricLogger) (*BatchAttestationDB, error) {
	db, err := pebbledb.Open(path, opts, logger)
	if err != nil {
		return nil, err
	}
	return &BatchAttestationDB{db: db, logger: logger}, nil
}

// Close closes the database. Closing it again does nothing. Afterwards Exists reports every digest as
// absent and List returns nothing, while Put and Clean panic.
func (db *BatchAttestationDB) Close() {
	db.lock.Lock()
	defer db.lock.Unlock()
	if db.closed {
		return
	}
	db.closed = true
	if err := db.db.Close(); err != nil {
		db.logger.Errorf("Failed closing database: %v", err)
	}
}

// List returns every stored digest and, at the same index, its epoch, in digest order. It panics on a
// read error.
func (db *BatchAttestationDB) List() ([][]byte, []uint64) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	if db.closed {
		return nil, nil
	}

	iter, err := db.db.NewIter(prefixBounds(digestKeyPrefix))
	if err != nil {
		db.logger.Panicf("Failed iterating over database: %v", err)
	}

	var digests [][]byte
	var epochs []uint64

	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			db.logger.Panicf("Failed iterating over database: %v", err)
		}

		key := iter.Key()
		digestCopy := make([]byte, len(key[1:]))
		copy(digestCopy, key[1:])

		digests = append(digests, digestCopy)
		epochs = append(epochs, binary.BigEndian.Uint64(value))
	}

	if err := iter.Close(); err != nil {
		db.logger.Panicf("Failed iterating over database: %v", err)
	}

	return digests, epochs
}

// Exists reports whether digest is stored. It returns false on a closed database and panics on a read
// error.
func (db *BatchAttestationDB) Exists(digest []byte) bool {
	db.lock.RLock()
	defer db.lock.RUnlock()
	if db.closed {
		// The DB may be closed while the consenter is soft-stopping (the BFT
		// engine can still drive Exists via SimulateStateTransition). Treat a
		// closed DB as "digest absent" rather than fail-stopping.
		return false
	}

	_, closer, err := db.db.Get(makeDigestKey(digest))
	switch err {
	case nil:
		if err := closer.Close(); err != nil {
			db.logger.Panicf("Failed reading from database: %v", err)
		}
		return true
	case pebble.ErrNotFound:
		return false
	default:
		// A corruption/IO error must not be silently reported as absence; match
		// Put's fail-stop behavior.
		db.logger.Panicf("Failed reading from database: %v", err)
		return false
	}
}

// Put indexes the batches that one decision ordered: each batch is stored under its digest, which Exists
// checks, and under its batch ID <shard, primary, seq, digest>, with the decision as the value of both. It
// writes in one atomic write synced to disk, and storing the same batches again changes nothing. It panics
// on a write error or a closed database.
func (db *BatchAttestationDB) Put(ids []types.BatchID, decision types.DecisionNum) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	if db.closed {
		db.logger.Panicf("Failed updating database: %v", pebble.ErrClosed)
	}

	decisionBuff := make([]byte, 8)
	binary.BigEndian.PutUint64(decisionBuff, uint64(decision))

	batch := db.db.NewBatch()
	defer batch.Close()
	for _, id := range ids {
		if err := batch.Set(makeDigestKey(id.Digest()), decisionBuff, nil); err != nil {
			db.logger.Panicf("Failed updating database: %v", err)
		}
		if err := batch.Set(makeEpochKey(id.Digest(), decisionBuff), []byte{0}, nil); err != nil {
			db.logger.Panicf("Failed updating database: %v", err)
		}
		if err := batch.Set(makeBatchIDKey(id), decisionBuff, nil); err != nil {
			db.logger.Panicf("Failed updating database: %v", err)
		}
	}

	// Synced, because the consenter indexes a decision before appending it to its ledger, and an unsynced
	// Pebble write is lost on a process crash.
	if err := batch.Commit(pebble.Sync); err != nil {
		db.logger.Panicf("Failed updating database: %v", err)
	}
}

// Clean deletes every digest whose epoch is lower than epochToDelete, in one atomic write synced to
// disk. It panics on a read or write error, or on a closed database.
func (db *BatchAttestationDB) Clean(epochToDelete uint64) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	if db.closed {
		db.logger.Panicf("Failed updating database: %v", pebble.ErrClosed)
	}

	iter, err := db.db.NewIter(prefixBounds(epochKeyPrefix))
	if err != nil {
		db.logger.Panicf("Failed iterating over database: %v", err)
	}

	batch := db.db.NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		epochKey := iter.Key()
		epochBuff := epochKey[1:]
		epoch := binary.BigEndian.Uint64(epochBuff[:8])
		digest := epochBuff[8:]
		if epoch >= epochToDelete {
			continue
		}

		// The batch copies the keys, so the iterator may reuse its buffer afterwards.
		if err := batch.Delete(makeDigestKey(digest), nil); err != nil {
			db.logger.Panicf("Failed updating database: %v", err)
		}
		if err := batch.Delete(epochKey, nil); err != nil {
			db.logger.Panicf("Failed updating database: %v", err)
		}
	}

	// Iteration stops on error, and the error is returned by Close. Without
	// this check Clean could write a partial/empty delete batch and mask an
	// underlying corruption/IO fault.
	if err := iter.Close(); err != nil {
		db.logger.Panicf("Failed iterating over database: %v", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		db.logger.Panicf("Failed updating database: %v", err)
	}
}

// prefixBounds returns iterator bounds that cover exactly the keys starting with prefix.
func prefixBounds(prefix byte) *pebble.IterOptions {
	return &pebble.IterOptions{LowerBound: []byte{prefix}, UpperBound: []byte{prefix + 1}}
}

// makeDigestKey returns the key of digest in the digest keyspace.
func makeDigestKey(digest []byte) []byte {
	buff := make([]byte, len(digest)+1)
	buff[0] = digestKeyPrefix
	copy(buff[1:], digest)
	return buff
}

// makeBatchIDKey returns the key of a batch ID in the batch ID keyspace: the shard and the primary as 2
// big-endian bytes each, the seq as 8 big-endian bytes, then the digest. The keys therefore sort by shard,
// primary and seq, so the batches of one <shard, primary> are contiguous and in sequence order.
func makeBatchIDKey(id types.BatchID) []byte {
	digest := id.Digest()
	buff := make([]byte, 1+2+2+8+len(digest))
	buff[0] = batchIDKeyPrefix
	binary.BigEndian.PutUint16(buff[1:], uint16(id.Shard()))
	binary.BigEndian.PutUint16(buff[3:], uint16(id.Primary()))
	binary.BigEndian.PutUint64(buff[5:], uint64(id.Seq()))
	copy(buff[13:], digest)
	return buff
}

// makeEpochKey returns the key of digest in the epoch keyspace. The 8-byte big-endian epoch comes first,
// so the keys sort by epoch.
func makeEpochKey(digest []byte, epoch []byte) []byte {
	buff := make([]byte, len(digest)+len(epoch)+1)
	buff[0] = epochKeyPrefix
	copy(buff[1:], epoch)
	copy(buff[len(epoch)+1:], digest)
	return buff
}
