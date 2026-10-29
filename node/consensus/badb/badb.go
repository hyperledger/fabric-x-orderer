/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package badb

import (
	"encoding/binary"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type BatchAttestationDB struct {
	db     *leveldb.DB
	logger *flogging.FabricLogger
}

func NewBatchAttestationDB(path string, logger *flogging.FabricLogger) (*BatchAttestationDB, error) {
	db, err := leveldb.OpenFile(path, nil)
	return &BatchAttestationDB{db: db, logger: logger}, err
}

func (db *BatchAttestationDB) Close() {
	db.db.Close()
}

func (db *BatchAttestationDB) List() ([][]byte, []uint64) {
	iter := db.db.NewIterator(&util.Range{}, nil)
	defer iter.Release()

	var digests [][]byte
	var epochs []uint64

	for iter.Next() {
		key := iter.Key()
		if key[0] == 0 {
			digestCopy := make([]byte, len(key[1:]))
			copy(digestCopy, key[1:])

			digests = append(digests, digestCopy)
			epochs = append(epochs, binary.BigEndian.Uint64(iter.Value()))
		}
	}

	return digests, epochs
}

func (db *BatchAttestationDB) Exists(digest []byte) bool {
	_, err := db.db.Get(makeDigestKey(digest), nil)
	switch err {
	case nil:
		return true
	case leveldb.ErrNotFound:
		return false
	case leveldb.ErrClosed:
		// The DB may be closed while the consenter is soft-stopping (the BFT
		// engine can still drive Exists via SimulateStateTransition). Treat a
		// closed DB as "digest absent" rather than fail-stopping.
		return false
	default:
		// A corruption/IO error must not be silently reported as absence; match
		// Put's fail-stop behavior.
		db.logger.Panicf("Failed reading from database: %v", err)
		return false
	}
}

// Put indexes the batches that one decision committed. Every batch is recorded under its digest, which
// Exists checks, and under its batch ID <shard, primary, seq, digest>, with the decision as the value of
// both. All the keys are written in one atomic batch, and writing the same batches again is harmless.
func (db *BatchAttestationDB) Put(ids []types.BatchID, decision types.DecisionNum) {
	decisionBuff := make([]byte, 8)
	binary.BigEndian.PutUint64(decisionBuff, uint64(decision))

	batch := new(leveldb.Batch)
	for _, id := range ids {
		batch.Put(makeDigestKey(id.Digest()), decisionBuff)
		batch.Put(makeEpochKey(id.Digest(), decisionBuff), []byte{0})
		batch.Put(makeBatchIDKey(id), decisionBuff)
	}

	// TODO the write is not synced to disk, unlike the consenter ledger's appends, so a power loss may keep a
	// decision in the ledger and lose its index entries, which nothing re-indexes.
	if err := db.db.Write(batch, nil); err != nil {
		db.logger.Panicf("Failed updating database: %v", err)
	}
}

func (db *BatchAttestationDB) Clean(epochToDelete uint64) {
	iter := db.db.NewIterator(util.BytesPrefix([]byte{1}), nil)
	defer iter.Release()

	batch := new(leveldb.Batch)

	for iter.Next() {
		epochKey := iter.Key()
		epochBuff := epochKey[1:]
		epoch := binary.BigEndian.Uint64(epochBuff[:8])
		digest := epochBuff[8:]
		if epoch >= epochToDelete {
			continue
		}

		batch.Delete(makeDigestKey(digest))
		batch.Delete(epochKey)
	}

	// Next stops returning pairs on error; the error is retrieved via Error().
	// Without this check Clean could write a partial/empty delete batch and
	// mask an underlying corruption/IO fault.
	if err := iter.Error(); err != nil {
		db.logger.Panicf("Failed iterating over database: %v", err)
	}

	if err := db.db.Write(batch, nil); err != nil {
		db.logger.Panicf("Failed updating database: %v", err)
	}
}

func makeDigestKey(digest []byte) []byte {
	buff := make([]byte, len(digest)+1)
	copy(buff[1:], digest)
	return buff
}

// makeBatchIDKey returns the key of a batch ID: prefix 2, then the shard, the primary and the seq in
// big-endian, then the digest. Keys therefore sort by shard, primary and seq, so the batches of one
// <shard, primary> are contiguous and in sequence order.
func makeBatchIDKey(id types.BatchID) []byte {
	digest := id.Digest()
	buff := make([]byte, 1+2+2+8+len(digest))
	buff[0] = 2
	binary.BigEndian.PutUint16(buff[1:], uint16(id.Shard()))
	binary.BigEndian.PutUint16(buff[3:], uint16(id.Primary()))
	binary.BigEndian.PutUint64(buff[5:], uint64(id.Seq()))
	copy(buff[13:], digest)
	return buff
}

func makeEpochKey(digest []byte, epoch []byte) []byte {
	buff := make([]byte, len(digest)+len(epoch)+1)
	buff[0] = 1
	copy(buff[1:], epoch)
	copy(buff[len(epoch)+1:], digest)
	return buff
}
