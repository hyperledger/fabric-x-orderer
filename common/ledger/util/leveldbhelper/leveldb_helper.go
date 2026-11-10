/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package leveldbhelper

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-common/tools/fileutil"
	"github.com/hyperledger/fabric-x-orderer/common/pebbledb"
)

var logger = flogging.MustGetLogger("leveldbhelper")

type dbState int32

const (
	closed dbState = iota
	opened
)

// DB - a wrapper on an actual store
type DB struct {
	conf *Conf
	db   *pebble.DB
	// dbState is guarded by mutex. Every operation holds mutex for reading while it uses db, and Open and
	// Close hold it for writing. Pebble panics on a closed DB, so once closed every operation returns
	// pebble.ErrClosed without reaching it.
	dbState dbState
	mutex   sync.RWMutex
}

// CreateDB constructs a `DB`
func CreateDB(conf *Conf) *DB {
	return &DB{
		conf:    conf,
		dbState: closed,
	}
}

// Open opens the underlying db
func (dbInst *DB) Open() {
	dbInst.mutex.Lock()
	defer dbInst.mutex.Unlock()
	if dbInst.dbState == opened {
		return
	}
	dbPath := dbInst.conf.DBPath
	var err error
	var dirEmpty bool
	if dirEmpty, err = fileutil.CreateDirIfMissing(dbPath); err != nil {
		panic(fmt.Sprintf("Error creating dir if missing: %s", err))
	}
	opts := &pebble.Options{ErrorIfNotExists: !dirEmpty}
	if dbInst.db, err = pebbledb.Open(dbPath, opts, logger); err != nil {
		panic(fmt.Sprintf("Error opening pebble db: %s", err))
	}
	dbInst.dbState = opened
}

// IsEmpty returns whether or not a database is empty
func (dbInst *DB) IsEmpty() (bool, error) {
	empty, err := dbInst.isEmptyRange(nil, nil)
	if err != nil {
		return false, fmt.Errorf("error while trying to see if the db at path [%s] is empty: %w", dbInst.conf.DBPath, err)
	}
	return empty, nil
}

// isEmptyRange reports whether no key lies in [lower, upper). A nil bound is unbounded.
func (dbInst *DB) isEmptyRange(lower, upper []byte) (bool, error) {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	if dbInst.dbState == closed {
		return false, pebble.ErrClosed
	}
	itr, err := dbInst.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return false, err
	}
	hasItems := itr.First()
	if err := itr.Close(); err != nil {
		return false, err
	}
	return !hasItems, nil
}

// Close closes the underlying db
func (dbInst *DB) Close() {
	dbInst.mutex.Lock()
	defer dbInst.mutex.Unlock()
	if dbInst.dbState == closed {
		return
	}
	if err := dbInst.db.Close(); err != nil {
		logger.Errorf("Error closing pebble db: %s", err)
	}
	dbInst.dbState = closed
}

// Get returns the value for the given key
func (dbInst *DB) Get(key []byte) ([]byte, error) {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	if dbInst.dbState == closed {
		return nil, fmt.Errorf("error retrieving key [%#v]: %w", key, pebble.ErrClosed)
	}
	value, closer, err := dbInst.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		logger.Errorf("Error retrieving key [%#v]: %s", key, err)
		return nil, fmt.Errorf("error retrieving key [%#v]: %w", key, err)
	}
	// Pebble owns value until closer is closed.
	value = bytes.Clone(value)
	if err := closer.Close(); err != nil {
		return nil, fmt.Errorf("error retrieving key [%#v]: %w", key, err)
	}
	return value, nil
}

// Put saves the key/value
func (dbInst *DB) Put(key []byte, value []byte, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	if dbInst.dbState == closed {
		return fmt.Errorf("error writing key [%#v]: %w", key, pebble.ErrClosed)
	}
	if err := dbInst.db.Set(key, value, writeOptions(sync)); err != nil {
		logger.Errorf("Error writing key [%#v]", key)
		return fmt.Errorf("error writing key [%#v]: %w", key, err)
	}
	return nil
}

// Delete deletes the given key
func (dbInst *DB) Delete(key []byte, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	if dbInst.dbState == closed {
		return fmt.Errorf("error deleting key [%#v]: %w", key, pebble.ErrClosed)
	}
	if err := dbInst.db.Delete(key, writeOptions(sync)); err != nil {
		logger.Errorf("Error deleting key [%#v]", key)
		return fmt.Errorf("error deleting key [%#v]: %w", key, err)
	}
	return nil
}

// DeleteRange deletes every key in [startKey, endKey), atomically
func (dbInst *DB) DeleteRange(startKey, endKey []byte, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	if dbInst.dbState == closed {
		return fmt.Errorf("error deleting range: %w", pebble.ErrClosed)
	}
	if err := dbInst.db.DeleteRange(startKey, endKey, writeOptions(sync)); err != nil {
		return fmt.Errorf("error deleting range: %w", err)
	}
	return nil
}

// newBatch returns a batch from Pebble's pool, bound to the open db. On a closed db it returns a
// standalone batch, which WriteBatch then refuses with pebble.ErrClosed.
func (dbInst *DB) newBatch() *pebble.Batch {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	if dbInst.dbState == closed {
		return &pebble.Batch{}
	}
	return dbInst.db.NewBatch()
}

// WriteBatch writes a batch. A batch can be written once: writing it again panics unless it is Reset
// first.
func (dbInst *DB) WriteBatch(batch *pebble.Batch, sync bool) error {
	dbInst.mutex.RLock()
	defer dbInst.mutex.RUnlock()
	if dbInst.dbState == closed {
		return fmt.Errorf("error writing batch: %w", pebble.ErrClosed)
	}
	if err := dbInst.db.Apply(batch, writeOptions(sync)); err != nil {
		return fmt.Errorf("error writing batch: %w", err)
	}
	return nil
}

func writeOptions(sync bool) *pebble.WriteOptions {
	if sync {
		return pebble.Sync
	}
	return pebble.NoSync
}
