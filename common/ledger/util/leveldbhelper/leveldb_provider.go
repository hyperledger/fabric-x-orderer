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
	"github.com/hyperledger/fabric-x-orderer/common/ledger/dataformat"
)

const (
	// internalDBName is used to keep track of data related to internals such as data format
	// _ is used as name because this is not allowed as a channelname
	internalDBName = "_"
)

var (
	// errBatchWritten is returned by WriteBatch for a batch that was already written and not Reset since.
	errBatchWritten = errors.New("the batch was already written; Reset it to write it again")

	dbNameKeySep     = []byte{0x00}
	lastKeyIndicator = byte(0x01)
	formatVersionKey = []byte{'f'} // a single key in db whose value indicates the version of the data format
)

// closeFunc closes the db handle
type closeFunc func()

// Conf configuration for `Provider`
//
// `ExpectedFormat` is the expected value of the format key in the internal database.
// At the time of opening the db, A check is performed that
// either the db is empty (i.e., opening for the first time) or the value
// of the formatVersionKey is equal to `ExpectedFormat`. Otherwise, an error is returned.
// A nil value for ExpectedFormat indicates that the format is never set and hence there is no such record.
type Conf struct {
	DBPath         string
	ExpectedFormat string
}

// Provider enables to use a single db as multiple logical dbs
type Provider struct {
	db *DB

	mux       sync.Mutex
	dbHandles map[string]*DBHandle
}

// DataFormatInfo contains the information about the version of the data format
type DataFormatInfo struct {
	FormatVerison string // version of the data format
	IsDBEmpty     bool   // set to true if the db does not contain any data
}

// RetrieveDataFormatInfo retrieves the DataFormatInfo for the db at the supplied `dbPath`
func RetrieveDataFormatInfo(dbPath string) (*DataFormatInfo, error) {
	db := CreateDB(&Conf{DBPath: dbPath})
	db.Open()
	defer db.Close()

	dbEmpty, err := db.IsEmpty()
	if err != nil {
		return nil, err
	}

	internalDB := &DBHandle{
		db:     db,
		dbName: internalDBName,
	}

	formatVersion, err := internalDB.Get(formatVersionKey)
	if err != nil {
		return nil, err
	}

	return &DataFormatInfo{
		IsDBEmpty:     dbEmpty,
		FormatVerison: string(formatVersion),
	}, nil
}

// NewProvider constructs a Provider
func NewProvider(conf *Conf) (*Provider, error) {
	db, err := openDBAndCheckFormat(conf)
	if err != nil {
		return nil, err
	}
	return &Provider{
		db:        db,
		dbHandles: make(map[string]*DBHandle),
	}, nil
}

func openDBAndCheckFormat(conf *Conf) (d *DB, e error) {
	db := CreateDB(conf)
	db.Open()

	defer func() {
		if e != nil {
			db.Close()
		}
	}()

	internalDB := &DBHandle{
		db:     db,
		dbName: internalDBName,
	}

	dbEmpty, err := db.IsEmpty()
	if err != nil {
		return nil, err
	}

	if dbEmpty && conf.ExpectedFormat != "" {
		logger.Infof("DB is empty Setting db format as %s", conf.ExpectedFormat)
		if err := internalDB.Put(formatVersionKey, []byte(conf.ExpectedFormat), true); err != nil {
			return nil, err
		}
		return db, nil
	}

	formatVersion, err := internalDB.Get(formatVersionKey)
	if err != nil {
		return nil, err
	}
	logger.Debugf("Checking for db format at path [%s]", conf.DBPath)

	if !bytes.Equal(formatVersion, []byte(conf.ExpectedFormat)) {
		logger.Errorf("The db at path [%s] contains data in unexpected format. expected data format = [%s] (%#v), data format = [%s] (%#v).",
			conf.DBPath, conf.ExpectedFormat, []byte(conf.ExpectedFormat), formatVersion, formatVersion)
		return nil, &dataformat.ErrFormatMismatch{
			ExpectedFormat: conf.ExpectedFormat,
			Format:         string(formatVersion),
			DBInfo:         fmt.Sprintf("pebble db at [%s]", conf.DBPath),
		}
	}
	logger.Debug("format is latest, nothing to do")
	return db, nil
}

// GetDataFormat returns the format of the data
func (p *Provider) GetDataFormat() (string, error) {
	f, err := p.GetDBHandle(internalDBName).Get(formatVersionKey)
	return string(f), err
}

func (p *Provider) SetDataFormat(format string) error {
	db := p.GetDBHandle(internalDBName)
	return db.Put(formatVersionKey, []byte(format), true)
}

// GetDBHandle returns a handle to a named db
func (p *Provider) GetDBHandle(dbName string) *DBHandle {
	p.mux.Lock()
	defer p.mux.Unlock()
	dbHandle := p.dbHandles[dbName]
	if dbHandle == nil {
		closeFunc := func() {
			p.mux.Lock()
			defer p.mux.Unlock()
			delete(p.dbHandles, dbName)
		}
		dbHandle = &DBHandle{dbName, p.db, closeFunc}
		p.dbHandles[dbName] = dbHandle
	}
	return dbHandle
}

// Close closes the underlying db
func (p *Provider) Close() {
	p.db.Close()
}

// Drop drops all the data for the given dbName
func (p *Provider) Drop(dbName string) error {
	dbHandle := p.GetDBHandle(dbName)
	defer dbHandle.Close()
	return dbHandle.deleteAll()
}

// DBHandle is an handle to a named db
type DBHandle struct {
	dbName    string
	db        *DB
	closeFunc closeFunc
}

// Get returns the value for the given key
func (h *DBHandle) Get(key []byte) ([]byte, error) {
	return h.db.Get(constructLevelKey(h.dbName, key))
}

// Put saves the key/value
func (h *DBHandle) Put(key []byte, value []byte, sync bool) error {
	return h.db.Put(constructLevelKey(h.dbName, key), value, sync)
}

// Delete deletes the given key
func (h *DBHandle) Delete(key []byte, sync bool) error {
	return h.db.Delete(constructLevelKey(h.dbName, key), sync)
}

// deleteAll deletes all the keys that belong to the channel (dbName), in one atomic range deletion.
func (h *DBHandle) deleteAll() error {
	sKey, eKey := h.keyRange()
	if err := h.db.DeleteRange(sKey, eKey, true); err != nil {
		return fmt.Errorf("error deleting the keys of [%s]: %w", h.dbName, err)
	}
	return nil
}

// IsEmpty returns true if no data exists for the DBHandle
func (h *DBHandle) IsEmpty() (bool, error) {
	sKey, eKey := h.keyRange()
	empty, err := h.db.isEmptyRange(sKey, eKey)
	if err != nil {
		return false, fmt.Errorf("internal db error while checking whether [%s] is empty: %w", h.dbName, err)
	}
	return empty, nil
}

// NewUpdateBatch returns a new UpdateBatch that can be used to update the db
func (h *DBHandle) NewUpdateBatch() *UpdateBatch {
	return &UpdateBatch{
		db:     h.db,
		dbName: h.dbName,
		batch:  h.db.newBatch(),
	}
}

// WriteBatch writes a batch in an atomic way. A batch is written once: the write releases it, and
// writing it again returns errBatchWritten unless it is Reset first. A batch is written to the db it was
// created from: if that db is closed, the write fails with pebble.ErrClosed and can be tried again only
// while the same db stays closed, since a reopened db is a new Pebble instance.
func (h *DBHandle) WriteBatch(batch *UpdateBatch, sync bool) error {
	if batch == nil {
		return nil
	}
	if batch.batch == nil {
		return errBatchWritten
	}
	if batch.batch.Empty() {
		return nil
	}
	err := h.db.WriteBatch(batch.batch, sync)
	if errors.Is(err, pebble.ErrClosed) {
		// Refused before Pebble saw the batch, so it is still intact.
		return err
	}
	if err == nil {
		// Return the batch to Pebble's pool, as Pebble's own Set does after a successful Apply.
		err = batch.batch.Close()
	}
	// Pebble refuses to apply a batch twice, including one whose Apply failed, so it is dropped either way.
	batch.batch = nil
	batch.size = 0
	return err
}

// keyRange returns the db keys that bound all the keys of this handle, as [start, end).
func (h *DBHandle) keyRange() ([]byte, []byte) {
	sKey := constructLevelKey(h.dbName, nil)
	eKey := constructLevelKey(h.dbName, nil)
	// replace the last byte 'dbNameKeySep' by 'lastKeyIndicator'
	eKey[len(eKey)-1] = lastKeyIndicator
	return sKey, eKey
}

// Close closes the DBHandle after its db data have been deleted
func (h *DBHandle) Close() {
	if h.closeFunc != nil {
		h.closeFunc()
	}
}

// UpdateBatch encloses the details of multiple `updates`
type UpdateBatch struct {
	// batch is nil once the batch was written.
	batch  *pebble.Batch
	db     *DB
	dbName string
	size   int
}

// Put adds a KV
func (b *UpdateBatch) Put(key []byte, value []byte) {
	if value == nil {
		panic("Nil value not allowed")
	}
	b.requireNotWritten()
	k := constructLevelKey(b.dbName, key)
	// A batch without an index never fails to record an operation.
	_ = b.batch.Set(k, value, nil)
	b.size += len(k) + len(value)
}

// Delete deletes a Key and associated value
func (b *UpdateBatch) Delete(key []byte) {
	b.requireNotWritten()
	k := constructLevelKey(b.dbName, key)
	b.size += len(k)
	_ = b.batch.Delete(k, nil)
}

// Size returns the current size of the batch. A written batch has none.
func (b *UpdateBatch) Size() int {
	return b.size
}

// Len returns number of records in the batch. A written batch has none.
func (b *UpdateBatch) Len() int {
	if b.batch == nil {
		return 0
	}
	return int(b.batch.Count())
}

// Reset empties the batch, so it can be written again. A written batch gets a new batch from the db.
func (b *UpdateBatch) Reset() {
	if b.batch == nil {
		b.batch = b.db.newBatch()
	} else {
		b.batch.Reset()
	}
	b.size = 0
}

// requireNotWritten panics if the batch was written: Pebble recycles a written batch, so adding to it
// would write into a batch that another caller may hold.
func (b *UpdateBatch) requireNotWritten() {
	if b.batch == nil {
		panic(errBatchWritten)
	}
}

func constructLevelKey(dbName string, key []byte) []byte {
	return append(append([]byte(dbName), dbNameKeySep...), key...)
}
