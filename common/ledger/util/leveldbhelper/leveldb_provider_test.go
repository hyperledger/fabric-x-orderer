/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package leveldbhelper

import (
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/dataformat"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	flogging.ActivateSpec("leveldbhelper=debug")
	os.Exit(m.Run())
}

// Scenario:
//  1. Put the same keys, with different values, in three handles of one provider.
//  2. Expect each handle to read back its own values.
//  3. Delete the keys in each handle and expect them to read back as nil.
func TestDBHandleBasicWriteAndReads(t *testing.T) {
	p := newTestProvider(t)
	dbNames := []string{"db1", "db2", ""}

	for _, dbName := range dbNames {
		db := p.GetDBHandle(dbName)
		require.NoError(t, db.Put([]byte("key1"), []byte("value1_"+dbName), false))
		require.NoError(t, db.Put([]byte("key2"), []byte("value2_"+dbName), true))
	}

	for _, dbName := range dbNames {
		db := p.GetDBHandle(dbName)
		val, err := db.Get([]byte("key1"))
		require.NoError(t, err)
		require.Equal(t, []byte("value1_"+dbName), val)
		val, err = db.Get([]byte("key2"))
		require.NoError(t, err)
		require.Equal(t, []byte("value2_"+dbName), val)
	}

	for _, dbName := range dbNames {
		db := p.GetDBHandle(dbName)
		require.NoError(t, db.Delete([]byte("key1"), false))
		require.NoError(t, db.Delete([]byte("key2"), true))
		val, err := db.Get([]byte("key1"))
		require.NoError(t, err)
		require.Nil(t, val)
		val, err = db.Get([]byte("key2"))
		require.NoError(t, err)
		require.Nil(t, val)
	}
}

// Scenario:
//  1. Put keys 0 to 19 in three handles of one provider.
//  2. Expect the middle handle to hold exactly its own keys and values.
func TestDBHandleKeyRange(t *testing.T) {
	p := newTestProvider(t)
	db1 := p.GetDBHandle("db1")
	db2 := p.GetDBHandle("db2")
	db3 := p.GetDBHandle("db3")
	for i := 0; i < 20; i++ {
		require.NoError(t, db1.Put([]byte(createTestKey(i)), []byte(createTestValue("db1", i)), false))
		require.NoError(t, db2.Put([]byte(createTestKey(i)), []byte(createTestValue("db2", i)), false))
		require.NoError(t, db3.Put([]byte(createTestKey(i)), []byte(createTestValue("db3", i)), false))
	}

	checkHandleEntries(t, db2, createTestKeys(0, 19), createTestValues("db2", 0, 19))
}

// Scenario:
//  1. Write a batch of three keys to each of two handles.
//  2. Write a second batch to each handle that deletes one key.
//  3. Expect the two remaining keys in each handle.
func TestDBHandleBatchedUpdates(t *testing.T) {
	p := newTestProvider(t)
	dbs := []*DBHandle{p.GetDBHandle("db1"), p.GetDBHandle("db2")}

	for _, db := range dbs {
		batch := db.NewUpdateBatch()
		batch.Put([]byte("key1"), []byte("value1"))
		batch.Put([]byte("key2"), []byte("value2"))
		batch.Put([]byte("key3"), []byte("value3"))
		require.NoError(t, db.WriteBatch(batch, true))
	}

	for _, db := range dbs {
		batch := db.NewUpdateBatch()
		batch.Delete([]byte("key2"))
		require.NoError(t, db.WriteBatch(batch, true))
	}

	for _, db := range dbs {
		checkHandleEntries(t, db, []string{"key1", "key3"}, []string{"value1", "value3"})
	}
}

// Scenario:
//  1. Write a batch, and expect writing it again to fail with errBatchWritten and adding to it to panic.
//  2. Reset the batch, put another key and expect the write to succeed.
//  3. Expect both keys to be stored.
func TestDBHandleWriteBatchOnce(t *testing.T) {
	db := newTestProvider(t).GetDBHandle("db1")
	batch := db.NewUpdateBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, db.WriteBatch(batch, true))

	require.ErrorIs(t, db.WriteBatch(batch, true), errBatchWritten)
	require.Panics(t, func() { batch.Put([]byte("key3"), []byte("value3")) })
	require.Equal(t, 0, batch.Len())
	require.Equal(t, 0, batch.Size())

	batch.Reset()
	batch.Put([]byte("key2"), []byte("value2"))
	require.NoError(t, db.WriteBatch(batch, true))

	for _, key := range []string{"key1", "key2"} {
		val, err := db.Get([]byte(key))
		require.NoError(t, err)
		require.NotNil(t, val, key)
	}
}

// Scenario:
//  1. Close the db and write a batch.
//  2. Expect pebble.ErrClosed, and the batch to keep its key.
func TestDBHandleWriteBatchAfterClose(t *testing.T) {
	p := newTestProvider(t)
	db := p.GetDBHandle("db1")
	batch := db.NewUpdateBatch()
	batch.Put([]byte("key"), []byte("value"))

	p.Close()
	require.ErrorIs(t, db.WriteBatch(batch, true), pebble.ErrClosed)
	require.Equal(t, 1, batch.Len())
}

// Scenario:
//  1. Write an empty batch and a nil batch.
//  2. Expect both writes to succeed and the handle to stay empty.
func TestDBHandleWriteEmptyBatch(t *testing.T) {
	db := newTestProvider(t).GetDBHandle("db1")

	require.NoError(t, db.WriteBatch(db.NewUpdateBatch(), true))
	require.NoError(t, db.WriteBatch(nil, true))

	empty, err := db.IsEmpty()
	require.NoError(t, err)
	require.True(t, empty)
}

// Scenario:
//  1. Put keys in two handles.
//  2. Drop the first handle.
//  3. Expect the first handle to be empty and released by the provider, and the second to keep its keys.
//  4. Close the provider and expect deleting the second handle's keys to fail with pebble.ErrClosed.
func TestDrop(t *testing.T) {
	p := newTestProvider(t)
	db1 := p.GetDBHandle("db1")
	db2 := p.GetDBHandle("db2")
	for i := 0; i < 20; i++ {
		require.NoError(t, db1.Put([]byte(createTestKey(i)), []byte(createTestValue("db1", i)), false))
		require.NoError(t, db2.Put([]byte(createTestKey(i)), []byte(createTestValue("db2", i)), false))
	}

	require.NoError(t, p.Drop("db1"))

	require.NotContains(t, p.dbHandles, "db1")
	require.Contains(t, p.dbHandles, "db2")
	checkHandleEntries(t, db1, nil, nil)
	checkHandleEntries(t, db2, createTestKeys(0, 19), createTestValues("db2", 0, 19))

	p.Close()
	require.ErrorIs(t, db2.deleteAll(), pebble.ErrClosed)
}

func TestFormatCheck(t *testing.T) {
	testCases := []struct {
		dataFormat     string
		dataExists     bool
		expectedFormat string
		expectedErr    *dataformat.ErrFormatMismatch
	}{
		{
			dataFormat:     "",
			dataExists:     true,
			expectedFormat: "",
			expectedErr:    nil,
		},
		{
			dataFormat:     "",
			dataExists:     false,
			expectedFormat: "",
			expectedErr:    nil,
		},
		{
			dataFormat:     "",
			dataExists:     false,
			expectedFormat: "2.0",
			expectedErr:    nil,
		},
		{
			dataFormat:     "",
			dataExists:     true,
			expectedFormat: "2.0",
			expectedErr:    &dataformat.ErrFormatMismatch{Format: "", ExpectedFormat: "2.0"},
		},
		{
			dataFormat:     "2.0",
			dataExists:     true,
			expectedFormat: "2.0",
			expectedErr:    nil,
		},
		{
			dataFormat:     "2.0",
			dataExists:     true,
			expectedFormat: "3.0",
			expectedErr:    &dataformat.ErrFormatMismatch{Format: "2.0", ExpectedFormat: "3.0"},
		},
	}

	for i, testCase := range testCases {
		t.Run(
			fmt.Sprintf("testCase %d", i),
			func(t *testing.T) {
				testFormatCheck(t, testCase.dataFormat, testCase.expectedFormat, testCase.dataExists, testCase.expectedErr)
			},
		)
	}
}

// Scenario:
//  1. Get two handles from a provider.
//  2. Close each handle and expect the provider to release it.
func TestClose(t *testing.T) {
	p := newTestProvider(t)
	db1 := p.GetDBHandle("db1")
	db2 := p.GetDBHandle("db2")
	require.Equal(t, map[string]*DBHandle{"db1": db1, "db2": db2}, p.dbHandles)

	db1.Close()
	require.Equal(t, map[string]*DBHandle{"db2": db2}, p.dbHandles)

	db2.Close()
	require.Equal(t, map[string]*DBHandle{}, p.dbHandles)
}

func TestIsEmpty(t *testing.T) {
	setup := func(t *testing.T) (*Provider, *DBHandle, *DBHandle) {
		p := newTestProvider(t)
		return p, p.GetDBHandle("db1"), p.GetDBHandle("db2")
	}

	t.Run("both the dbs are empty", func(t *testing.T) {
		_, db1, db2 := setup(t)

		empty, err := db1.IsEmpty()
		require.NoError(t, err)
		require.True(t, empty)

		empty, err = db2.IsEmpty()
		require.NoError(t, err)
		require.True(t, empty)
	})

	t.Run("only one db is empty", func(t *testing.T) {
		_, db1, db2 := setup(t)

		require.NoError(t, db1.Put([]byte("key"), []byte("value"), false))
		empty, err := db1.IsEmpty()
		require.NoError(t, err)
		require.False(t, empty)

		empty, err = db2.IsEmpty()
		require.NoError(t, err)
		require.True(t, empty)
	})

	t.Run("closed provider", func(t *testing.T) {
		p, db1, _ := setup(t)

		p.Close()
		empty, err := db1.IsEmpty()
		require.ErrorIs(t, err, pebble.ErrClosed)
		require.False(t, empty)
	})
}

func TestRetrieveDataFormatInfo(t *testing.T) {
	t.Run("fresh db provider", func(t *testing.T) {
		dir := t.TempDir()
		p, err := NewProvider(&Conf{DBPath: dir})
		require.NoError(t, err)
		p.Close()

		info, err := RetrieveDataFormatInfo(dir)
		require.NoError(t, err)
		require.Equal(t, &DataFormatInfo{FormatVerison: "", IsDBEmpty: true}, info)
	})

	t.Run("db provider with existing data and version", func(t *testing.T) {
		dir := t.TempDir()
		p, err := NewProvider(&Conf{DBPath: dir})
		require.NoError(t, err)
		require.NoError(t, p.SetDataFormat("2.6"))
		require.NoError(t, p.GetDBHandle("dummy").Put([]byte("k"), []byte("v"), true))
		p.Close()

		info, err := RetrieveDataFormatInfo(dir)
		require.NoError(t, err)
		require.Equal(t, &DataFormatInfo{FormatVerison: "2.6", IsDBEmpty: false}, info)
	})
}

// Scenario:
//  1. Put and delete keys in a batch, then reset it.
//  2. Expect Size and Len to track every operation and to be zero after the reset.
func TestUpdateBatch(t *testing.T) {
	b := &UpdateBatch{
		dbName: "mydb",
		batch:  &pebble.Batch{},
	}

	require.Equal(t, 0, b.Size())
	require.Equal(t, 0, b.Len())

	k := []byte("key")
	v := []byte("value")

	// put a key and verify size and len
	b.Put(k, v)
	expectedSize := len("mydb") + len(dbNameKeySep) + len(k) + len(v)
	require.Equal(t, expectedSize, b.Size())
	require.Equal(t, 1, b.Len())

	// put the key again and verify size and len
	b.Put(k, v)
	expectedSize *= 2
	require.Equal(t, expectedSize, b.Size())
	require.Equal(t, 2, b.Len())

	// delete the key and verify size and len
	b.Delete(k)
	expectedSize += len("mydb") + len(dbNameKeySep) + len(k)
	require.Equal(t, expectedSize, b.Size())
	require.Equal(t, 3, b.Len())

	// reset and verify size and len
	b.Reset()
	require.Equal(t, 0, b.Size())
	require.Equal(t, 0, b.Len())

	// add key again after reset
	b.Put(k, v)
	expectedSize = len("mydb") + len(dbNameKeySep) + len(k) + len(v)
	require.Equal(t, expectedSize, b.Size())
	require.Equal(t, 1, b.Len())
}

func testFormatCheck(t *testing.T, dataFormat, expectedFormat string, dataExists bool, expectedErr *dataformat.ErrFormatMismatch) {
	dir := t.TempDir()

	// setup test pre-conditions (create a db with dbformat)
	p, err := NewProvider(&Conf{DBPath: dir, ExpectedFormat: dataFormat})
	require.NoError(t, err)
	f, err := p.GetDataFormat()
	require.NoError(t, err)
	require.Equal(t, dataFormat, f)
	if dataExists {
		require.NoError(t, p.GetDBHandle("testdb").Put([]byte("key"), []byte("value"), true))
	}

	// close and reopen with new conf
	p.Close()
	p, err = NewProvider(&Conf{DBPath: dir, ExpectedFormat: expectedFormat})
	if expectedErr != nil {
		expectedErr.DBInfo = fmt.Sprintf("pebble db at [%s]", dir)
		require.Equal(t, err, expectedErr)
		return
	}
	require.NoError(t, err)
	defer p.Close()
	f, err = p.GetDataFormat()
	require.NoError(t, err)
	require.Equal(t, expectedFormat, f)
}
