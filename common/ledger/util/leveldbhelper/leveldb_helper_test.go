/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package leveldbhelper

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Create a db and do not open it.
//  2. Expect Get, Put, Delete, IsEmpty, WriteBatch and DeleteRange to fail with pebble.ErrClosed.
func TestDBNeverOpened(t *testing.T) {
	db := CreateDB(&Conf{DBPath: t.TempDir()})
	requireClosed(t, db)
}

// Scenario:
//  1. Open a db twice, write a key and close the db twice.
//  2. Expect every operation to fail with pebble.ErrClosed.
//  3. Open the db again and expect the key to be read back.
func TestDBOpenCloseReopen(t *testing.T) {
	db := CreateDB(&Conf{DBPath: t.TempDir()})
	db.Open()
	db.Open()
	require.NoError(t, db.Put([]byte("key"), []byte("value"), true))

	db.Close()
	db.Close()
	requireClosed(t, db)

	db.Open()
	defer db.Close()
	value, err := db.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}

// Scenario:
//  1. Write keys a, b, c and d.
//  2. Delete the range [b, d).
//  3. Expect only a and d to remain.
func TestDBDeleteRange(t *testing.T) {
	db := CreateDB(&Conf{DBPath: t.TempDir()})
	db.Open()
	defer db.Close()
	for _, k := range []string{"a", "b", "c", "d"} {
		require.NoError(t, db.Put([]byte(k), []byte("v"), false))
	}

	require.NoError(t, db.DeleteRange([]byte("b"), []byte("d"), true))

	itr, err := db.db.NewIter(nil)
	require.NoError(t, err)
	var keys []string
	for itr.First(); itr.Valid(); itr.Next() {
		keys = append(keys, string(itr.Key()))
	}
	require.NoError(t, itr.Close())
	require.Equal(t, []string{"a", "d"}, keys)
}

// Scenario:
//  1. Open a db in an existing empty directory.
//  2. Expect no panic.
func TestCreateDBInEmptyDir(t *testing.T) {
	db := CreateDB(&Conf{DBPath: t.TempDir()})
	defer db.Close()
	require.NotPanics(t, db.Open)
}

// Scenario:
//  1. Put a file that is not a db in a directory, and open a db there.
//  2. Expect a panic.
func TestCreateDBInNonEmptyDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "dummyfile.txt"), nil, 0o600))
	db := CreateDB(&Conf{DBPath: dir})
	defer db.Close()
	require.Panics(t, db.Open)
}

// requireClosed expects every operation on db to fail with pebble.ErrClosed.
func requireClosed(t *testing.T, db *DB) {
	_, err := db.Get([]byte("key"))
	require.ErrorIs(t, err, pebble.ErrClosed)
	require.ErrorIs(t, db.Put([]byte("key"), []byte("value"), false), pebble.ErrClosed)
	require.ErrorIs(t, db.Delete([]byte("key"), false), pebble.ErrClosed)
	_, err = db.IsEmpty()
	require.ErrorIs(t, err, pebble.ErrClosed)
	require.ErrorIs(t, db.WriteBatch(&pebble.Batch{}, true), pebble.ErrClosed)
	require.ErrorIs(t, db.DeleteRange([]byte("a"), []byte("b"), true), pebble.ErrClosed)
}
