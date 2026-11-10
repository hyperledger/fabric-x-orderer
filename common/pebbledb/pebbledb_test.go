/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pebbledb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/stretchr/testify/require"
)

// testLogger returns the logger the tests open databases with. testutil cannot be used here: it imports
// the ledger, which imports this package.
func testLogger(t *testing.T) *flogging.FabricLogger {
	return flogging.MustGetLogger("pebbledb." + t.Name())
}

// Scenario:
//  1. Open a database in an empty directory, passing options with an older format.
//  2. Expect the database to be created at FormatMajorVersion.
//  3. Expect the passed options to be left unchanged.
func TestOpenPinsFormat(t *testing.T) {
	opts := &pebble.Options{FormatMajorVersion: pebble.FormatMinSupported}

	db, err := Open(t.TempDir(), opts, testLogger(t))
	require.NoError(t, err)
	defer db.Close()

	require.Equal(t, FormatMajorVersion, db.FormatMajorVersion())
	require.Equal(t, pebble.FormatMinSupported, opts.FormatMajorVersion)
	require.Nil(t, opts.Logger)
}

// Scenario:
//  1. Write a key, close the database and reopen it with nil options.
//  2. Expect the key to be read back.
func TestOpenReopens(t *testing.T) {
	dir := t.TempDir()
	logger := testLogger(t)

	db, err := Open(dir, nil, logger)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("k"), []byte("v"), pebble.Sync))
	require.NoError(t, db.Close())

	db, err = Open(dir, nil, logger)
	require.NoError(t, err)
	defer db.Close()
	value, closer, err := db.Get([]byte("k"))
	require.NoError(t, err)
	defer closer.Close()
	require.Equal(t, []byte("v"), value)
}

// Scenario:
//  1. Open a database and, while it is open, open it again.
//  2. Expect the second open to fail and to name the path.
func TestOpenLockedDirectoryFails(t *testing.T) {
	dir := t.TempDir()
	logger := testLogger(t)

	db, err := Open(dir, nil, logger)
	require.NoError(t, err)
	defer db.Close()

	_, err = Open(dir, nil, logger)
	require.ErrorContains(t, err, dir)
}

// Scenario:
//  1. Put a file that is not a database in a directory, and open a database there.
//  2. Expect the open to fail.
func TestOpenNotADatabaseFails(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("MANIFEST-000000\n"), 0o600))

	_, err := Open(dir, nil, testLogger(t))
	require.Error(t, err)
}

// Scenario:
//  1. Open a database in a directory that does not exist yet.
//  2. Expect the database to be created.
func TestOpenCreatesMissingDirectory(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "a", "b"), nil, testLogger(t))
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

// Scenario:
//  1. Open a database with a nil logger, write a key and close it.
//  2. Expect no panic.
func TestOpenNilLogger(t *testing.T) {
	db, err := Open(t.TempDir(), nil, nil)
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("k"), []byte("v"), pebble.Sync))
	require.NoError(t, db.Close())
}

// Scenario:
//  1. Call Fatalf on the logger Open gives Pebble.
//  2. Expect a panic, not a process exit.
func TestOpenLoggerPanicsOnFatal(t *testing.T) {
	logger := panicOnFatal{testLogger(t)}

	require.Panics(t, func() {
		logger.Fatalf("injected fatal error")
	})
}
