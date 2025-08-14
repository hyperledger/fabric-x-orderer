/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package badb

import (
	"os"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/assert"
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
