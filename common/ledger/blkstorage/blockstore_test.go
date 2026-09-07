/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/testutil"

	"github.com/stretchr/testify/require"
)

func TestWrongBlockNumber(t *testing.T) {
	env := newTestEnv(t, NewConf(t.TempDir(), 0))
	defer env.Cleanup()

	provider := env.provider
	store, _ := provider.Open("testLedger")
	defer store.Shutdown()

	blocks := testutil.ConstructTestBlocks(t, 5)
	for i := 0; i < 3; i++ {
		err := store.AddBlock(blocks[i])
		require.NoError(t, err)
	}
	err := store.AddBlock(blocks[4])
	require.Error(t, err, "Error shold have been thrown when adding block number 4 while block number 3 is expected")
}

func TestBlockNumIndexErrorPropagation(t *testing.T) {
	// Scenario:
	// 1. Open a store and add three blocks.
	// 2. Close the underlying leveldb provider under the store.
	// 3. Retrieve a block by number and expect the leveldb failure to surface.
	env := newTestEnv(t, NewConf(t.TempDir(), 0))
	defer env.Cleanup()

	provider := env.provider
	store, err := provider.Open("testLedger")
	require.NoError(t, err)
	defer store.Shutdown()

	blocks := testutil.ConstructTestBlocks(t, 3)
	for _, b := range blocks {
		require.NoError(t, store.AddBlock(b))
	}

	env.provider.leveldbProvider.Close()

	block, err := store.RetrieveBlockByNumber(1)
	require.Nil(t, block)
	require.ErrorContains(t, err, "leveldb: closed")
}
