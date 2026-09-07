/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/testutil"

	"github.com/stretchr/testify/require"
)

func TestBlockIndexSync(t *testing.T) {
	testBlockIndexSync(t, 10, 5, false)
	testBlockIndexSync(t, 10, 5, true)
	testBlockIndexSync(t, 10, 0, true)
	testBlockIndexSync(t, 10, 10, true)
}

func testBlockIndexSync(t *testing.T, numBlocks int, numBlocksToIndex int, syncByRestart bool) {
	testName := fmt.Sprintf("%v/%v/%v", numBlocks, numBlocksToIndex, syncByRestart)
	t.Run(testName, func(t *testing.T) {
		env := newTestEnv(t, NewConf(t.TempDir(), 0))
		defer env.Cleanup()
		ledgerid := "testledger"
		blkfileMgrWrapper := newTestBlockfileWrapper(env, ledgerid)
		defer blkfileMgrWrapper.close()
		blkfileMgr := blkfileMgrWrapper.blockfileMgr
		originalIndexStore := blkfileMgr.index.db
		// construct blocks for testing
		blocks := testutil.ConstructTestBlocks(t, numBlocks)
		// add a few blocks
		blkfileMgrWrapper.addBlocks(blocks[:numBlocksToIndex])

		// redirect index writes to some random place and add remaining blocks
		blkfileMgr.index.db = env.provider.leveldbProvider.GetDBHandle("someRandomPlace")
		blkfileMgrWrapper.addBlocks(blocks[numBlocksToIndex:])

		// Plug-in back the original index store
		blkfileMgr.index.db = originalIndexStore
		// Verify that the first set of blocks are indexed in the original index
		for i := 0; i < numBlocksToIndex; i++ {
			block, err := blkfileMgr.retrieveBlockByNumber(uint64(i))
			require.NoError(t, err, "block [%d] should have been present in the index", i)
			require.Equal(t, blocks[i], block)
		}

		// Before, we test for index sync-up, verify that the last set of blocks not indexed in the original index
		for i := numBlocksToIndex + 1; i <= numBlocks; i++ {
			_, err := blkfileMgr.retrieveBlockByNumber(uint64(i))
			require.EqualError(t, err, fmt.Sprintf("no such block number [%d] in index", i))
		}

		// perform index sync
		if syncByRestart {
			blkfileMgrWrapper.close()
			blkfileMgrWrapper = newTestBlockfileWrapper(env, ledgerid)
			defer blkfileMgrWrapper.close()
			blkfileMgr = blkfileMgrWrapper.blockfileMgr
		} else {
			blkfileMgr.syncIndex()
		}

		// Now, last set of blocks should also be indexed in the original index
		for i := numBlocksToIndex; i < numBlocks; i++ {
			block, err := blkfileMgr.retrieveBlockByNumber(uint64(i))
			require.NoError(t, err, "block [%d] should have been present in the index", i)
			require.Equal(t, blocks[i], block)
		}
	})
}
