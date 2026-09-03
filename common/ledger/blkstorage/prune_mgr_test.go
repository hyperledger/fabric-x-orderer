/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/stretchr/testify/require"
)

// blocksPerFileForTest is the number of blocks written per block file by newPrunableTestLedger.
const blocksPerFileForTest = 10

// newPrunableTestLedger stores numBlocks blocks with exactly blocksPerFileForTest blocks per block file,
// so that blockNum / blocksPerFileForTest is that block's file number and every file starts on a multiple
// of blocksPerFileForTest. The layout is asserted, not assumed, because the tests derive a prune point's
// file number from its block number.
func newPrunableTestLedger(
	t *testing.T, path string, numBlocks int,
) (*testEnv, *testBlockfileMgrWrapper, []*common.Block) {
	blocks := testutil.ConstructTestBlocks(t, numBlocks)
	env := newTestEnv(t, NewConf(path, 0))
	w := newTestBlockfileWrapper(env, "testLedger")
	for i, b := range blocks {
		if i != 0 && i%blocksPerFileForTest == 0 {
			w.blockfileMgr.moveToNextFile()
		}
		require.NoError(t, w.blockfileMgr.addBlock(b))
	}

	lastFileNum := (numBlocks - 1) / blocksPerFileForTest
	require.Equal(t, lastFileNum, w.blockfileMgr.blockfilesInfo.latestFileNumber)
	for f := 0; f <= lastFileNum; f++ {
		firstInFile, err := retrieveFirstBlockNumFromFile(w.blockfileMgr.rootDir, f)
		require.NoError(t, err)
		require.Equal(t, uint64(f*blocksPerFileForTest), firstInFile)
	}

	return env, w, blocks
}

// pruneFilesUpTo advances the prune info to the first block of firstStoredBlockfileNum and unlinks every
// block file below it, producing the on-disk state that pruning leaves behind.
func pruneFilesUpTo(t *testing.T, mgr *blockfileMgr, firstStoredBlockfileNum int) {
	// Pruning never removes the file holding lastPersistedBlock, so neither may this fixture.
	require.LessOrEqual(t, uint64(firstStoredBlockfileNum)*blocksPerFileForTest, mgr.blockfilesInfo.lastPersistedBlock,
		"fixture would remove the file holding lastPersistedBlock")

	require.NoError(t, mgr.pruner.setInfo(&pruneInfo{
		firstReadableBlockNum:   uint64(firstStoredBlockfileNum * blocksPerFileForTest),
		firstStoredBlockfileNum: firstStoredBlockfileNum,
	}))
	for f := 0; f < firstStoredBlockfileNum; f++ {
		require.NoError(t, os.Remove(deriveBlockfilePath(mgr.rootDir, f)))
	}
}

// rewindIndexSavepoint sets the index savepoint back to lastIndexedBlock, leaving the index behind the
// block files. syncIndex rebuilds only in that state.
func rewindIndexSavepoint(t *testing.T, mgr *blockfileMgr, lastIndexedBlock uint64) {
	require.NoError(t, mgr.db.Put(indexSavePointKey, encodeBlockNum(lastIndexedBlock), true))
}

// Scenario:
//  1. Store 50 blocks across 5 block files, writing no prune info.
//  2. Expect the readable bound and the lowest block file to be 0.
//  3. Expect every block, including block 0, to be retrievable by number, and MaxUint64 to return the last.
//  4. Expect block 0 to pass the availability check.
func TestPruneInfoAbsentByDefault(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.Equal(t, uint64(0), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 0, mgr.pruner.firstStoredBlockfileNum())

	// Every block, including block 0, is still served (also covers the MaxUint64 "newest" alias).
	w.testGetBlockByNumber(blocks)
	require.NoError(t, mgr.checkBlockAvailable(0))
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Set prune info at block 20 / file 2, leaving every block file in place.
//  3. Expect the accessors to report it and Height to stay 50.
//  4. For blocks 0, 1 and 19, expect retrieveBlockByNumber, retrieveBlockHeaderByNumber, retrieveBlocks
//     and retrieveTransactionByBlockNumTranNum to fail with ErrPruned.
//  5. For blocks 20..49, expect those paths to succeed and return the original blocks, and MaxUint64 to
//     return the last block.
//  6. Expect an iterator started at block 20 to return block 20.
func TestPruneInfoReadGuards(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	const firstAvailable = 20
	require.NoError(t, mgr.pruner.setInfo(&pruneInfo{
		firstReadableBlockNum:   firstAvailable,
		firstStoredBlockfileNum: firstAvailable / blocksPerFileForTest,
	}))

	require.Equal(t, uint64(firstAvailable), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 2, mgr.pruner.firstStoredBlockfileNum())

	// Height is about the tail and must be untouched by a prune.
	require.Equal(t, uint64(len(blocks)), mgr.getBlockchainInfo().Height)

	t.Run("blocks below the bound are reported as pruned", func(t *testing.T) {
		for _, blockNum := range []uint64{0, 1, firstAvailable - 1} {
			_, err := mgr.retrieveBlockByNumber(blockNum)
			require.ErrorIs(t, err, ErrPruned)
			require.ErrorContains(t, err,
				fmt.Sprintf("cannot serve block [%d]. First available block = [%d]", blockNum, firstAvailable))

			_, err = mgr.retrieveBlockHeaderByNumber(blockNum)
			require.ErrorIs(t, err, ErrPruned)

			_, err = mgr.retrieveBlocks(blockNum)
			require.ErrorIs(t, err, ErrPruned)

			_, err = mgr.retrieveTransactionByBlockNumTranNum(blockNum, 0)
			require.ErrorIs(t, err, ErrPruned)
		}
	})

	t.Run("blocks at or above the bound are served unchanged", func(t *testing.T) {
		// Also covers the MaxUint64 "give me the newest" alias, which must stay reachable.
		w.testGetBlockByNumber(blocks[firstAvailable:])

		header, err := mgr.retrieveBlockHeaderByNumber(firstAvailable)
		require.NoError(t, err)
		require.Equal(t, blocks[firstAvailable].Header, header)

		_, err = mgr.retrieveTransactionByBlockNumTranNum(firstAvailable, 0)
		require.NoError(t, err)
	})

	t.Run("an iterator may start at the bound", func(t *testing.T) {
		itr, err := mgr.retrieveBlocks(firstAvailable)
		require.NoError(t, err)
		defer itr.Close()

		got, err := itr.Next()
		require.NoError(t, err)
		require.Equal(t, blocks[firstAvailable], got)
	})
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Set the readable bound to block 30 while leaving every block file in place, the state a crash
//     between publishing a bound and unlinking the blocks below it leaves behind.
//  3. Close the store and reopen it.
//  4. Expect the bound to be reported back, the first stored block file to still be 0, and Height to
//     stay 50.
//  5. Expect block 29 to fail with ErrPruned and block 30 to be returned.
func TestPruneInfoSurvivesCloseAndReopen(t *testing.T) {
	path := t.TempDir()
	const firstAvailable = 30

	env, w, blocks := newPrunableTestLedger(t, path, 50)
	require.NoError(t, w.blockfileMgr.pruner.setInfo(&pruneInfo{
		firstReadableBlockNum: firstAvailable,
	}))
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	require.Equal(t, uint64(firstAvailable), store.FirstAvailableBlockNumber())
	require.Equal(t, 0, store.fileMgr.pruner.firstStoredBlockfileNum())

	info, err := store.GetBlockchainInfo()
	require.NoError(t, err)
	require.Equal(t, uint64(len(blocks)), info.Height)

	_, err = store.RetrieveBlockByNumber(firstAvailable - 1)
	require.ErrorIs(t, err, ErrPruned)

	got, err := store.RetrieveBlockByNumber(firstAvailable)
	require.NoError(t, err)
	require.Equal(t, blocks[firstAvailable], got)
}

// Scenario:
//  1. Construct a pruneMgr over a ledger's root directory and expect a zero-valued record.
//  2. Set prune info through it.
//  3. Construct a second pruneMgr over the same directory and expect it to load that record.
func TestPruneMgrLoadsThePersistedInfo(t *testing.T) {
	rootDir := t.TempDir()

	p, err := newPruneMgr(rootDir)
	require.NoError(t, err)
	require.Equal(t, uint64(0), p.firstReadableBlockNum())
	require.Equal(t, 0, p.firstStoredBlockfileNum())

	require.NoError(t, p.setInfo(&pruneInfo{firstReadableBlockNum: 3, firstStoredBlockfileNum: 1}))

	reloaded, err := newPruneMgr(rootDir)
	require.NoError(t, err)
	require.Equal(t, uint64(3), reloaded.firstReadableBlockNum())
	require.Equal(t, 1, reloaded.firstStoredBlockfileNum())
}

// The index database is disposable by contract: delete it and it is rebuilt by scanning the block files.
// The readable bound is the one thing that cannot be recovered that way, which is why it lives in a file of
// its own beside the blocks rather than in the index.
//
// Scenario:
//  1. Store 50 blocks across 5 block files, prune away files 0 and 1, and move the readable bound to
//     block 25, which sits inside the oldest surviving file.
//  2. Close the store and delete the whole index directory.
//  3. Reopen, which rebuilds the index by scanning the surviving block files.
//  4. Expect the readable bound to be reported back unchanged, and blocks 0 and 24 to still fail with
//     ErrPruned rather than to have become readable again.
//  5. Expect block 25 and the last block to be served from the rebuilt index.
func TestPruneInfoSurvivesIndexRebuild(t *testing.T) {
	path := t.TempDir()
	const firstAvailable = 25

	env, w, blocks := newPrunableTestLedger(t, path, 50)
	pruneFilesUpTo(t, w.blockfileMgr, firstAvailable/blocksPerFileForTest)
	require.NoError(t, w.blockfileMgr.pruner.setInfo(&pruneInfo{
		firstReadableBlockNum:   firstAvailable,
		firstStoredBlockfileNum: firstAvailable / blocksPerFileForTest,
	}))
	env.provider.Close()

	require.NoError(t, os.RemoveAll(filepath.Join(path, IndexDir)))

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	require.Equal(t, uint64(firstAvailable), store.FirstAvailableBlockNumber())
	require.Equal(t, 2, store.fileMgr.pruner.firstStoredBlockfileNum())

	for _, blockNum := range []uint64{0, firstAvailable - 1} {
		_, err = store.RetrieveBlockByNumber(blockNum)
		require.ErrorIs(t, err, ErrPruned)
	}

	for _, blockNum := range []uint64{firstAvailable, uint64(len(blocks) - 1)} {
		got, err := store.RetrieveBlockByNumber(blockNum)
		require.NoError(t, err)
		require.Equal(t, blocks[blockNum], got)
	}
}

// Scenario:
// 1. Store 50 blocks across 5 block files.
// 2. Write prune info at block 20 / file 2 and delete block files 0 and 1.
// 3. Rewind the index savepoint to block 45, so syncIndex has a tail to rebuild.
// 4. Close the store and reopen it.
// 5. Expect the open to succeed, the prune info to be reported back, and Height to stay 50.
// 6. Expect block 19 to fail with ErrPruned and blocks 20..49 to be returned.
// 7. Append one more block and expect Height to become 51.
func TestReopenPrunedLedgerRebuildsIndexFromSurvivingFiles(t *testing.T) {
	path := t.TempDir()
	const firstAvailableFile = 2
	const firstAvailable = firstAvailableFile * blocksPerFileForTest

	env, w, blocks := newPrunableTestLedger(t, path, 50)
	pruneFilesUpTo(t, w.blockfileMgr, firstAvailableFile)
	rewindIndexSavepoint(t, w.blockfileMgr, uint64(len(blocks)-5))
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	require.Equal(t, uint64(firstAvailable), store.FirstAvailableBlockNumber())
	require.Equal(t, firstAvailableFile, store.fileMgr.pruner.firstStoredBlockfileNum())

	info, err := store.GetBlockchainInfo()
	require.NoError(t, err)
	require.Equal(t, uint64(len(blocks)), info.Height)

	_, err = store.RetrieveBlockByNumber(firstAvailable - 1)
	require.ErrorIs(t, err, ErrPruned)

	for i := firstAvailable; i < len(blocks); i++ {
		got, err := store.RetrieveBlockByNumber(uint64(i))
		require.NoError(t, err)
		require.Equal(t, blocks[i], got)
	}

	next := testutil.ConstructTestBlock(t, info.Height, 1, 10)
	next.Header.PreviousHash = info.CurrentBlockHash
	require.NoError(t, store.AddBlock(next))
	require.Equal(t, uint64(len(blocks)+1), store.fileMgr.getBlockchainInfo().Height)
}

// Scenario:
// 1. Store 50 blocks across 5 block files.
// 2. Write prune info at block 20 / file 2 and delete block files 0 and 1.
// 3. Rewind the index savepoint to block 5, below the readable bound.
// 4. Close the store and reopen it.
// 5. Expect the open to succeed and blocks 20..49 to be returned.
// 6. Expect block 19 to fail with ErrPruned.
func TestReopenPrunedLedgerWithIndexBehindPruneFrontier(t *testing.T) {
	path := t.TempDir()
	const firstAvailableFile = 2
	const firstAvailable = firstAvailableFile * blocksPerFileForTest

	env, w, blocks := newPrunableTestLedger(t, path, 50)
	pruneFilesUpTo(t, w.blockfileMgr, firstAvailableFile)
	rewindIndexSavepoint(t, w.blockfileMgr, 5)
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	for i := firstAvailable; i < len(blocks); i++ {
		got, err := store.RetrieveBlockByNumber(uint64(i))
		require.NoError(t, err)
		require.Equal(t, blocks[i], got)
	}
	_, err = store.RetrieveBlockByNumber(firstAvailable - 1)
	require.ErrorIs(t, err, ErrPruned)
}

// Scenario:
// 1. Store 50 blocks across 5 block files.
// 2. Write prune info at block 20 / file 2 and delete block files 0 and 1.
// 3. Roll the prune info back to block 10 / file 1, which has already been deleted.
// 4. Rewind the index savepoint to block 45 and close the store.
// 5. Expect reopening to fail with an error naming the missing block file and the prune point.
func TestOpenPrunedLedgerWithStalePruneInfo(t *testing.T) {
	path := t.TempDir()

	env, w, blocks := newPrunableTestLedger(t, path, 50)
	pruneFilesUpTo(t, w.blockfileMgr, 2)
	require.NoError(t, w.blockfileMgr.pruner.setInfo(&pruneInfo{
		firstReadableBlockNum:   blocksPerFileForTest,
		firstStoredBlockfileNum: 1,
	}))
	rewindIndexSavepoint(t, w.blockfileMgr, uint64(len(blocks)-5))
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	_, err := reopened.provider.Open("testLedger")
	require.ErrorContains(t, err, "block file [1] is missing")
	require.ErrorContains(t, err, "pruned up to block [10]")
}

// Scenario:
// 1. Store 50 blocks across 5 block files.
// 2. Write prune info at block 20 / file 2 and delete block files 0 and 1.
// 3. Rewind the index savepoint to block 45 and close the store.
// 4. Delete the prune store, so nothing records how far the ledger was pruned.
// 5. Expect reopening to fail naming the lost prune info rather than a prune point of zero.
func TestOpenPrunedLedgerWithLostPruneInfo(t *testing.T) {
	path := t.TempDir()

	env, w, blocks := newPrunableTestLedger(t, path, 50)
	rootDir := w.blockfileMgr.rootDir
	pruneFilesUpTo(t, w.blockfileMgr, 2)
	rewindIndexSavepoint(t, w.blockfileMgr, uint64(len(blocks)-5))
	env.provider.Close()
	require.NoError(t, os.RemoveAll(pruneStoreDir(rootDir)))

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	_, err := reopened.provider.Open("testLedger")
	require.ErrorContains(t, err, "no prune info was found")
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Publish a bound at block 20 / file 2 without unlinking files 0 and 1, the state a crash between
//     publishing prune info and removing the blocks below it leaves behind.
//  3. Rewind the index savepoint to block 45 and close the store.
//  4. Expect reopening to succeed and blocks 20..49 to be served from the rebuilt tail.
//  5. Expect block 19 to fail with ErrPruned even though its bytes are still on disk.
//  6. Expect the block files below the recorded first one to be left for the next prune to remove.
func TestOpenPrunedLedgerWithOrphanBlockFiles(t *testing.T) {
	path := t.TempDir()
	const firstAvailableFile = 2
	const firstAvailable = firstAvailableFile * blocksPerFileForTest

	env, w, blocks := newPrunableTestLedger(t, path, 50)
	rootDir := w.blockfileMgr.rootDir
	require.NoError(t, w.blockfileMgr.pruner.setInfo(&pruneInfo{
		firstReadableBlockNum:   firstAvailable,
		firstStoredBlockfileNum: firstAvailableFile,
	}))
	rewindIndexSavepoint(t, w.blockfileMgr, uint64(len(blocks)-5))
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	require.Equal(t, uint64(firstAvailable), store.FirstAvailableBlockNumber())
	for i := firstAvailable; i < len(blocks); i++ {
		got, err := store.RetrieveBlockByNumber(uint64(i))
		require.NoError(t, err)
		require.Equal(t, blocks[i], got)
	}

	_, err = store.RetrieveBlockByNumber(firstAvailable - 1)
	require.ErrorIs(t, err, ErrPruned)

	for f := 0; f < firstAvailableFile; f++ {
		require.FileExists(t, deriveBlockfilePath(rootDir, f))
	}
}
