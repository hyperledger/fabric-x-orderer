/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

// newInfoOnlyPruneMgr constructs a pruneMgr for the tests that exercise only the prune info round-trip.
// It has no index, which is enough because only pruneBefore reaches for one.
func newInfoOnlyPruneMgr(t *testing.T, rootDir string) *pruneMgr {
	p, err := newPruneMgr(rootDir, nil)
	require.NoError(t, err)
	return p
}

// blockfileNums returns the block file numbers present in rootDir, ascending.
func blockfileNums(t *testing.T, rootDir string) []int {
	nums, err := blockfileNumsIn(rootDir)
	require.NoError(t, err)
	return nums
}

// appendBlocks appends n blocks to the tail of the ledger and returns them.
func appendBlocks(t *testing.T, mgr *blockfileMgr, n int) []*common.Block {
	appended := make([]*common.Block, 0, n)
	for i := 0; i < n; i++ {
		info := mgr.getBlockchainInfo()
		b := testutil.ConstructTestBlock(t, info.Height, 1, 10)
		b.Header.PreviousHash = info.CurrentBlockHash
		require.NoError(t, mgr.addBlock(b))
		appended = append(appended, b)
	}
	return appended
}

// requirePruneInvariant asserts the readable bound sits at or above the first block of the lowest block
// file the prune info records. Reversed, a read guard would admit a block whose file has been removed.
func requirePruneInvariant(t *testing.T, mgr *blockfileMgr) {
	info := mgr.pruner.info.Load()
	firstInLowest, err := retrieveFirstBlockNumFromFile(mgr.rootDir, info.firstStoredBlockfileNum)
	require.NoError(t, err)
	require.GreaterOrEqual(t, info.firstReadableBlockNum, firstInLowest)
}

// requireBlocksPruned asserts that every block below firstAvailable is reported as pruned and every block
// from firstAvailable up is returned unchanged.
func requireBlocksPruned(t *testing.T, mgr *blockfileMgr, blocks []*common.Block, firstAvailable int) {
	for i := 0; i < firstAvailable; i++ {
		_, err := mgr.retrieveBlockByNumber(uint64(i))
		require.ErrorIs(t, err, ErrPruned)
	}
	for i := firstAvailable; i < len(blocks); i++ {
		got, err := mgr.retrieveBlockByNumber(uint64(i))
		require.NoError(t, err)
		require.Equal(t, blocks[i], got)
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

	p := newInfoOnlyPruneMgr(t, rootDir)
	require.Equal(t, uint64(0), p.firstReadableBlockNum())
	require.Equal(t, 0, p.firstStoredBlockfileNum())

	require.NoError(t, p.setInfo(&pruneInfo{firstReadableBlockNum: 3, firstStoredBlockfileNum: 1}))

	reloaded := newInfoOnlyPruneMgr(t, rootDir)
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
//  1. Store 50 blocks across 5 block files.
//  2. Set prune info at block 20 / file 2 while leaving every block file and index entry in place, the
//     state a prune interrupted before it deleted them leaves behind.
//  3. For a block below the bound, expect retrieveBlockByHash, retrieveBlockByTxID and
//     retrieveTransactionByID to fail with ErrPruned rather than to serve it.
//  4. For a block at or above the bound, expect all three to return it unchanged.
func TestPruneInfoReadGuardsOnHashAndTxIDLookups(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	const firstAvailable = 20
	require.NoError(t, mgr.pruner.setInfo(&pruneInfo{
		firstReadableBlockNum:   firstAvailable,
		firstStoredBlockfileNum: firstAvailable / blocksPerFileForTest,
	}))

	t.Run("a block below the bound is reported as pruned", func(t *testing.T) {
		pruned := blocks[firstAvailable-1]

		_, err := mgr.retrieveBlockByHash(protoutil.BlockHeaderHash(pruned.Header))
		require.ErrorIs(t, err, ErrPruned)
		require.ErrorContains(t, err, "cannot serve a block held in block file [1]")

		txID := txIDOfFirstTx(t, pruned)
		_, err = mgr.retrieveBlockByTxID(txID)
		require.ErrorIs(t, err, ErrPruned)

		_, err = mgr.retrieveTransactionByID(txID)
		require.ErrorIs(t, err, ErrPruned)
	})

	t.Run("a block at the bound is served unchanged", func(t *testing.T) {
		available := blocks[firstAvailable]

		got, err := mgr.retrieveBlockByHash(protoutil.BlockHeaderHash(available.Header))
		require.NoError(t, err)
		require.Equal(t, available, got)

		txID := txIDOfFirstTx(t, available)
		got, err = mgr.retrieveBlockByTxID(txID)
		require.NoError(t, err)
		require.Equal(t, available, got)

		_, err = mgr.retrieveTransactionByID(txID)
		require.NoError(t, err)
	})
}

// txIDOfFirstTx returns the transaction ID of the block's first transaction.
func txIDOfFirstTx(t *testing.T, block *common.Block) string {
	env, err := protoutil.GetEnvelopeFromBlock(block.Data.Data[0])
	require.NoError(t, err)
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	require.NoError(t, err)
	channelHeader, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	require.NoError(t, err)
	require.NotEmpty(t, channelHeader.TxId)
	return channelHeader.TxId
}

// Scenario:
//  1. Construct a pruneMgr over a directory holding no prune store and expect a zero bound.
//  2. Expect no prune store directory to have been created for it.
//  3. Set prune info through it, and expect the store to appear and the bound to reload from it.
func TestPruneMgrCreatesTheStoreOnlyWhenABoundIsSet(t *testing.T) {
	rootDir := t.TempDir()
	storeDir := filepath.Join(rootDir, pruneStoreFileSuffix)

	p := newInfoOnlyPruneMgr(t, rootDir)
	require.Equal(t, uint64(0), p.firstReadableBlockNum())
	require.NoDirExists(t, storeDir)

	require.NoError(t, p.setInfo(&pruneInfo{firstReadableBlockNum: 3, firstStoredBlockfileNum: 1}))
	require.DirExists(t, storeDir)

	reloaded := newInfoOnlyPruneMgr(t, rootDir)
	require.Equal(t, uint64(3), reloaded.firstReadableBlockNum())
	require.Equal(t, 1, reloaded.firstStoredBlockfileNum())
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

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Call pruneBefore(20), a file boundary.
//  3. Expect the prune info to be block 20 / file 2, and block files 0 and 1 to be gone.
//  4. Expect blocks 0..19 to fail with ErrPruned and blocks 20..49 to be returned unchanged.
//  5. Expect Height to stay 50 and an iterator started at block 20 to return block 20.
func TestPruneBeforeOnFileBoundary(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(20))

	require.Equal(t, uint64(20), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 2, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{2, 3, 4}, blockfileNums(t, mgr.rootDir))

	requireBlocksPruned(t, mgr, blocks, 20)
	require.Equal(t, uint64(len(blocks)), mgr.getBlockchainInfo().Height)

	itr, err := mgr.retrieveBlocks(20)
	require.NoError(t, err)
	defer itr.Close()
	got, err := itr.Next()
	require.NoError(t, err)
	require.Equal(t, blocks[20], got)
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Call pruneBefore(25), which falls inside block file 2.
//  3. Expect the readable bound to be exactly 25, and block file 2 to survive whole because it also holds
//     blocks 25..29.
//  4. Expect block 24 to fail with ErrPruned even though its bytes are still on disk, and block 25 to be
//     returned.
func TestPruneBeforeMidFileHidesTheBlocksItKeeps(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(25))

	require.Equal(t, uint64(25), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 2, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{2, 3, 4}, blockfileNums(t, mgr.rootDir))
	requirePruneInvariant(t, mgr)

	_, err := mgr.retrieveBlockByNumber(24)
	require.ErrorIs(t, err, ErrPruned)

	got, err := mgr.retrieveBlockByNumber(25)
	require.NoError(t, err)
	require.Equal(t, blocks[25], got)
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Call pruneBefore(5), which falls inside block file 0.
//  3. Expect the readable bound to be 5 with no file removed and every block file still present.
//  4. Expect blocks 0..4 to fail with ErrPruned and blocks 5..49 to be returned.
func TestPruneBeforeAdvancesBoundWithoutRemovingAFile(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(5))

	require.Equal(t, uint64(5), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 0, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{0, 1, 2, 3, 4}, blockfileNums(t, mgr.rootDir))

	requireBlocksPruned(t, mgr, blocks, 5)
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Call pruneBefore(5), which is below the first file boundary.
//  3. Expect the bound to move to 5 with no file removed.
//  4. Call pruneBefore(20) twice, then pruneBefore(10) and pruneBefore(0).
//  5. Expect the bound to settle at block 20 and never decrease.
func TestPruneBeforeIsIdempotentAndMonotone(t *testing.T) {
	env, w, _ := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(5))
	require.Equal(t, uint64(5), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, []int{0, 1, 2, 3, 4}, blockfileNums(t, mgr.rootDir))

	require.NoError(t, mgr.pruneBefore(20))
	require.Equal(t, uint64(20), mgr.pruner.firstReadableBlockNum())

	for _, blockNum := range []uint64{20, 10, 0} {
		require.NoError(t, mgr.pruneBefore(blockNum))
		require.Equal(t, uint64(20), mgr.pruner.firstReadableBlockNum())
		require.Equal(t, 2, mgr.pruner.firstStoredBlockfileNum())
		require.Equal(t, []int{2, 3, 4}, blockfileNums(t, mgr.rootDir))
	}
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Call pruneBefore(MaxUint64), far past the end of the ledger.
//  3. Expect the bound to be capped at 49, the last block, and block file 4 which holds it to survive.
//  4. Expect block 48 to fail with ErrPruned, block 49 to be returned, and Height to stay 50.
//  5. Call pruneBefore(MaxUint64) again and expect it to change nothing.
//  6. Append one more block and expect it to be readable.
func TestPruneBeforeKeepsFileHoldingLastBlock(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(math.MaxUint64))

	lastBlock := uint64(len(blocks) - 1)
	require.Equal(t, lastBlock, mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 4, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{4}, blockfileNums(t, mgr.rootDir))
	requirePruneInvariant(t, mgr)

	_, err := mgr.retrieveBlockByNumber(lastBlock - 1)
	require.ErrorIs(t, err, ErrPruned)

	got, err := mgr.retrieveBlockByNumber(lastBlock)
	require.NoError(t, err)
	require.Equal(t, blocks[lastBlock], got)
	require.Equal(t, uint64(len(blocks)), mgr.getBlockchainInfo().Height)

	require.NoError(t, mgr.pruneBefore(math.MaxUint64))
	require.Equal(t, lastBlock, mgr.pruner.firstReadableBlockNum())
	require.Equal(t, []int{4}, blockfileNums(t, mgr.rootDir))

	appended := appendBlocks(t, mgr, 1)
	got, err = mgr.retrieveBlockByNumber(uint64(len(blocks)))
	require.NoError(t, err)
	require.Equal(t, appended[0], got)
}

// Scenario:
//  1. Store 50 blocks across 5 block files, then roll over without appending, leaving block file 5 empty.
//  2. Call pruneBefore(MaxUint64).
//  3. Expect block file 4, which holds block 49, and the empty block file 5 to both survive, with the
//     bound capped at 49.
//  4. Close the store and reopen it, and expect block 49 to still be returned.
func TestPruneBeforeWithEmptyActiveFile(t *testing.T) {
	path := t.TempDir()
	env, w, blocks := newPrunableTestLedger(t, path, 50)
	mgr := w.blockfileMgr
	mgr.moveToNextFile()

	require.NoError(t, mgr.pruneBefore(math.MaxUint64))

	require.Equal(t, uint64(len(blocks)-1), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 4, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{4, 5}, blockfileNums(t, mgr.rootDir))
	requirePruneInvariant(t, mgr)
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	got, err := store.RetrieveBlockByNumber(uint64(len(blocks) - 1))
	require.NoError(t, err)
	require.Equal(t, blocks[len(blocks)-1], got)
}

// Scenario:
//  1. Store 50 blocks across 5 block files and call pruneBefore(25), so the readable bound and the lowest
//     block file no longer correspond.
//  2. Close the store and reopen it.
//  3. Expect both to survive independently: bound 25, lowest block file 2.
//  4. Expect Height to stay 50, blocks 0..24 to fail with ErrPruned and blocks 25..49 to be returned.
func TestPruneBeforeSurvivesReopen(t *testing.T) {
	path := t.TempDir()
	env, w, blocks := newPrunableTestLedger(t, path, 50)
	require.NoError(t, w.blockfileMgr.pruneBefore(25))
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	require.Equal(t, uint64(25), store.FirstAvailableBlockNumber())
	require.Equal(t, 2, store.fileMgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{2, 3, 4}, blockfileNums(t, store.fileMgr.rootDir))
	requirePruneInvariant(t, store.fileMgr)

	info, err := store.GetBlockchainInfo()
	require.NoError(t, err)
	require.Equal(t, uint64(len(blocks)), info.Height)

	requireBlocksPruned(t, store.fileMgr, blocks, 25)
}

// Scenario:
//  1. Store 50 blocks across 5 block files and record the index savepoint.
//  2. Call pruneBefore(20).
//  3. Expect the block-number index entries for blocks 0..19 to be gone and those for 20..49 to remain.
//  4. Expect the index savepoint to be unchanged, since it records how far indexing has reached and
//     pruning the front does not move it.
func TestPruneBeforeDeletesIndexEntries(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	savepointBefore, err := mgr.index.getLastBlockIndexed()
	require.NoError(t, err)

	require.NoError(t, mgr.pruneBefore(20))

	for i := 0; i < 20; i++ {
		_, err := mgr.index.getBlockLocByBlockNum(uint64(i))
		require.ErrorContains(t, err, fmt.Sprintf("no such block number [%d] in index", i))
	}
	for i := 20; i < len(blocks); i++ {
		_, err := mgr.index.getBlockLocByBlockNum(uint64(i))
		require.NoError(t, err)
	}

	savepointAfter, err := mgr.index.getLastBlockIndexed()
	require.NoError(t, err)
	require.Equal(t, savepointBefore, savepointAfter)
}

// Scenario:
// 1. Open a ledger and append nothing.
// 2. Call pruneBefore(10).
// 3. Expect no error, the prune info to stay 0, and nothing to be removed.
func TestPruneBeforeOnEmptyLedger(t *testing.T) {
	env := newTestEnv(t, NewConf(t.TempDir(), 0))
	defer env.Cleanup()
	mgr := newTestBlockfileWrapper(env, "testLedger").blockfileMgr

	require.NoError(t, mgr.pruneBefore(10))

	require.Equal(t, uint64(0), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 0, mgr.pruner.firstStoredBlockfileNum())
	require.NoDirExists(t, pruneStoreDir(mgr.rootDir))
}

// Scenario:
//  1. Store 5 blocks, which all fit in block file 0.
//  2. Call pruneBefore(MaxUint64).
//  3. Expect no file to be removed, because the only file holds the last block, and the bound to be
//     capped at block 4.
//  4. Expect blocks 0..3 to fail with ErrPruned and block 4 to be returned.
func TestPruneBeforeWithSingleBlockfile(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 5)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(math.MaxUint64))

	require.Equal(t, uint64(len(blocks)-1), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 0, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{0}, blockfileNums(t, mgr.rootDir))

	requireBlocksPruned(t, mgr, blocks, len(blocks)-1)
}

// Scenario:
//  1. Store a single block, so the only block file holds the last block.
//  2. Call pruneBefore(MaxUint64).
//  3. Expect nothing to change: no file is eligible and the bound cannot pass the last block.
func TestPruneBeforeOnSingleBlockLedger(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 1)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(math.MaxUint64))

	require.Equal(t, uint64(0), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 0, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{0}, blockfileNums(t, mgr.rootDir))

	got, err := mgr.retrieveBlockByNumber(0)
	require.NoError(t, err)
	require.Equal(t, blocks[0], got)
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Call pruneBefore(10), exactly the first block of block file 1.
//  3. Expect only block file 0 to be removed and the bound to be block 10.
//  4. Call pruneBefore(30), exactly the first block of block file 3.
//  5. Expect block files 1 and 2 to be removed and the bound to be block 30.
//  6. Call pruneBefore(40) and expect block file 3 to be removed, leaving only block file 4.
//  7. Expect blocks 0..39 to fail with ErrPruned, blocks 40..49 to be returned, and Height to stay 50.
func TestPruneBeforeAdvancesAcrossSuccessiveCalls(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(10))
	require.Equal(t, uint64(10), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 1, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{1, 2, 3, 4}, blockfileNums(t, mgr.rootDir))

	require.NoError(t, mgr.pruneBefore(30))
	require.Equal(t, uint64(30), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 3, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{3, 4}, blockfileNums(t, mgr.rootDir))

	require.NoError(t, mgr.pruneBefore(40))
	require.Equal(t, uint64(40), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 4, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{4}, blockfileNums(t, mgr.rootDir))

	requireBlocksPruned(t, mgr, blocks, 40)
	require.Equal(t, uint64(len(blocks)), mgr.getBlockchainInfo().Height)
}

// Scenario:
//  1. Store 50 blocks across 5 block files and call pruneBefore(MaxUint64), leaving only block file 4.
//  2. Roll over and append 10 more blocks, so block file 5 holds blocks 50..59.
//  3. Call pruneBefore(MaxUint64) again.
//  4. Expect block file 4 to be removed now that it no longer holds the last block, leaving only block
//     file 5 and a bound of block 59.
//  5. Expect blocks 40..58 to fail with ErrPruned and block 59 to be returned.
func TestPruneBeforeAfterAppendingMoreBlocks(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(math.MaxUint64))
	require.Equal(t, []int{4}, blockfileNums(t, mgr.rootDir))

	mgr.moveToNextFile()
	appended := appendBlocks(t, mgr, blocksPerFileForTest)

	require.NoError(t, mgr.pruneBefore(math.MaxUint64))

	lastBlock := uint64(len(blocks) + len(appended) - 1)
	require.Equal(t, lastBlock, mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 5, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{5}, blockfileNums(t, mgr.rootDir))
	requirePruneInvariant(t, mgr)

	for i := uint64(40); i < lastBlock; i++ {
		_, err := mgr.retrieveBlockByNumber(i)
		require.ErrorIs(t, err, ErrPruned)
	}
	got, err := mgr.retrieveBlockByNumber(lastBlock)
	require.NoError(t, err)
	require.Equal(t, appended[len(appended)-1], got)
}

// Scenario:
//  1. Store 401 blocks across 41 block files, ten blocks per file, so file 40 holds block 400 alone.
//  2. Call pruneBefore(i*100+99) for i = 0..3.
//  3. Expect each call to move the bound to exactly its request, and the lowest surviving file to i*10+9:
//     the file whose last block is the bound keeps its blocks, since pruning removes whole files.
//  4. Expect two files left at the end -- file 39, holding blocks 390..399, and file 40, holding the last
//     block -- with blocks below 399 refused and 399 and 400 returned.
//  5. Call pruneBefore(400) and expect file 39 to go too, leaving one file.
func TestPruneBeforeRepeatedlyDownToTheLastFiles(t *testing.T) {
	const blocks = 401
	env, w, all := newPrunableTestLedger(t, t.TempDir(), blocks)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.Len(t, blockfileNums(t, mgr.rootDir), 41)

	for i := uint64(0); i < 4; i++ {
		bound := i*100 + 99
		require.NoError(t, mgr.pruneBefore(bound))

		require.Equal(t, bound, mgr.pruner.firstReadableBlockNum())
		require.Equal(t, int(i*10+9), mgr.pruner.firstStoredBlockfileNum())
		requirePruneInvariant(t, mgr)
	}

	require.Equal(t, []int{39, 40}, blockfileNums(t, mgr.rootDir))
	require.Equal(t, uint64(399), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, uint64(blocks), mgr.getBlockchainInfo().Height)
	requireBlocksPruned(t, mgr, all, 399)

	// The bound could not pass block 399 while its file held it. Asking for the block after frees that
	// file too, and only the file holding the last block remains.
	require.NoError(t, mgr.pruneBefore(400))

	require.Equal(t, []int{40}, blockfileNums(t, mgr.rootDir))
	require.Equal(t, uint64(400), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 40, mgr.pruner.firstStoredBlockfileNum())
	requireBlocksPruned(t, mgr, all, 400)
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Call pruneBefore(30) from 8 goroutines at once.
//  3. Expect every call to return without error.
//  4. Expect the bound to be block 30 / file 3, exactly the files below it to be gone, and blocks 0..29
//     to fail with ErrPruned.
func TestPruneBeforeConcurrentCalls(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	const callers = 8
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- mgr.pruneBefore(30)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Equal(t, uint64(30), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 3, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{3, 4}, blockfileNums(t, mgr.rootDir))
	requireBlocksPruned(t, mgr, blocks, 30)
}

// Counting the eligible files runs to completion before anything is removed, so a file that cannot be
// read stops the prune with the ledger exactly as it was, however far up the request would have reached.
//
// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Truncate block file 2 to zero bytes, so the probe for the first block above block file 1 fails.
//  3. Call pruneBefore(45), which would otherwise remove block files 0 through 3.
//  4. Expect an error naming block file 2, and no progress at all: the bound is still 0 and every block
//     file, including the two below the damaged one, is still there.
//  5. Call pruneBefore(5), which stops counting below the damaged file.
//  6. Expect it to succeed, since a request that never probes that file is unaffected by it.
func TestPruneBeforeMakesNoProgressWhenALaterFileIsUnreadable(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, os.Truncate(deriveBlockfilePath(mgr.rootDir, 2), 0))

	err := mgr.pruneBefore(45)
	require.ErrorContains(t, err, "cannot determine the first block of block file [2]")

	require.Equal(t, uint64(0), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 0, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{0, 1, 2, 3, 4}, blockfileNums(t, mgr.rootDir))

	require.NoError(t, mgr.pruneBefore(5))

	require.Equal(t, uint64(5), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 0, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{0, 1, 2, 3, 4}, blockfileNums(t, mgr.rootDir))

	for i := 0; i < 5; i++ {
		_, err := mgr.retrieveBlockByNumber(uint64(i))
		require.ErrorIs(t, err, ErrPruned)
	}
	got, err := mgr.retrieveBlockByNumber(5)
	require.NoError(t, err)
	require.Equal(t, blocks[5], got)
}

// Scenario:
//  1. Store 50 blocks across 5 block files, and prune below block 20 so the prune store exists.
//  2. Make the ledger's root directory read-only, so unlinking a block file fails while the prune store,
//     which lives in a subdirectory the process still owns, keeps working.
//  3. Call pruneBefore(40).
//  4. Expect an error naming the block file it could not remove.
//  5. Restore the directory's permissions and expect the file to be there still, recorded as gone.
//  6. Call pruneBefore(40) again and expect the orphan to be swept and the prune to complete.
func TestPruneBeforeUnlinkFailureLeavesRecoverableOrphans(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(20))
	require.Equal(t, []int{2, 3, 4}, blockfileNums(t, mgr.rootDir))

	require.NoError(t, os.Chmod(mgr.rootDir, 0o500))
	err := mgr.pruneBefore(40)
	require.NoError(t, os.Chmod(mgr.rootDir, 0o700))
	require.ErrorContains(t, err, "error removing block file [2]")

	require.Equal(t, uint64(40), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 3, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{2, 3, 4}, blockfileNums(t, mgr.rootDir))

	require.NoError(t, mgr.pruneBefore(40))

	require.Equal(t, uint64(40), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 4, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{4}, blockfileNums(t, mgr.rootDir))
	requirePruneInvariant(t, mgr)
	requireBlocksPruned(t, mgr, blocks, 40)
}

// Scenario:
//  1. Store 50 blocks across 5 block files and prune below block 20, leaving block files 2 through 4.
//  2. Write block files 0 and 1 back, the state an interrupted prune leaves: recorded as gone, still on
//     disk.
//  3. Call pruneBefore(20), which is the bound already in force, so nothing else is eligible.
//  4. Expect the two files to be swept and the prune info to be untouched, since no durable write was
//     needed.
func TestPruneBeforeSweepsFilesLeftBehindByACrash(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(20))
	require.Equal(t, []int{2, 3, 4}, blockfileNums(t, mgr.rootDir))

	for _, fileNum := range []int{0, 1} {
		require.NoError(t, os.WriteFile(deriveBlockfilePath(mgr.rootDir, fileNum), []byte("orphan"), 0o600))
	}
	require.Equal(t, []int{0, 1, 2, 3, 4}, blockfileNums(t, mgr.rootDir))

	require.NoError(t, mgr.pruneBefore(20))

	require.Equal(t, []int{2, 3, 4}, blockfileNums(t, mgr.rootDir))
	require.Equal(t, uint64(20), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 2, mgr.pruner.firstStoredBlockfileNum())
	requireBlocksPruned(t, mgr, blocks, 20)
}

// A real prune deletes index entries, so a reopen after one has to rebuild a tail that starts above the
// blocks that are gone.
//
// Scenario:
//  1. Store 50 blocks across 5 block files and prune below block 20.
//  2. Rewind the index savepoint to block 45, so syncIndex has a tail to rebuild.
//  3. Reopen and expect the rebuild to pick up at block 46 without looking for the pruned files.
//  4. Expect the bound to reload unchanged, blocks 0..19 to fail with ErrPruned, and blocks 20..49 to be
//     returned.
func TestReopenAfterRealPruneWithIndexBehind(t *testing.T) {
	path := t.TempDir()
	env, w, blocks := newPrunableTestLedger(t, path, 50)
	require.NoError(t, w.blockfileMgr.pruneBefore(20))
	rewindIndexSavepoint(t, w.blockfileMgr, 45)
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	require.Equal(t, uint64(20), store.FirstAvailableBlockNumber())
	require.Equal(t, 2, store.fileMgr.pruner.firstStoredBlockfileNum())
	requirePruneInvariant(t, store.fileMgr)

	lastIndexed, err := store.fileMgr.index.getLastBlockIndexed()
	require.NoError(t, err)
	require.Equal(t, uint64(len(blocks)-1), lastIndexed)

	requireBlocksPruned(t, store.fileMgr, blocks, 20)
}

// Scenario:
//  1. Store 50 blocks across 5 block files and prune below block 30.
//  2. Rewind the index savepoint to block 10, a block that no longer exists, so the rebuild's starting
//     point is below the surviving blocks.
//  3. Reopen and expect the rebuild to start from the first surviving block instead of from block 11.
//  4. Expect blocks 30..49 to be returned from the rebuilt index and blocks 0..29 to fail with ErrPruned.
func TestReopenAfterRealPruneWithIndexBelowTheBound(t *testing.T) {
	path := t.TempDir()
	env, w, blocks := newPrunableTestLedger(t, path, 50)
	require.NoError(t, w.blockfileMgr.pruneBefore(30))
	rewindIndexSavepoint(t, w.blockfileMgr, 10)
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	require.Equal(t, uint64(30), store.FirstAvailableBlockNumber())
	require.Equal(t, 3, store.fileMgr.pruner.firstStoredBlockfileNum())

	lastIndexed, err := store.fileMgr.index.getLastBlockIndexed()
	require.NoError(t, err)
	require.Equal(t, uint64(len(blocks)-1), lastIndexed)

	requireBlocksPruned(t, store.fileMgr, blocks, 30)
}

// The read guards report ErrPruned; the index lookups they guard report their own absence. A caller that
// reaches the index directly must therefore not see ErrPruned, so that a missing entry is never mistaken
// for a pruned block.
//
// Scenario:
//  1. Store 50 blocks across 5 block files and prune below block 20.
//  2. Look up a pruned block's number, hash and <block, tx> tuple directly in the index.
//  3. Expect each to report no such entry, and none of them to report ErrPruned.
func TestPruneBeforeUnguardedLookupsDoNotReportErrPruned(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruneBefore(20))

	pruned := blocks[5]

	_, err := mgr.index.getBlockLocByBlockNum(5)
	require.ErrorContains(t, err, "no such block number [5] in index")
	require.NotErrorIs(t, err, ErrPruned)

	hash := protoutil.BlockHeaderHash(pruned.Header)
	_, err = mgr.index.getBlockLocByHash(hash)
	require.ErrorContains(t, err, fmt.Sprintf("no such block hash [%x] in index", hash))
	require.NotErrorIs(t, err, ErrPruned)

	_, err = mgr.index.getTXLocByBlockNumTranNum(5, 0)
	require.ErrorContains(t, err, "no such blockNumber, transactionNumber <5, 0> in index")
	require.NotErrorIs(t, err, ErrPruned)
}

// A reader can pass the availability check, resolve a block, and only then have the prune it was racing
// delete the index entry or unlink the file underneath it. Because the bound is made durable before either
// removal begins, the failed read can always be attributed: the second check sees the bound that overtook
// it, and the caller hears that the block was pruned rather than that a file is missing.
//
// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Read every block by number, and its header, from 4 goroutines while a fifth prunes the bound up in
//     four steps.
//  3. Expect every read to either return the block unchanged or fail with ErrPruned, and none to surface
//     a missing index entry or a missing block file.
func TestPruneBeforeConcurrentReadsNeverSurfaceRemovalDebris(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	const readers = 4
	unexpected := make(chan error, 2*readers*len(blocks))
	done := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				for num, want := range blocks {
					got, err := mgr.retrieveBlockByNumber(uint64(num))
					switch {
					case err == nil && !proto.Equal(got, want):
						unexpected <- fmt.Errorf("block [%d] came back changed", num)
					case err != nil && !errors.Is(err, ErrPruned):
						unexpected <- err
					}

					header, err := mgr.retrieveBlockHeaderByNumber(uint64(num))
					switch {
					case err == nil && !proto.Equal(header, want.Header):
						unexpected <- fmt.Errorf("header of block [%d] came back changed", num)
					case err != nil && !errors.Is(err, ErrPruned):
						unexpected <- err
					}
				}
			}
		}()
	}

	for _, bound := range []uint64{10, 20, 30, 40} {
		require.NoError(t, mgr.pruneBefore(bound))
	}
	close(done)
	wg.Wait()
	close(unexpected)

	for err := range unexpected {
		require.NoError(t, err)
	}

	require.Equal(t, uint64(40), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, []int{4}, blockfileNums(t, mgr.rootDir))
}

// The concurrent case above asserts the invariant but cannot pin the window it depends on, so the
// attribution itself is exercised directly: it is what turns the debris of a prune a read lost the race to
// into ErrPruned, and it must leave every other failure alone.
//
// Scenario:
//  1. Store 50 blocks and set the bound to block 20, the state a prune publishes before it removes
//     anything.
//  2. Attribute a read failure for a block below the bound and expect ErrPruned, with the original error
//     dropped.
//  3. Attribute the same failure for the block at the bound, and for one above it, and expect the original
//     error back unchanged both times.
func TestErrAfterFailedReadAttributesOnlyPrunedBlocks(t *testing.T) {
	env, w, _ := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	const firstAvailable = 20
	require.NoError(t, mgr.pruner.setInfo(&pruneInfo{
		firstReadableBlockNum:   firstAvailable,
		firstStoredBlockfileNum: firstAvailable / blocksPerFileForTest,
	}))

	lost := errors.New("no such block number [19] in index")

	err := mgr.errAfterFailedRead(firstAvailable-1, lost)
	require.ErrorIs(t, err, ErrPruned)
	require.NotErrorIs(t, err, lost)

	for _, blockNum := range []uint64{firstAvailable, firstAvailable + 1} {
		err := mgr.errAfterFailedRead(blockNum, lost)
		require.ErrorIs(t, err, lost)
		require.NotErrorIs(t, err, ErrPruned)
	}
}
