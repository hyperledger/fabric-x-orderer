/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/stretchr/testify/require"
)

// blocksPerFileForTest is the number of blocks written per block file by newPrunableTestLedger.
const blocksPerFileForTest = 10

// newPrunableTestLedger stores numBlocks blocks with exactly blocksPerFileForTest blocks per block file,
// so that blockNum / blocksPerFileForTest is that block's file number and every file starts on a multiple
// of blocksPerFileForTest. The layout is asserted, not assumed, because the tests derive a marker's file
// number from its block number.
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

// pruneFilesUpTo advances the marker to the first block of firstStoredBlockfileNum and unlinks every block
// file below it, producing the on-disk state that pruning leaves behind.
func pruneFilesUpTo(t *testing.T, mgr *blockfileMgr, firstStoredBlockfileNum int) {
	// Pruning never removes the file holding lastPersistedBlock, so neither may this fixture.
	require.LessOrEqual(t, uint64(firstStoredBlockfileNum)*blocksPerFileForTest, mgr.blockfilesInfo.lastPersistedBlock,
		"fixture would remove the file holding lastPersistedBlock")

	require.NoError(t, mgr.pruner.setMarker(&pruneMarker{
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
// file still on disk. Reversed, the read guard would admit a block whose file has been removed.
func requirePruneInvariant(t *testing.T, mgr *blockfileMgr) {
	marker := mgr.pruner.marker.Load()
	firstInLowest, err := retrieveFirstBlockNumFromFile(mgr.rootDir, marker.firstStoredBlockfileNum)
	require.NoError(t, err)
	require.GreaterOrEqual(t, marker.firstReadableBlockNum, firstInLowest)
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

// Scenario:
// 1. Marshal a prune marker.
// 2. Unmarshal the bytes into a fresh prune marker.
// 3. Expect it to equal the original, for zero, small and large field values.
func TestPruneMarkerMarshalUnmarshal(t *testing.T) {
	for _, marker := range []*pruneMarker{
		{firstReadableBlockNum: 0, firstStoredBlockfileNum: 0},
		{firstReadableBlockNum: 1, firstStoredBlockfileNum: 1},
		{firstReadableBlockNum: 20, firstStoredBlockfileNum: 2},
		{firstReadableBlockNum: 1 << 40, firstStoredBlockfileNum: 1 << 20},
	} {
		t.Run(marker.String(), func(t *testing.T) {
			decoded := &pruneMarker{}
			require.NoError(t, decoded.unmarshal(marker.marshal()))
			require.Equal(t, marker, decoded)
		})
	}
}

// Scenario:
//  1. Store 50 blocks across 5 block files, writing no prune marker.
//  2. Expect the readable bound and the lowest block file to be 0, and the ledger not to be reported as
//     bootstrapped from a snapshot.
//  3. Expect every block, including block 0, to be retrievable by number, and MaxUint64 to return the last.
//  4. Expect block 0 to pass the availability check.
func TestPruneMarkerAbsentByDefault(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.Equal(t, uint64(0), mgr.firstAvailableBlockNum())
	require.Equal(t, uint64(0), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 0, mgr.pruner.firstStoredBlockfileNum())
	require.False(t, mgr.bootstrappedFromSnapshot())

	// Every block, including block 0, is still served (also covers the MaxUint64 "newest" alias).
	w.testGetBlockByNumber(blocks)
	require.NoError(t, mgr.checkBlockAvailable(0))
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Write a prune marker at block 20 / file 2, leaving every block file in place.
//  3. Expect the accessors to report the marker, Height to stay 50, and the ledger not to be reported as
//     bootstrapped from a snapshot.
//  4. For blocks 0, 1 and 19, expect retrieveBlockByNumber, retrieveBlockHeaderByNumber, retrieveBlocks
//     and retrieveTransactionByBlockNumTranNum to fail with ErrPruned.
//  5. For blocks 20..49, expect those paths to succeed and return the original blocks, and MaxUint64 to
//     return the last block.
//  6. Expect an iterator started at block 20 to return block 20.
func TestPruneMarkerReadGuards(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	const firstAvailable = 20
	require.NoError(t, mgr.pruner.setMarker(&pruneMarker{
		firstReadableBlockNum:   firstAvailable,
		firstStoredBlockfileNum: firstAvailable / blocksPerFileForTest,
	}))

	require.Equal(t, uint64(firstAvailable), mgr.firstAvailableBlockNum())
	require.Equal(t, uint64(firstAvailable), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 2, mgr.pruner.firstStoredBlockfileNum())

	// Height is about the tail and must be untouched by a prune.
	require.Equal(t, uint64(len(blocks)), mgr.getBlockchainInfo().Height)

	// A pruned ledger is not a snapshot-bootstrapped one.
	require.False(t, mgr.bootstrappedFromSnapshot())
	require.Equal(t, uint64(0), mgr.firstBlockNumAfterSnapshotBootstrap())

	t.Run("blocks below the marker are reported as pruned", func(t *testing.T) {
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

	t.Run("blocks at or above the marker are served unchanged", func(t *testing.T) {
		// Also covers the MaxUint64 "give me the newest" alias, which must stay reachable.
		w.testGetBlockByNumber(blocks[firstAvailable:])

		header, err := mgr.retrieveBlockHeaderByNumber(firstAvailable)
		require.NoError(t, err)
		require.Equal(t, blocks[firstAvailable].Header, header)

		_, err = mgr.retrieveTransactionByBlockNumTranNum(firstAvailable, 0)
		require.NoError(t, err)
	})

	t.Run("an iterator may start at the marker", func(t *testing.T) {
		itr, err := mgr.retrieveBlocks(firstAvailable)
		require.NoError(t, err)
		defer itr.Close()

		got, err := itr.Next()
		require.NoError(t, err)
		require.Equal(t, blocks[firstAvailable], got)
	})
}

// Scenario:
// 1. Store 50 blocks across 5 block files.
// 2. Write a prune marker at block 30 / file 3, leaving every block file in place.
// 3. Close the store and reopen it.
// 4. Expect the marker to be reported back and Height to stay 50.
// 5. Expect block 29 to fail with ErrPruned and block 30 to be returned.
func TestPruneMarkerSurvivesCloseAndReopen(t *testing.T) {
	path := t.TempDir()
	const firstAvailable = 30

	env, w, blocks := newPrunableTestLedger(t, path, 50)
	require.NoError(t, w.blockfileMgr.pruner.setMarker(&pruneMarker{
		firstReadableBlockNum:   firstAvailable,
		firstStoredBlockfileNum: firstAvailable / blocksPerFileForTest,
	}))
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	require.Equal(t, uint64(firstAvailable), store.FirstAvailableBlockNumber())
	require.Equal(t, 3, store.fileMgr.pruner.firstStoredBlockfileNum())

	info, err := store.GetBlockchainInfo()
	require.NoError(t, err)
	require.Equal(t, uint64(len(blocks)), info.Height)

	_, err = store.RetrieveBlockByNumber(firstAvailable - 1)
	require.ErrorIs(t, err, ErrPruned)

	got, err := store.RetrieveBlockByNumber(firstAvailable)
	require.NoError(t, err)
	require.Equal(t, blocks[firstAvailable], got)
}

// The state below is synthetic: snapshot bootstrap is only reachable from tests in this repo, so the two
// bounds never coexist in practice. The test pins checkBlockAvailable's precedence in case they ever do.
//
// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Set a bootstrapping snapshot whose last block is 9, and a prune marker at block 30 / file 3.
//  3. Expect the snapshot frontier to be 10, the readable bound to be 30, and the ledger to be reported
//     as bootstrapped from a snapshot.
//  4. Expect block 9 to be rejected as bootstrapped-from-snapshot and not as ErrPruned.
//  5. Expect blocks 10 and 29 to be rejected with ErrPruned.
//  6. Expect block 30 to pass the availability check.
//  7. Lower the marker below the snapshot frontier and expect the snapshot bound to win instead.
func TestFirstAvailableBlockNumCombinesSnapshotAndPruneMarker(t *testing.T) {
	env, w, _ := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	const snapshotLastBlock = 9 // the snapshot covered blocks 0-9, so the files start at 10
	const markerBlock = 30

	mgr.bootstrappingSnapshotInfo = &BootstrappingSnapshotInfo{LastBlockNum: snapshotLastBlock}
	require.NoError(t, mgr.pruner.setMarker(&pruneMarker{
		firstReadableBlockNum:   markerBlock,
		firstStoredBlockfileNum: markerBlock / blocksPerFileForTest,
	}))

	require.Equal(t, uint64(snapshotLastBlock+1), mgr.firstBlockNumAfterSnapshotBootstrap())
	require.Equal(t, uint64(markerBlock), mgr.firstAvailableBlockNum())
	require.True(t, mgr.bootstrappedFromSnapshot())

	// Below the snapshot frontier: those blocks were never in the block files, so the snapshot
	// explanation is the precise one and ErrPruned would be misleading.
	err := mgr.checkBlockAvailable(snapshotLastBlock)
	require.NotErrorIs(t, err, ErrPruned)
	require.ErrorContains(t, err, "bootstrapped from a snapshot")
	require.ErrorContains(t, err, fmt.Sprintf("First available block = [%d]", markerBlock))

	// Between the two bounds: pruning is what removed these.
	require.ErrorIs(t, mgr.checkBlockAvailable(snapshotLastBlock+1), ErrPruned)
	require.ErrorIs(t, mgr.checkBlockAvailable(markerBlock-1), ErrPruned)

	// At or above both bounds: available.
	require.NoError(t, mgr.checkBlockAvailable(markerBlock))

	// The other branch of the max: a bound below the snapshot frontier leaves the frontier in charge.
	require.NoError(t, mgr.pruner.setMarker(&pruneMarker{firstReadableBlockNum: 5, firstStoredBlockfileNum: 0}))
	require.Equal(t, uint64(snapshotLastBlock+1), mgr.firstAvailableBlockNum())
	require.ErrorContains(t, mgr.checkBlockAvailable(snapshotLastBlock), "bootstrapped from a snapshot")
}

// Scenario:
//  1. Store 20 blocks across 2 block files.
//  2. Write a prune marker at block 10 / file 1.
//  3. Expect the ledger not to be reported as bootstrapped from a snapshot, and the snapshot frontier to
//     stay 0.
//  4. Delete the index savepoint, so syncIndex takes the full-rebuild path.
//  5. Expect syncIndex to succeed.
func TestPruneMarkerDoesNotAffectSnapshotDetection(t *testing.T) {
	env, w, _ := newPrunableTestLedger(t, t.TempDir(), 20)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruner.setMarker(&pruneMarker{firstReadableBlockNum: 10, firstStoredBlockfileNum: 1}))

	require.False(t, mgr.bootstrappedFromSnapshot())
	require.Equal(t, uint64(0), mgr.firstBlockNumAfterSnapshotBootstrap())

	// syncIndex only consults bootstrappedFromSnapshot() when there is no index savepoint, i.e. on a
	// full rebuild. Drop the savepoint to actually reach that branch -- with a healthy savepoint
	// syncIndex returns early at "already in sync" and this assertion would be vacuous.
	require.NoError(t, mgr.db.Delete(indexSavePointKey, true))
	require.NoError(t, mgr.syncIndex())
}

// Scenario:
// 1. Store 5 blocks.
// 2. Overwrite the prune marker key with a truncated varint.
// 3. Expect loading the marker to fail with an unmarshalling error.
func TestLoadPruneMarkerOnCorruptValue(t *testing.T) {
	env, w, _ := newPrunableTestLedger(t, t.TempDir(), 5)
	defer env.Cleanup()

	// A varint that claims more continuation bytes than are present.
	require.NoError(t, w.blockfileMgr.db.Put(pruneMarkerKey, []byte{0xff}, true))

	_, err := w.blockfileMgr.pruner.load()
	require.ErrorContains(t, err, "error unmarshalling prune marker")
}

// Scenario:
//  1. Construct a pruneMgr over a ledger's index database handle and expect a zero marker.
//  2. Set a marker through it.
//  3. Construct a second pruneMgr over the same handle and expect it to load that marker.
func TestPruneMgrLoadsThePersistedMarker(t *testing.T) {
	env, w, _ := newPrunableTestLedger(t, t.TempDir(), 5)
	defer env.Cleanup()

	p, err := newPruneMgr(w.blockfileMgr.rootDir, w.blockfileMgr.db, w.blockfileMgr.index)
	require.NoError(t, err)
	require.Equal(t, uint64(0), p.firstReadableBlockNum())
	require.Equal(t, 0, p.firstStoredBlockfileNum())

	require.NoError(t, p.setMarker(&pruneMarker{firstReadableBlockNum: 3, firstStoredBlockfileNum: 1}))

	reloaded, err := newPruneMgr(w.blockfileMgr.rootDir, w.blockfileMgr.db, w.blockfileMgr.index)
	require.NoError(t, err)
	require.Equal(t, uint64(3), reloaded.firstReadableBlockNum())
	require.Equal(t, 1, reloaded.firstStoredBlockfileNum())
}

// Scenario:
// 1. Store 50 blocks across 5 block files.
// 2. Write a prune marker at block 20 / file 2 and delete block files 0 and 1.
// 3. Rewind the index savepoint to block 45, so syncIndex has a tail to rebuild.
// 4. Close the store and reopen it.
// 5. Expect the open to succeed, the marker to be reported back, and Height to stay 50.
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
// 2. Write a prune marker at block 20 / file 2 and delete block files 0 and 1.
// 3. Rewind the index savepoint to block 5, below the marker.
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
// 2. Write a prune marker at block 20 / file 2 and delete block files 0 and 1.
// 3. Roll the marker back to block 10 / file 1, which has already been deleted.
// 4. Rewind the index savepoint to block 45 and close the store.
// 5. Expect reopening to fail with an error naming the missing block file and the prune point.
func TestOpenPrunedLedgerWithStaleMarker(t *testing.T) {
	path := t.TempDir()

	env, w, blocks := newPrunableTestLedger(t, path, 50)
	pruneFilesUpTo(t, w.blockfileMgr, 2)
	require.NoError(t, w.blockfileMgr.pruner.setMarker(&pruneMarker{
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
//  1. Store 50 blocks across 5 block files.
//  2. Call pruneBefore(20), a file boundary.
//  3. Expect the marker to be block 20 / file 2, and block files 0 and 1 to be gone.
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
//  4. Expect the index savepoint to be unchanged.
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
//  1. Open a ledger and append nothing.
//  2. Call pruneBefore(10).
//  3. Expect no error, the marker to stay 0, and nothing to be removed.
func TestPruneBeforeOnEmptyLedger(t *testing.T) {
	env := newTestEnv(t, NewConf(t.TempDir(), 0))
	defer env.Cleanup()
	mgr := newTestBlockfileWrapper(env, "testLedger").blockfileMgr

	require.NoError(t, mgr.pruneBefore(10))

	require.Equal(t, uint64(0), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 0, mgr.pruner.firstStoredBlockfileNum())
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
//  1. Store 50 blocks across 5 block files.
//  2. Call pruneBefore(10), exactly the first block of block file 1.
//  3. Expect only block file 0 to be removed and the marker to be block 10.
//  4. Call pruneBefore(30), exactly the first block of block file 3.
//  5. Expect block files 1 and 2 to be removed and the marker to be block 30.
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
//     file 5 and a marker of block 50.
//  5. Expect blocks 40..49 to fail with ErrPruned and blocks 50..59 to be returned.
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
//  1. Store 50 blocks across 5 block files.
//  2. Call pruneBefore(30) from 8 goroutines at once.
//  3. Expect every call to return without error.
//  4. Expect the marker to be block 30 / file 3, exactly the files below it to be gone, and blocks 0..29
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

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Truncate the tail of block file 2, so streaming its blocks fails partway.
//  3. Call pruneBefore(45).
//  4. Expect an error, and the two files removed before the failure to stay removed: block files 0
//     and 1 gone, marker advanced to block 20 and no further.
//  5. Expect block files 2, 3 and 4 to remain.
func TestPruneBeforeKeepsProgressWhenAFileFails(t *testing.T) {
	env, w, _ := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	corrupted := deriveBlockfilePath(mgr.rootDir, 2)
	info, err := os.Stat(corrupted)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(corrupted, info.Size()-10))

	require.Error(t, mgr.pruneBefore(45))

	// The bound is durable from the first commit, so it reflects the whole request; only the physical
	// pruning stopped short.
	require.Equal(t, uint64(45), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 2, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{2, 3, 4}, blockfileNums(t, mgr.rootDir))
	requirePruneInvariant(t, mgr)
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Advance the marker to block 20 / file 2 and unlink block file 0 only, the state a crash partway
//     through the unlinks leaves behind.
//  3. Call pruneBefore(20), which has nothing new to remove.
//  4. Expect block file 1 to be removed too, the already-missing block file 0 to be tolerated, and the
//     marker to stay at block 20.
func TestPruneBeforeSweepsFilesLeftBehindByACrash(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, mgr.pruner.setMarker(&pruneMarker{firstReadableBlockNum: 20, firstStoredBlockfileNum: 2}))
	require.NoError(t, os.Remove(deriveBlockfilePath(mgr.rootDir, 0)))
	require.Equal(t, []int{1, 2, 3, 4}, blockfileNums(t, mgr.rootDir))

	require.NoError(t, mgr.pruneBefore(20))

	require.Equal(t, []int{2, 3, 4}, blockfileNums(t, mgr.rootDir))
	require.Equal(t, uint64(20), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 2, mgr.pruner.firstStoredBlockfileNum())
	requireBlocksPruned(t, mgr, blocks, 20)
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Truncate block file 1 to zero bytes, so its first block cannot be read.
//  3. Call pruneBefore(40).
//  4. Expect an error naming that block file, and nothing to have been removed: the probe for the first
//     candidate file is what fails, before any commit, so the marker stays 0 and every file remains.
func TestPruneBeforeFailsBeforeCommittingWhenAFileIsUnreadable(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, os.Truncate(deriveBlockfilePath(mgr.rootDir, 1), 0))

	err := mgr.pruneBefore(40)
	require.ErrorContains(t, err, "cannot determine the first block of block file [1]")

	require.Equal(t, uint64(0), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 0, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{0, 1, 2, 3, 4}, blockfileNums(t, mgr.rootDir))

	got, err := mgr.retrieveBlockByNumber(0)
	require.NoError(t, err)
	require.Equal(t, blocks[0], got)
}

// Scenario:
//  1. Store 50 blocks across 5 block files and call pruneBefore(25), so the readable bound sits inside the
//     lowest surviving file rather than on its boundary.
//  2. Rewind the index savepoint to block 45 and close the store.
//  3. Reopen it, and expect the open to succeed with the bound and lowest file both preserved.
//  4. Expect blocks 0..24 to fail with ErrPruned and blocks 25..49 to be returned.
func TestReopenAfterRealPruneWithIndexBehind(t *testing.T) {
	path := t.TempDir()
	env, w, blocks := newPrunableTestLedger(t, path, 50)
	require.NoError(t, w.blockfileMgr.pruneBefore(25))
	rewindIndexSavepoint(t, w.blockfileMgr, uint64(len(blocks)-5))
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	require.Equal(t, uint64(25), store.fileMgr.pruner.firstReadableBlockNum())
	require.Equal(t, 2, store.fileMgr.pruner.firstStoredBlockfileNum())
	requireBlocksPruned(t, store.fileMgr, blocks, 25)
}

// Scenario:
//  1. Store 50 blocks across 5 block files and call pruneBefore(25).
//  2. Rewind the index savepoint below the readable bound, to block 5, and reopen the store.
//  3. Expect the open to succeed: the rebuild starts at the lowest surviving file and re-indexes blocks
//     20..49, including 20..24 which the bound hides.
//  4. Expect blocks 20..24 to have index entries again, and still to fail with ErrPruned, because the read
//     guard rejects them before the index is consulted.
func TestReopenAfterRealPruneWithIndexBelowTheBound(t *testing.T) {
	path := t.TempDir()
	env, w, blocks := newPrunableTestLedger(t, path, 50)
	require.NoError(t, w.blockfileMgr.pruneBefore(25))
	rewindIndexSavepoint(t, w.blockfileMgr, 5)
	env.provider.Close()

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	store, err := reopened.provider.Open("testLedger")
	require.NoError(t, err)

	for i := 20; i < 25; i++ {
		_, err := store.fileMgr.index.getBlockLocByBlockNum(uint64(i))
		require.NoError(t, err, "block %d is on disk, so the rebuild indexes it", i)
		_, err = store.RetrieveBlockByNumber(uint64(i))
		require.ErrorIs(t, err, ErrPruned)
	}
	requireBlocksPruned(t, store.fileMgr, blocks, 25)
}

// Scenario:
//  1. Store 50 blocks across 5 block files.
//  2. Make the ledger directory read-only, so unlinking fails after the commit that records the file gone.
//  3. Call pruneBefore(20) and expect an error, with the bound and lowest file already advanced and both
//     block files still on disk.
//  4. Restore write permission and call pruneBefore(20) again.
//  5. Expect the orphaned files to be swept and the marker to stay where it was.
func TestPruneBeforeUnlinkFailureLeavesRecoverableOrphans(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	require.NoError(t, os.Chmod(mgr.rootDir, 0o500))
	err := mgr.pruneBefore(20)
	require.NoError(t, os.Chmod(mgr.rootDir, 0o700))
	require.ErrorContains(t, err, "error removing block file [0]")

	require.Equal(t, uint64(20), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 1, mgr.pruner.firstStoredBlockfileNum())
	require.Equal(t, []int{0, 1, 2, 3, 4}, blockfileNums(t, mgr.rootDir))

	require.NoError(t, mgr.pruneBefore(20))

	require.Equal(t, []int{2, 3, 4}, blockfileNums(t, mgr.rootDir))
	require.Equal(t, uint64(20), mgr.pruner.firstReadableBlockNum())
	require.Equal(t, 2, mgr.pruner.firstStoredBlockfileNum())
	requireBlocksPruned(t, mgr, blocks, 20)
}

// Scenario:
//  1. Store 50 blocks across 5 block files and call pruneBefore(20).
//  2. Expect a block hash lookup for a pruned block to fail, but not with ErrPruned: its hash index
//     entry went with the file, and that path has no availability guard.
//  3. Expect a transaction lookup for a pruned block to fail the same way.
//  4. Expect an iterator started below the bound to fail with ErrPruned, because that path is guarded.
func TestPruneBeforeUnguardedLookupsDoNotReportErrPruned(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	txID, err := protoutil.GetOrComputeTxIDFromEnvelope(blocks[5].Data.Data[0])
	require.NoError(t, err)
	require.NoError(t, mgr.pruneBefore(20))

	_, err = mgr.retrieveBlockByHash(protoutil.BlockHeaderHash(blocks[5].Header))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrPruned)

	_, err = mgr.retrieveTransactionByID(txID)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrPruned)

	_, err = mgr.retrieveBlocks(19)
	require.ErrorIs(t, err, ErrPruned)
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
//  1. Store 50 blocks over five block files and prune the two lowest, so the ledger starts at block 20.
//  2. Close the store and call ResetBlockStore.
//  3. Expect an error naming the ledger, the pruning, and the block the ledger now starts at.
//  4. Expect nothing to have been removed: the index directory and every surviving block file are intact,
//     since resetting a pruned ledger must not get halfway through before failing.
func TestResetBlockStoreRefusesAPrunedLedger(t *testing.T) {
	path := t.TempDir()
	env, w, _ := newPrunableTestLedger(t, path, 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	pruneFilesUpTo(t, mgr, 2)
	filesBefore := blockfileNums(t, mgr.rootDir)
	env.provider.Close()

	err := ResetBlockStore(path)
	require.ErrorContains(t, err,
		"cannot reset ledger [testLedger]: it has been pruned, so it starts at block [20] in block file [2] "+
			"rather than at the genesis block")

	require.Equal(t, filesBefore, blockfileNums(t, mgr.rootDir), "a refused reset must not remove block files")
	indexEntries, err := os.ReadDir((&Conf{blockStorageDir: path}).getIndexDir())
	require.NoError(t, err)
	require.NotEmpty(t, indexEntries, "a refused reset must not drop the index")
}

// Scenario:
//  1. Store 50 blocks over five block files and prune the two lowest, so the ledger starts at block 20.
//  2. Close the store and roll back to a block below 20.
//  3. Expect an error naming the ledger, the target, and the first available block.
//  4. Expect the block files to be untouched, since the target can never be reached.
func TestRollbackRefusesATargetBelowThePrunePoint(t *testing.T) {
	path := t.TempDir()
	env, w, _ := newPrunableTestLedger(t, path, 50)
	defer env.Cleanup()
	mgr := w.blockfileMgr

	pruneFilesUpTo(t, mgr, 2)
	filesBefore := blockfileNums(t, mgr.rootDir)
	env.provider.Close()
	w.close()

	err := Rollback(path, "testLedger", 15, &IndexConfig{AttrsToIndex: attrsToIndex})
	require.ErrorContains(t, err,
		"cannot roll back ledger [testLedger] to block [15]: the ledger has been pruned and its first "+
			"available block is [20]")

	require.Equal(t, filesBefore, blockfileNums(t, mgr.rootDir), "a refused rollback must not remove block files")
}

// Scenario:
//  1. Store 50 blocks over five block files and prune the two lowest, so the ledger starts at block 20.
//  2. Close the store and roll back to block 45, which the ledger still holds.
//  3. Expect the rollback to succeed and the store to reopen.
//  4. Expect the height to follow the rollback, the surviving blocks to be readable and unchanged, and the
//     pruned blocks to still report ErrPruned: the marker survives a rollback.
func TestRollbackToATargetAboveThePrunePoint(t *testing.T) {
	path := t.TempDir()
	env, w, blocks := newPrunableTestLedger(t, path, 50)
	mgr := w.blockfileMgr

	pruneFilesUpTo(t, mgr, 2)
	env.provider.Close()
	w.close()

	require.NoError(t, Rollback(path, "testLedger", 45, &IndexConfig{AttrsToIndex: attrsToIndex}))

	reopened := newTestEnv(t, NewConf(path, 0))
	defer reopened.Cleanup()
	w = newTestBlockfileWrapper(reopened, "testLedger")
	defer w.close()
	mgr = w.blockfileMgr

	require.Equal(t, uint64(46), mgr.getBlockchainInfo().Height)
	require.Equal(t, uint64(20), mgr.pruner.firstReadableBlockNum())

	_, err := mgr.retrieveBlockByNumber(19)
	require.ErrorIs(t, err, ErrPruned)

	for i := 20; i <= 45; i++ {
		block, err := mgr.retrieveBlockByNumber(uint64(i))
		require.NoError(t, err, "block %d must survive the rollback", i)
		require.Equal(t, blocks[i], block)
	}
}
