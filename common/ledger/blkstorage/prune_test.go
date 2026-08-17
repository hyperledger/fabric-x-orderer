/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"
	"os"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
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

	p, err := newPruneMgr(w.blockfileMgr.db)
	require.NoError(t, err)
	require.Equal(t, uint64(0), p.firstReadableBlockNum())
	require.Equal(t, 0, p.firstStoredBlockfileNum())

	require.NoError(t, p.setMarker(&pruneMarker{firstReadableBlockNum: 3, firstStoredBlockfileNum: 1}))

	reloaded, err := newPruneMgr(w.blockfileMgr.db)
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
