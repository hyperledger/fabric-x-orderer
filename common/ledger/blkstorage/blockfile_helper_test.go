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

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestConstructBlockfilesInfo(t *testing.T) {
	ledgerid := "testLedger"
	conf := NewConf(t.TempDir(), 0)
	blkStoreDir := conf.getLedgerBlockDir(ledgerid)
	env := newTestEnv(t, conf)
	require.NoError(t, os.MkdirAll(blkStoreDir, 0o755))
	defer env.Cleanup()

	// constructBlockfilesInfo on an empty block folder should return blockfileInfo with noBlockFiles: true
	blkfilesInfo, err := constructBlockfilesInfo(blkStoreDir)
	require.NoError(t, err)
	require.Equal(
		t,
		&blockfilesInfo{
			noBlockFiles:       true,
			lastPersistedBlock: 0,
			latestFileSize:     0,
			latestFileNumber:   0,
		},
		blkfilesInfo,
	)

	w := newTestBlockfileWrapper(env, ledgerid)
	defer w.close()
	blockfileMgr := w.blockfileMgr
	bg, gb := testutil.NewBlockGenerator(t, ledgerid, false)

	// Add a few blocks and verify that blockfilesInfo derived from filesystem should be same as from the blockfile manager
	blockfileMgr.addBlock(gb)
	for _, blk := range bg.NextTestBlocks(3) {
		blockfileMgr.addBlock(blk)
	}
	checkBlockfilesInfoFromFS(t, blkStoreDir, blockfileMgr.blockfilesInfo)

	// Move the chain to new file and check blockfilesInfo derived from file system
	blockfileMgr.moveToNextFile()
	checkBlockfilesInfoFromFS(t, blkStoreDir, blockfileMgr.blockfilesInfo)

	// Add a few blocks that would go to new file and verify that blockfilesInfo derived from filesystem should be same as from the blockfile manager
	for _, blk := range bg.NextTestBlocks(3) {
		blockfileMgr.addBlock(blk)
	}
	checkBlockfilesInfoFromFS(t, blkStoreDir, blockfileMgr.blockfilesInfo)

	// Write a partial block (to simulate a crash) and verify that blockfilesInfo derived from filesystem should be same as from the blockfile manager
	lastTestBlk := bg.NextTestBlocks(1)[0]
	blockBytes, _ := serializeBlock(lastTestBlk, false)
	partialByte := append(protowire.AppendVarint(nil, uint64(len(blockBytes))), blockBytes[len(blockBytes)/2:]...)
	blockfileMgr.currentFileWriter.append(partialByte, true)
	checkBlockfilesInfoFromFS(t, blkStoreDir, blockfileMgr.blockfilesInfo)

	// Close the block storage, drop the index and restart and verify
	blkfilesInfoBeforeClose := blockfileMgr.blockfilesInfo
	w.close()
	env.provider.Close()
	indexFolder := conf.getIndexDir()
	require.NoError(t, os.RemoveAll(indexFolder))

	env = newTestEnv(t, conf)
	w = newTestBlockfileWrapper(env, ledgerid)
	blockfileMgr = w.blockfileMgr
	require.Equal(t, blkfilesInfoBeforeClose, blockfileMgr.blockfilesInfo)

	lastBlkIndexed, err := blockfileMgr.index.getLastBlockIndexed()
	require.NoError(t, err)
	require.Equal(t, uint64(6), lastBlkIndexed)

	// Add the last block again after start and check blockfilesInfo again
	require.NoError(t, blockfileMgr.addBlock(lastTestBlk))
	checkBlockfilesInfoFromFS(t, blkStoreDir, blockfileMgr.blockfilesInfo)
}

func TestBinarySearchBlockFileNum(t *testing.T) {
	blockStoreRootDir := t.TempDir()
	blocks := testutil.ConstructTestBlocks(t, 100)
	maxFileSie := int(0.1 * float64(testutilEstimateTotalSizeOnDisk(t, blocks)))
	env := newTestEnv(t, NewConf(blockStoreRootDir, maxFileSie))
	defer env.Cleanup()
	blkfileMgrWrapper := newTestBlockfileWrapper(env, "testLedger")
	blkfileMgr := blkfileMgrWrapper.blockfileMgr

	blkfileMgrWrapper.addBlocks(blocks)

	ledgerDir := (&Conf{blockStorageDir: blockStoreRootDir}).getLedgerBlockDir("testLedger")
	files, err := os.ReadDir(ledgerDir)
	require.NoError(t, err)
	require.Len(t, files, 11)

	for i := uint64(0); i < 100; i++ {
		fileNum, err := binarySearchFileNumForBlock(ledgerDir, 0, i)
		require.NoError(t, err)
		locFromIndex, err := blkfileMgr.index.getBlockLocByBlockNum(i)
		require.NoError(t, err)
		expectedFileNum := locFromIndex.fileSuffixNum
		require.Equal(t, expectedFileNum, fileNum)
	}

	// Scenario:
	//  1. Delete block files 0..2, so the ledger starts at file 3.
	//  2. For every block from the first one in file 3 onward, expect the bounded search to return the
	//     file number the index reports.
	//  3. For a block below the first surviving one, expect the bounded search to return file 3 and the
	//     unbounded search to fail.
	t.Run("with a lower bound after the lowest files are removed", func(t *testing.T) {
		const firstFileNum = 3
		firstSurvivingBlock, err := retrieveFirstBlockNumFromFile(ledgerDir, firstFileNum)
		require.NoError(t, err)
		for f := 0; f < firstFileNum; f++ {
			require.NoError(t, os.Remove(deriveBlockfilePath(ledgerDir, f)))
		}

		for i := firstSurvivingBlock; i < 100; i++ {
			fileNum, err := binarySearchFileNumForBlock(ledgerDir, firstFileNum, i)
			require.NoError(t, err)
			locFromIndex, err := blkfileMgr.index.getBlockLocByBlockNum(i)
			require.NoError(t, err)
			require.Equal(t, locFromIndex.fileSuffixNum, fileNum)
		}

		belowFrontier := firstSurvivingBlock - 1

		fileNum, err := binarySearchFileNumForBlock(ledgerDir, firstFileNum, belowFrontier)
		require.NoError(t, err)
		require.Equal(t, firstFileNum, fileNum)

		_, err = binarySearchFileNumForBlock(ledgerDir, 0, belowFrontier)
		require.Error(t, err)
	})
}

func TestIsBootstrappedFromSnapshot(t *testing.T) {
	testDir := t.TempDir()

	t.Run("no_bootstrapping_snapshot_info_file", func(t *testing.T) {
		// create chains directory for the ledger without bootstrappingSnapshotInfoFile
		ledgerid := "testnosnapshotinfofile"
		require.NoError(t, os.MkdirAll(filepath.Join(testDir, ChainsDir, ledgerid), 0o755))
		isFromSnapshot, err := IsBootstrappedFromSnapshot(testDir, ledgerid)
		require.NoError(t, err)
		require.False(t, isFromSnapshot)
	})

	t.Run("with_bootstrapping_snapshot_info_file", func(t *testing.T) {
		// create chains directory for the ledger with bootstrappingSnapshotInfoFile
		ledgerid := "testwithsnapshotinfofile"
		ledgerChainDir := filepath.Join(testDir, ChainsDir, ledgerid)
		require.NoError(t, os.MkdirAll(ledgerChainDir, 0o755))
		file, err := os.Create(filepath.Join(ledgerChainDir, bootstrappingSnapshotInfoFile))
		require.NoError(t, err)
		defer file.Close()
		isFromSnapshot, err := IsBootstrappedFromSnapshot(testDir, ledgerid)
		require.NoError(t, err)
		require.True(t, isFromSnapshot)
	})
}

func TestGetLedgersBootstrappedFromSnapshot(t *testing.T) {
	t.Run("no_bootstrapping_snapshot_info_file", func(t *testing.T) {
		testDir := t.TempDir()

		// create chains directories for ledgers without bootstrappingSnapshotInfoFile
		for i := 0; i < 5; i++ {
			require.NoError(t, os.MkdirAll(filepath.Join(testDir, ChainsDir, fmt.Sprintf("ledger_%d", i)), 0o755))
		}

		ledgersFromSnapshot, err := GetLedgersBootstrappedFromSnapshot(testDir)
		require.NoError(t, err)
		require.Equal(t, 0, len(ledgersFromSnapshot))
	})

	t.Run("with_bootstrapping_snapshot_info_file", func(t *testing.T) {
		testDir := t.TempDir()

		// create chains directories for ledgers
		// also create bootstrappingSnapshotInfoFile for ledger_0 and ledger_1
		for i := 0; i < 5; i++ {
			ledgerChainDir := filepath.Join(testDir, ChainsDir, fmt.Sprintf("ledger_%d", i))
			require.NoError(t, os.MkdirAll(ledgerChainDir, 0o755))
			if i < 2 {
				file, err := os.Create(filepath.Join(ledgerChainDir, bootstrappingSnapshotInfoFile))
				require.NoError(t, err)
				defer file.Close()
			}
		}

		ledgersFromSnapshot, err := GetLedgersBootstrappedFromSnapshot(testDir)
		require.NoError(t, err)
		require.ElementsMatch(t, ledgersFromSnapshot, []string{"ledger_0", "ledger_1"})
	})
}

func checkBlockfilesInfoFromFS(t *testing.T, blkStoreDir string, expected *blockfilesInfo) {
	blkfilesInfo, err := constructBlockfilesInfo(blkStoreDir)
	require.NoError(t, err)
	require.Equal(t, expected, blkfilesInfo)
}
