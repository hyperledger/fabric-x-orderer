/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
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

func checkBlockfilesInfoFromFS(t *testing.T, blkStoreDir string, expected *blockfilesInfo) {
	blkfilesInfo, err := constructBlockfilesInfo(blkStoreDir)
	require.NoError(t, err)
	require.Equal(t, expected, blkfilesInfo)
}

// Scenario:
//  1. Create a directory holding three block files, a stray file of another name, and the prune store's
//     subdirectory.
//  2. Expect blockfileNumsIn to return only the block file numbers, ascending.
//  3. Add a file whose name starts with the block file prefix but does not end in a number.
//  4. Expect an error naming it, rather than that file being skipped.
func TestBlockfileNumsIn(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		blockfilePrefix + "000002",
		blockfilePrefix + "000000",
		blockfilePrefix + "000010",
		"__backupGenesisBlockBytes",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o600))
	}
	// The prune store sits in the ledger's own directory, so directories must be skipped.
	require.NoError(t, os.Mkdir(filepath.Join(dir, pruneStoreFileSuffix), 0o750))

	nums, err := blockfileNumsIn(dir)
	require.NoError(t, err)
	require.Equal(t, []int{0, 2, 10}, nums)

	require.NoError(t, os.WriteFile(filepath.Join(dir, blockfilePrefix+"bogus"), nil, 0o600))

	_, err = blockfileNumsIn(dir)
	require.ErrorContains(t, err, "unexpected block file name "+blockfilePrefix+"bogus")
}

// Scenario:
// 1. Call blockfileNumsIn on an empty directory and expect no numbers and no error.
// 2. Call it on a directory that does not exist and expect an error.
func TestBlockfileNumsInEdgeCases(t *testing.T) {
	nums, err := blockfileNumsIn(t.TempDir())
	require.NoError(t, err)
	require.Empty(t, nums)

	_, err = blockfileNumsIn(filepath.Join(t.TempDir(), "missing"))
	require.ErrorContains(t, err, "error reading dir")
}
