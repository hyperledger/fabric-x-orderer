/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"os"
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
