/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"os"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestBlockfileStream(t *testing.T) {
	testBlockfileStream(t, 0)
	testBlockfileStream(t, 1)
	testBlockfileStream(t, 10)
}

func testBlockfileStream(t *testing.T, numBlocks int) {
	env := newTestEnv(t, NewConf(t.TempDir(), 0))
	defer env.Cleanup()
	ledgerid := "testledger"
	w := newTestBlockfileWrapper(env, ledgerid)
	blocks := testutil.ConstructTestBlocks(t, numBlocks)
	w.addBlocks(blocks)
	w.close()

	s, err := newBlockfileStream(w.blockfileMgr.rootDir, 0, 0)
	defer s.close()
	require.NoError(t, err, "Error in constructing blockfile stream")

	blockCount := 0
	for {
		blockBytes, err := s.nextBlockBytes()
		require.NoError(t, err, "Error in getting next block")
		if blockBytes == nil {
			break
		}
		blockCount++
	}
	// After the stream has been exhausted, both blockBytes and err should be nil
	blockBytes, err := s.nextBlockBytes()
	require.Nil(t, blockBytes)
	require.NoError(t, err, "Error in getting next block after exhausting the file")
	require.Equal(t, numBlocks, blockCount)
}

func TestBlockFileStreamUnexpectedEOF(t *testing.T) {
	partialBlockBytes := []byte{}
	dummyBlockBytes := testutil.ConstructRandomBytes(t, 100)
	lenBytes := protowire.AppendVarint(nil, uint64(len(dummyBlockBytes)))
	partialBlockBytes = append(partialBlockBytes, lenBytes...)
	partialBlockBytes = append(partialBlockBytes, dummyBlockBytes...)
	testBlockFileStreamUnexpectedEOF(t, 10, partialBlockBytes[:1])
	testBlockFileStreamUnexpectedEOF(t, 10, partialBlockBytes[:2])
	testBlockFileStreamUnexpectedEOF(t, 10, partialBlockBytes[:5])
	testBlockFileStreamUnexpectedEOF(t, 10, partialBlockBytes[:20])
}

func testBlockFileStreamUnexpectedEOF(t *testing.T, numBlocks int, partialBlockBytes []byte) {
	env := newTestEnv(t, NewConf(t.TempDir(), 0))
	defer env.Cleanup()
	w := newTestBlockfileWrapper(env, "testLedger")
	blockfileMgr := w.blockfileMgr
	blocks := testutil.ConstructTestBlocks(t, numBlocks)
	w.addBlocks(blocks)
	blockfileMgr.currentFileWriter.append(partialBlockBytes, true)
	w.close()
	s, err := newBlockfileStream(blockfileMgr.rootDir, 0, 0)
	defer s.close()
	require.NoError(t, err, "Error in constructing blockfile stream")

	for i := 0; i < numBlocks; i++ {
		blockBytes, err := s.nextBlockBytes()
		require.NotNil(t, blockBytes)
		require.NoError(t, err, "Error in getting next block")
	}
	blockBytes, err := s.nextBlockBytes()
	require.Nil(t, blockBytes)
	require.Exactly(t, ErrUnexpectedEndOfBlockfile, err)
}

func TestBlockStream(t *testing.T) {
	testBlockStream(t, 1)
	testBlockStream(t, 2)
	testBlockStream(t, 10)
}

func testBlockStream(t *testing.T, numFiles int) {
	ledgerID := "testLedger"
	env := newTestEnv(t, NewConf(t.TempDir(), 0))
	defer env.Cleanup()
	w := newTestBlockfileWrapper(env, ledgerID)
	defer w.close()
	blockfileMgr := w.blockfileMgr

	numBlocksInEachFile := 10
	bg, gb := testutil.NewBlockGenerator(t, ledgerID, false)
	w.addBlocks([]*common.Block{gb})
	for i := 0; i < numFiles; i++ {
		numBlocks := numBlocksInEachFile
		if i == 0 {
			// genesis block already added so adding one less block
			numBlocks = numBlocksInEachFile - 1
		}
		blocks := bg.NextTestBlocks(numBlocks)
		w.addBlocks(blocks)
		blockfileMgr.moveToNextFile()
	}
	s, err := newBlockStream(blockfileMgr.rootDir, 0, 0, numFiles-1)
	defer s.close()
	require.NoError(t, err, "Error in constructing new block stream")
	blockCount := 0
	for {
		blockBytes, err := s.nextBlockBytes()
		require.NoError(t, err, "Error in getting next block")
		if blockBytes == nil {
			break
		}
		blockCount++
	}
	// After the stream has been exhausted, both blockBytes and err should be nil
	blockBytes, err := s.nextBlockBytes()
	require.Nil(t, blockBytes)
	require.NoError(t, err, "Error in getting next block after exhausting the file")
	require.Equal(t, numFiles*numBlocksInEachFile, blockCount)
}

// A blockStream closes the file it holds before it opens the next one, so a failure to open leaves it
// owning nothing. Once pruning can unlink a file under a reader that has fallen behind, that failure is
// ordinary rather than a sign of a broken disk, and the stream has to survive being used and closed after
// it.
//
// Scenario:
//  1. Store 20 blocks across block files 0 and 1, and open a stream over both.
//  2. Remove block file 1, the state a prune leaves for a reader still inside block file 0.
//  3. Read block file 0 to its end, and expect the read that crosses into block file 1 to report that it
//     cannot be opened.
//  4. Read twice more and expect the same error each time, rather than a panic on the file the stream gave
//     up.
//  5. Close the stream and expect no error, since there is nothing left for it to close.
func TestBlockStreamFailingToAdvance(t *testing.T) {
	env, w, blocks := newPrunableTestLedger(t, t.TempDir(), 2*blocksPerFileForTest)
	defer env.Cleanup()
	rootDir := w.blockfileMgr.rootDir

	s, err := newBlockStream(rootDir, 0, 0, 1)
	require.NoError(t, err)

	require.NoError(t, os.Remove(deriveBlockfilePath(rootDir, 1)))

	for i := 0; i < blocksPerFileForTest; i++ {
		blockBytes, err := s.nextBlockBytes()
		require.NoError(t, err)
		require.NotNil(t, blockBytes, "block [%d] should have been read from block file 0", i)
	}

	blockBytes, err := s.nextBlockBytes()
	require.Nil(t, blockBytes)
	require.ErrorContains(t, err, "error opening block file")
	advanceErr := err

	for i := 0; i < 2; i++ {
		blockBytes, err = s.nextBlockBytes()
		require.Nil(t, blockBytes)
		require.Equal(t, advanceErr, err)
	}

	require.Nil(t, s.currentFileStream)
	require.NoError(t, s.close())
	require.Len(t, blocks, 2*blocksPerFileForTest)
}

// Scenario:
//  1. Store 20 blocks and prune below block 10.
//  2. Ask for an iterator starting at a pruned block.
//  3. Expect ErrPruned, and the interface the caller holds to compare equal to nil. A typed nil pointer
//     wrapped in the iterator interface does not, so a caller that tests the iterator instead of the error
//     would call a method on nothing.
func TestRetrieveBlocksReturnsANilIteratorOnError(t *testing.T) {
	env, _, _ := newPrunableTestLedger(t, t.TempDir(), 2*blocksPerFileForTest)
	defer env.Cleanup()

	store, err := env.provider.Open("testLedger")
	require.NoError(t, err)
	require.NoError(t, store.PruneBefore(blocksPerFileForTest))

	itr, err := store.RetrieveBlocks(0)
	require.ErrorIs(t, err, ErrPruned)
	// Compared with == rather than require.Nil, which reflects into the interface and accepts a typed nil.
	require.True(t, itr == nil, "iterator interface should be nil, got %T", itr)
}
