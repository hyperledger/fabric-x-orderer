/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/hyperledger/fabric-x-common/tools/fileutil"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/util/leveldbhelper"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	blockfilePrefix            = "blockfile_"
	defaultBlockCacheSizeBytes = 1024 * 1024 * 50
)

var blkMgrInfoKey = []byte("blkMgrInfo")

type blockfileMgr struct {
	rootDir           string
	conf              *Conf
	db                *leveldbhelper.DBHandle
	index             *blockIndex
	blockfilesInfo    *blockfilesInfo
	blkfilesInfoCond  *sync.Cond
	currentFileWriter *blockfileWriter
	bcInfo            atomic.Value
	cache             *cache
}

/*
Creates a new manager that will manage the files used for block persistence.
This manager manages the file system FS including

	-- the directory where the files are stored
	-- the individual files where the blocks are stored
	-- the blockfilesInfo which tracks the latest file being persisted to
	-- the index which tracks what block and transaction is in what file

When a new blockfile manager is started (i.e. only on start-up), it checks
if this start-up is the first time the system is coming up or is this a restart
of the system.

The blockfile manager stores blocks of data into a file system.  That file
storage is done by creating sequentially numbered files of a configured size
i.e blockfile_000000, blockfile_000001, etc..

Each block is stored with the total encoded length of that block.

Remember that these steps are only done once at start-up of the system.
At start up a new manager:

	  *) Checks if the directory for storing files exists, if not creates the dir
	  *) Checks if the key value database exists, if not creates one
	       (will create a db dir)
	  *) Determines the blockfilesInfo used for storage
			-- Loads from db if exist, if not instantiate a new blockfilesInfo
			-- If blockfilesInfo was loaded from db, compares to FS
			-- If blockfilesInfo and file system are not in sync, syncs blockfilesInfo from FS
	  *) Starts a new file writer
			-- truncates file per blockfilesInfo to remove any excess past last block
	  *) Determines the index information used to find blocks in
	  the file blkstorage
			-- Instantiates a new blockIdxInfo
			-- Loads the index from the db if exists
			-- syncIndex comparing the last block indexed to what is in the FS
			-- If index and file system are not in sync, syncs index from the FS
	  *)  Updates blockchain info used by the APIs
*/
func newBlockfileMgr(id string, conf *Conf, indexStore *leveldbhelper.DBHandle) (*blockfileMgr, error) {
	logger.Debugf("newBlockfileMgr() initializing file-based block storage for ledger: %s ", id)
	rootDir := conf.getLedgerBlockDir(id)
	_, err := fileutil.CreateDirIfMissing(rootDir)
	if err != nil {
		panic(fmt.Sprintf("Error creating block storage root dir [%s]: %s", rootDir, err))
	}
	mgr := &blockfileMgr{rootDir: rootDir, conf: conf, db: indexStore, cache: newCache(defaultBlockCacheSizeBytes)}

	blockfilesInfo, err := mgr.loadBlkfilesInfo()
	if err != nil {
		panic(fmt.Sprintf("Could not get block file info for current block file from db: %s", err))
	}
	if blockfilesInfo == nil {
		logger.Info(`Getting block information from block storage`)
		if blockfilesInfo, err = constructBlockfilesInfo(rootDir); err != nil {
			panic(fmt.Sprintf("Could not build blockfilesInfo info from block files: %s", err))
		}
		logger.Debugf("Info constructed by scanning the blocks dir = %+v", blockfilesInfo)
	} else {
		logger.Debug(`Syncing block information from block storage (if needed)`)
		syncBlockfilesInfoFromFS(rootDir, blockfilesInfo)
	}
	err = mgr.saveBlkfilesInfo(blockfilesInfo, true)
	if err != nil {
		panic(fmt.Sprintf("Could not save next block file info to db: %s", err))
	}

	currentFileWriter, err := newBlockfileWriter(deriveBlockfilePath(rootDir, blockfilesInfo.latestFileNumber))
	if err != nil {
		panic(fmt.Sprintf("Could not open writer to current file: %s", err))
	}
	err = currentFileWriter.truncateFile(blockfilesInfo.latestFileSize)
	if err != nil {
		panic(fmt.Sprintf("Could not truncate current file to known size in db: %s", err))
	}
	mgr.index = newBlockIndex(indexStore)

	mgr.blockfilesInfo = blockfilesInfo
	mgr.currentFileWriter = currentFileWriter
	mgr.blkfilesInfoCond = sync.NewCond(&sync.Mutex{})

	if err := mgr.syncIndex(); err != nil {
		return nil, err
	}

	bcInfo := &common.BlockchainInfo{}

	if !blockfilesInfo.noBlockFiles {
		lastBlockHeader, err := mgr.retrieveBlockHeaderByNumber(blockfilesInfo.lastPersistedBlock)
		if err != nil {
			panic(fmt.Sprintf("Could not retrieve header of the last block form file: %s", err))
		}
		// update bcInfo with lastPersistedBlock
		bcInfo.Height = blockfilesInfo.lastPersistedBlock + 1
		bcInfo.CurrentBlockHash = protoutil.BlockHeaderHash(lastBlockHeader)
		bcInfo.PreviousBlockHash = lastBlockHeader.PreviousHash
	}
	mgr.bcInfo.Store(bcInfo)
	return mgr, nil
}

func syncBlockfilesInfoFromFS(rootDir string, blkfilesInfo *blockfilesInfo) {
	logger.Debugf("Starting blockfilesInfo=%s", blkfilesInfo)
	// Checks if the file suffix of where the last block was written exists
	filePath := deriveBlockfilePath(rootDir, blkfilesInfo.latestFileNumber)
	exists, size, err := fileutil.FileExists(filePath)
	if err != nil {
		panic(fmt.Sprintf("Error in checking whether file [%s] exists: %s", filePath, err))
	}
	logger.Debugf("status of file [%s]: exists=[%t], size=[%d]", filePath, exists, size)
	// Test is !exists because when file number is first used the file does not exist yet
	// checks that the file exists and that the size of the file is what is stored in blockfilesInfo
	// status of file [<blkstorage_location>/blocks/blockfile_000000]: exists=[false], size=[0]
	if !exists || int(size) == blkfilesInfo.latestFileSize {
		// blockfilesInfo is in sync with the file on disk
		return
	}
	// Scan the file system to verify that the blockfilesInfo stored in db is correct
	_, endOffsetLastBlock, numBlocks, err := scanForLastCompleteBlock(
		rootDir, blkfilesInfo.latestFileNumber, int64(blkfilesInfo.latestFileSize),
	)
	if err != nil {
		panic(fmt.Sprintf("Could not open current file for detecting last block in the file: %s", err))
	}
	blkfilesInfo.latestFileSize = int(endOffsetLastBlock)
	if numBlocks == 0 {
		return
	}
	// Updates the blockfilesInfo for the actual last block number stored and it's end location
	if blkfilesInfo.noBlockFiles {
		blkfilesInfo.lastPersistedBlock = uint64(numBlocks - 1)
	} else {
		blkfilesInfo.lastPersistedBlock += uint64(numBlocks)
	}
	blkfilesInfo.noBlockFiles = false
	logger.Debugf("blockfilesInfo after updates by scanning the last file segment:%s", blkfilesInfo)
}

func deriveBlockfilePath(rootDir string, suffixNum int) string {
	return rootDir + "/" + blockfilePrefix + fmt.Sprintf("%06d", suffixNum)
}

func (mgr *blockfileMgr) close() {
	mgr.currentFileWriter.close()
}

func (mgr *blockfileMgr) moveToNextFile() {
	blkfilesInfo := &blockfilesInfo{
		latestFileNumber:   mgr.blockfilesInfo.latestFileNumber + 1,
		latestFileSize:     0,
		lastPersistedBlock: mgr.blockfilesInfo.lastPersistedBlock,
	}

	nextFileWriter, err := newBlockfileWriter(
		deriveBlockfilePath(mgr.rootDir, blkfilesInfo.latestFileNumber),
	)
	if err != nil {
		panic(fmt.Sprintf("Could not open writer to next file: %s", err))
	}
	mgr.currentFileWriter.close()
	err = mgr.saveBlkfilesInfo(blkfilesInfo, true)
	if err != nil {
		panic(fmt.Sprintf("Could not save next block file info to db: %s", err))
	}
	mgr.currentFileWriter = nextFileWriter
	mgr.updateBlockfilesInfo(blkfilesInfo)
}

func (mgr *blockfileMgr) addBlock(block *common.Block) error {
	bcInfo := mgr.getBlockchainInfo()
	if block.Header.Number != bcInfo.Height {
		return errors.Errorf(
			"block number should have been %d but was %d",
			mgr.getBlockchainInfo().Height, block.Header.Number,
		)
	}

	// Add the previous hash check - Though, not essential but may not be a bad idea to
	// verify the field `block.Header.PreviousHash` present in the block.
	// This check is a simple bytes comparison and hence does not cause any observable performance penalty
	// and may help in detecting a rare scenario if there is any bug in the ordering service.
	if !bytes.Equal(block.Header.PreviousHash, bcInfo.CurrentBlockHash) {
		return errors.Errorf(
			"unexpected Previous block hash. Expected PreviousHash = [%x], PreviousHash referred in the latest block= [%x]",
			bcInfo.CurrentBlockHash, block.Header.PreviousHash,
		)
	}
	blockBytes := serializeBlock(block)
	blockHash := protoutil.BlockHeaderHash(block.Header)
	currentOffset := mgr.blockfilesInfo.latestFileSize

	blockBytesLen := len(blockBytes)
	blockBytesEncodedLen := protowire.AppendVarint(nil, uint64(blockBytesLen))
	totalBytesToAppend := blockBytesLen + len(blockBytesEncodedLen)

	// Determine if we need to start a new file since the size of this block
	// exceeds the amount of space left in the current file
	if currentOffset+totalBytesToAppend > mgr.conf.maxBlockfileSize {
		mgr.moveToNextFile()
		currentOffset = 0
	}
	// append blockBytesEncodedLen to the file
	err := mgr.currentFileWriter.append(blockBytesEncodedLen, false)
	if err == nil {
		// append the actual block bytes to the file
		err = mgr.currentFileWriter.append(blockBytes, true)
	}
	if err != nil {
		truncateErr := mgr.currentFileWriter.truncateFile(mgr.blockfilesInfo.latestFileSize)
		if truncateErr != nil {
			panic(fmt.Sprintf("Could not truncate current file to known size after an error during block append: %s", err))
		}
		return errors.WithMessage(err, "error appending block to file")
	}

	defer mgr.cache.put(block, blockBytesLen)

	// Update the blockfilesInfo with the results of adding the new block
	currentBlkfilesInfo := mgr.blockfilesInfo
	newBlkfilesInfo := &blockfilesInfo{
		latestFileNumber:   currentBlkfilesInfo.latestFileNumber,
		latestFileSize:     currentBlkfilesInfo.latestFileSize + totalBytesToAppend,
		noBlockFiles:       false,
		lastPersistedBlock: block.Header.Number,
	}
	// save the blockfilesInfo in the database
	if err = mgr.saveBlkfilesInfo(newBlkfilesInfo, false); err != nil {
		truncateErr := mgr.currentFileWriter.truncateFile(currentBlkfilesInfo.latestFileSize)
		if truncateErr != nil {
			panic(fmt.Sprintf("Error in truncating current file to known size after an error in saving blockfiles info: %s", err))
		}
		return errors.WithMessage(err, "error saving blockfiles file info to db")
	}

	// Index block file location pointer updated with file suffex and offset for the new block
	blockFLP := &fileLocPointer{fileSuffixNum: newBlkfilesInfo.latestFileNumber}
	blockFLP.offset = currentOffset
	// save the index in the database
	if err = mgr.index.indexBlock(&blockIdxInfo{blockNum: block.Header.Number, flp: blockFLP}); err != nil {
		return err
	}

	// update the blockfilesInfo (for storage) and the blockchain info (for APIs) in the manager
	mgr.updateBlockfilesInfo(newBlkfilesInfo)
	mgr.updateBlockchainInfo(blockHash, block)
	return nil
}

func (mgr *blockfileMgr) syncIndex() error {
	nextIndexableBlock := uint64(0)
	lastBlockIndexed, err := mgr.index.getLastBlockIndexed()
	if err != nil {
		if !errors.Is(err, errIndexSavePointKeyNotPresent) {
			return err
		}
	} else {
		nextIndexableBlock = lastBlockIndexed + 1
	}

	if mgr.blockfilesInfo.noBlockFiles {
		logger.Debug("No block files present. This happens when there has not been any blocks added to the ledger yet")
		return nil
	}

	if nextIndexableBlock == mgr.blockfilesInfo.lastPersistedBlock+1 {
		logger.Debug("Both the block files and indices are in sync.")
		return nil
	}

	// TODO: when ledger pruning lands, the rebuild scan can no longer assume the block files start at
	// blockfile_000000. It has to begin at the first file that survived pruning, and if that file is
	// absent the marker and the files disagree, which is unrecoverable because the pruned blocks are gone.
	startFileNum := 0
	startOffset := 0
	skipFirstBlock := false
	endFileNum := mgr.blockfilesInfo.latestFileNumber

	firstAvailableBlkNum, err := retrieveFirstBlockNumFromFile(mgr.rootDir, 0)
	if err != nil {
		return err
	}

	if nextIndexableBlock > firstAvailableBlkNum {
		logger.Debugf("Last block indexed [%d], Last block present in block files [%d]", lastBlockIndexed, mgr.blockfilesInfo.lastPersistedBlock)
		var flp *fileLocPointer
		if flp, err = mgr.index.getBlockLocByBlockNum(lastBlockIndexed); err != nil {
			return err
		}
		startFileNum = flp.fileSuffixNum
		startOffset = flp.locPointer.offset
		skipFirstBlock = true
	}

	logger.Infof("Start building index from block [%d] to last block [%d]", nextIndexableBlock, mgr.blockfilesInfo.lastPersistedBlock)

	// open a blockstream to the file location that was stored in the index
	var stream *blockStream
	if stream, err = newBlockStream(mgr.rootDir, startFileNum, int64(startOffset), endFileNum); err != nil {
		return err
	}
	var blockBytes []byte
	var blockPlacementInfo *blockPlacementInfo

	if skipFirstBlock {
		if blockBytes, _, err = stream.nextBlockBytesAndPlacementInfo(); err != nil {
			return err
		}
		if blockBytes == nil {
			return errors.Errorf("block bytes for block num = [%d] should not be nil here. The indexes for the block are already present",
				lastBlockIndexed)
		}
	}

	// Should be at the last block already, but go ahead and loop looking for next blockBytes.
	// If there is another block, add it to the index.
	// This will ensure block indexes are correct, for example if peer had crashed before indexes got updated.
	blockIdxInfo := &blockIdxInfo{}
	for {
		if blockBytes, blockPlacementInfo, err = stream.nextBlockBytesAndPlacementInfo(); err != nil {
			return err
		}
		if blockBytes == nil {
			break
		}
		blockHeader, err := extractSerializedBlockHeader(blockBytes)
		if err != nil {
			return err
		}

		// Update the blockIndexInfo with what was actually stored in file system
		blockIdxInfo.blockNum = blockHeader.Number
		blockIdxInfo.flp = &fileLocPointer{
			fileSuffixNum: blockPlacementInfo.fileNum,
			locPointer:    locPointer{offset: int(blockPlacementInfo.blockStartOffset)},
		}

		logger.Debugf("syncIndex() indexing block [%d]", blockIdxInfo.blockNum)
		if err = mgr.index.indexBlock(blockIdxInfo); err != nil {
			return err
		}
		if blockIdxInfo.blockNum%10000 == 0 {
			logger.Infof("Indexed block number [%d]", blockIdxInfo.blockNum)
		}
	}
	logger.Infof("Finished building index. Last block indexed [%d]", blockIdxInfo.blockNum)
	return nil
}

func (mgr *blockfileMgr) getBlockchainInfo() *common.BlockchainInfo {
	return mgr.bcInfo.Load().(*common.BlockchainInfo)
}

func (mgr *blockfileMgr) updateBlockfilesInfo(blkfilesInfo *blockfilesInfo) {
	mgr.blkfilesInfoCond.L.Lock()
	defer mgr.blkfilesInfoCond.L.Unlock()
	mgr.blockfilesInfo = blkfilesInfo
	logger.Debugf("Broadcasting about update blockfilesInfo: %s", blkfilesInfo)
	mgr.blkfilesInfoCond.Broadcast()
}

func (mgr *blockfileMgr) updateBlockchainInfo(latestBlockHash []byte, latestBlock *common.Block) {
	currentBCInfo := mgr.getBlockchainInfo()
	newBCInfo := &common.BlockchainInfo{
		Height:            currentBCInfo.Height + 1,
		CurrentBlockHash:  latestBlockHash,
		PreviousBlockHash: latestBlock.Header.PreviousHash,
	}

	mgr.bcInfo.Store(newBCInfo)
}

// TODO: when ledger pruning lands, the reads keyed by block number need a front-boundary guard again:
// a block below the prune point must be reported as pruned rather than as missing from the index. The
// guard belongs at the top of retrieveBlockByNumber, retrieveBlockHeaderByNumber and retrieveBlocks,
// where the snapshot-bootstrap guard used to sit.
func (mgr *blockfileMgr) retrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	logger.Debugf("retrieveBlockByNumber() - blockNum = [%d]", blockNum)

	// interpret math.MaxUint64 as a request for last block
	if blockNum == math.MaxUint64 {
		blockNum = mgr.getBlockchainInfo().Height - 1
	}
	loc, err := mgr.index.getBlockLocByBlockNum(blockNum)
	if err != nil {
		return nil, err
	}
	return mgr.fetchBlock(loc)
}

func (mgr *blockfileMgr) retrieveBlockHeaderByNumber(blockNum uint64) (*common.BlockHeader, error) {
	logger.Debugf("retrieveBlockHeaderByNumber() - blockNum = [%d]", blockNum)
	loc, err := mgr.index.getBlockLocByBlockNum(blockNum)
	if err != nil {
		return nil, err
	}
	blockBytes, err := mgr.fetchBlockBytes(loc)
	if err != nil {
		return nil, err
	}

	return extractSerializedBlockHeader(blockBytes)
}

func (mgr *blockfileMgr) retrieveBlocks(startNum uint64) (*blocksItr, error) {
	return newBlockItr(mgr, startNum), nil
}

func (mgr *blockfileMgr) fetchBlock(lp *fileLocPointer) (*common.Block, error) {
	blockBytes, err := mgr.fetchBlockBytes(lp)
	if err != nil {
		return nil, err
	}
	block, err := deserializeBlock(blockBytes)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (mgr *blockfileMgr) fetchBlockBytes(lp *fileLocPointer) ([]byte, error) {
	stream, err := newBlockfileStream(mgr.rootDir, lp.fileSuffixNum, int64(lp.offset))
	if err != nil {
		return nil, err
	}
	defer stream.close()
	b, err := stream.nextBlockBytes()
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Get the current blockfilesInfo information that is stored in the database
func (mgr *blockfileMgr) loadBlkfilesInfo() (*blockfilesInfo, error) {
	var b []byte
	var err error
	if b, err = mgr.db.Get(blkMgrInfoKey); b == nil || err != nil {
		return nil, err
	}
	i := &blockfilesInfo{}
	if err = i.unmarshal(b); err != nil {
		return nil, err
	}
	logger.Debugf("loaded blockfilesInfo:%s", i)
	return i, nil
}

func (mgr *blockfileMgr) saveBlkfilesInfo(i *blockfilesInfo, sync bool) error {
	b := i.marshal()
	if err := mgr.db.Put(blkMgrInfoKey, b, sync); err != nil {
		return err
	}
	return nil
}

// scanForLastCompleteBlock scan a given block file and detects the last offset in the file
// after which there may lie a block partially written (towards the end of the file in a crash scenario).
func scanForLastCompleteBlock(rootDir string, fileNum int, startingOffset int64) ([]byte, int64, int, error) {
	// scan the passed file number suffix starting from the passed offset to find the last completed block
	numBlocks := 0
	var lastBlockBytes []byte
	blockStream, errOpen := newBlockfileStream(rootDir, fileNum, startingOffset)
	if errOpen != nil {
		return nil, 0, 0, errOpen
	}
	defer blockStream.close()
	var errRead error
	var blockBytes []byte
	for {
		blockBytes, errRead = blockStream.nextBlockBytes()
		if blockBytes == nil || errRead != nil {
			break
		}
		lastBlockBytes = blockBytes
		numBlocks++
	}
	if errRead == ErrUnexpectedEndOfBlockfile {
		logger.Debugf(`Error:%s
		The error may happen if a crash has happened during block appending.
		Resetting error to nil and returning current offset as a last complete block's end offset`, errRead)
		errRead = nil
	}
	logger.Debugf("scanForLastCompleteBlock(): last complete block ends at offset=[%d]", blockStream.currentOffset)
	return lastBlockBytes, blockStream.currentOffset, numBlocks, errRead
}

// blockfilesInfo maintains the summary about the blockfiles
type blockfilesInfo struct {
	latestFileNumber   int
	latestFileSize     int
	noBlockFiles       bool
	lastPersistedBlock uint64
}

func (i *blockfilesInfo) marshal() []byte {
	var buf []byte
	buf = protowire.AppendVarint(buf, uint64(i.latestFileNumber))
	buf = protowire.AppendVarint(buf, uint64(i.latestFileSize))
	buf = protowire.AppendVarint(buf, i.lastPersistedBlock)
	var noBlockFilesMarker uint64
	if i.noBlockFiles {
		noBlockFilesMarker = 1
	}
	buf = protowire.AppendVarint(buf, noBlockFilesMarker)

	return buf
}

func (i *blockfilesInfo) unmarshal(b []byte) error {
	var (
		val                uint64
		noBlockFilesMarker uint64
		position           int
	)

	val, n := protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return protowire.ParseError(n)
	}
	position += n
	i.latestFileNumber = int(val)

	val, n = protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return protowire.ParseError(n)
	}
	position += n
	i.latestFileSize = int(val)

	val, n = protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return protowire.ParseError(n)
	}
	position += n
	i.lastPersistedBlock = val
	noBlockFilesMarker, n = protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return protowire.ParseError(n)
	}
	i.noBlockFiles = noBlockFilesMarker == 1
	return nil
}

func (i *blockfilesInfo) String() string {
	return fmt.Sprintf("latestFileNumber=[%d], latestFileSize=[%d], noBlockFiles=[%t], lastPersistedBlock=[%d]",
		i.latestFileNumber, i.latestFileSize, i.noBlockFiles, i.lastPersistedBlock)
}
