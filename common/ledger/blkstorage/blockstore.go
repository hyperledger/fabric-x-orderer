/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/ledger"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/util/leveldbhelper"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
)

// BlockStore - filesystem based implementation for `BlockStore`
type BlockStore struct {
	id      string
	conf    *Conf
	fileMgr *blockfileMgr
	stats   *ledgerStats
}

// newBlockStore constructs a `BlockStore`
func newBlockStore(id string, conf *Conf, dbHandle *leveldbhelper.DBHandle, stats *stats) (*BlockStore, error) {
	fileMgr, err := newBlockfileMgr(id, conf, dbHandle)
	if err != nil {
		return nil, err
	}

	// create ledgerStats and initialize blockchain_height stat
	ledgerStats := stats.ledgerStats(id)
	info := fileMgr.getBlockchainInfo()
	ledgerStats.updateBlockchainHeight(info.Height)

	return &BlockStore{id, conf, fileMgr, ledgerStats}, nil
}

// AddBlock adds a new block
func (store *BlockStore) AddBlock(block *common.Block) error {
	// track elapsed time to collect block commit time
	startBlockCommit := time.Now()
	result := store.fileMgr.addBlock(block)
	elapsedBlockCommit := time.Since(startBlockCommit)

	store.updateBlockStats(block.Header.Number, elapsedBlockCommit)

	return result
}

// GetBlockchainInfo returns the current info about blockchain
func (store *BlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	return store.fileMgr.getBlockchainInfo(), nil
}

// RetrieveBlocks returns an iterator that can be used for iterating over a range of blocks
func (store *BlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	itr, err := store.fileMgr.retrieveBlocks(startNum)
	if err != nil {
		// Returned explicitly rather than as a typed nil, so that a caller testing the iterator instead of
		// the error does not get something that looks like an iterator and panics on first use.
		return nil, err
	}
	return itr, nil
}

// RetrieveBlockByNumber returns the block at a given blockchain height
func (store *BlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	return store.fileMgr.retrieveBlockByNumber(blockNum)
}

// PruneBefore removes the block files whose blocks all lie below blockNum, advancing a durable bound so
// that reads below it fail with ErrPruned. blockNum is an upper bound: files are removed on whole-file
// boundaries, so the bound lands at or below it. Height() is unchanged, and the last block always survives.
// Idempotent and monotone: a request below the bound in force does not lower it.
func (store *BlockStore) PruneBefore(blockNum uint64) error {
	return store.fileMgr.pruneBefore(blockNum)
}

// FirstAvailableBlockNumber returns the lowest block number this store can serve. It is 0 unless the
// ledger has been pruned. Reads below it fail with ErrPruned.
func (store *BlockStore) FirstAvailableBlockNumber() uint64 {
	return store.fileMgr.pruner.firstReadableBlockNum()
}

// Shutdown shuts down the block store
func (store *BlockStore) Shutdown() {
	logger.Debugf("closing fs blockStore:%s", store.id)
	store.fileMgr.close()
}

func (store *BlockStore) updateBlockStats(blockNum uint64, blockstorageCommitTime time.Duration) {
	store.stats.updateBlockchainHeight(blockNum + 1)
	store.stats.updateBlockstorageCommitTime(blockstorageCommitTime)
}
