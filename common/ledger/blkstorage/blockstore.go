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
	return store.fileMgr.retrieveBlocks(startNum)
}

// RetrieveBlockByNumber returns the block at a given blockchain height
func (store *BlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	return store.fileMgr.retrieveBlockByNumber(blockNum)
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
