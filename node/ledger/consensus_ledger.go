/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blkstorage"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blockledger"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blockledger/fileledger"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/append_listener.go . AppendListener
type AppendListener interface {
	OnAppend(block *common.Block)
}

type ConsensusLedger struct {
	blockStore     *blkstorage.BlockStore
	provider       *blkstorage.BlockStoreProvider
	ledger         blockledger.ReadWriter
	appendListener AppendListener
}

func NewConsensusLedger(ledgerDir string) (*ConsensusLedger, error) {
	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(ledgerDir, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{})
	if err != nil {
		return nil, errors.Wrap(err, "failed creating block provider")
	}

	blockStore, err := provider.Open("consensus")
	if err != nil {
		return nil, errors.Wrap(err, "failed creating consensus ledger")
	}

	fl := fileledger.NewFileLedger(blockStore)

	consensusLedger := &ConsensusLedger{
		blockStore: blockStore,
		provider:   provider,
		ledger:     fl,
	}

	return consensusLedger, nil
}

// TODO BFTSynch remove will not be used once we change to the BFT synchronizer
func (l *ConsensusLedger) RegisterAppendListener(listener AppendListener) {
	l.appendListener = listener
}

func (c *ConsensusLedger) Append(block *common.Block) {
	err := c.ledger.Append(block)
	if err != nil {
		panic(err)
	}

	if c.appendListener != nil {
		c.appendListener.OnAppend(block)
	}
}

func (c *ConsensusLedger) Iterator(startType *ab.SeekPosition) (blockledger.Iterator, uint64) {
	return c.ledger.Iterator(startType)
}

// WriteBlock commits a block to the ledger.
// ConsenterSupport API for BFT synchronizer, will be called by the synchronizer when it needs to commit a block to the ledger.
func (c *ConsensusLedger) WriteBlock(block *common.Block) {
	err := c.ledger.Append(block)
	if err != nil {
		panic(err)
	}
}

func (c *ConsensusLedger) Height() uint64 {
	return c.ledger.Height()
}

func (c *ConsensusLedger) RetrieveBlockByNumber(blockNumber uint64) (*common.Block, error) {
	return c.ledger.RetrieveBlockByNumber(blockNumber)
}

func (c *ConsensusLedger) Close() {
	c.blockStore.Shutdown()
	c.provider.Close()
}
