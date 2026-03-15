/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blkstorage"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blockledger"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blockledger/fileledger"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/append_listener.go . AppendListener
type AppendListener interface {
	OnAppend(block *common.Block)
}

type ConsensusLedger struct {
	blockStore     *blkstorage.BlockStore
	provider       *blkstorage.BlockStoreProvider
	prevHash       []byte
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

	height := fl.Height()
	if height > 0 {
		block, err := fl.RetrieveBlockByNumber(height - 1)
		if err != nil {
			return nil, errors.Wrap(err, "failed retrieving last block")
		}
		consensusLedger.prevHash = protoutil.BlockHeaderHash(block.Header)
	}

	return consensusLedger, nil
}

// TODO BFTSynch remove will not be used once we change to the BFT synchronizer
func (l *ConsensusLedger) RegisterAppendListener(listener AppendListener) {
	l.appendListener = listener
}

func (c *ConsensusLedger) Append(number uint64, proposal smartbft_types.Proposal, signatures []smartbft_types.Signature, decisionNumOfLastConfigBlock uint64) {
	proposalBytes := state.DecisionToBytes(proposal, nil) // TODO maybe use asn1 marshal proposal instead

	data := &common.BlockData{
		Data: [][]byte{proposalBytes},
	}

	block := &common.Block{
		Header: &common.BlockHeader{
			Number:       number,
			DataHash:     protoutil.ComputeBlockDataHash(data),
			PreviousHash: c.prevHash,
		},
		Data: data,
	}

	protoutil.InitBlockMetadata(block)
	block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = state.DecisionSignaturesToBytes(signatures)
	block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
		LastConfig:        &common.LastConfig{Index: decisionNumOfLastConfigBlock},
		ConsenterMetadata: proposal.Metadata,
	})

	err := c.ledger.Append(block)
	if err != nil {
		panic(err)
	}

	c.prevHash = protoutil.BlockHeaderHash(block.Header)

	if c.appendListener != nil {
		c.appendListener.OnAppend(block)
	}
}

func (c *ConsensusLedger) GetPrevHash() []byte {
	return c.prevHash
}

func (c *ConsensusLedger) GetPrevHashByNumber(number uint64) ([]byte, error) {
	block, err := c.RetrieveBlockByNumber(number - 1)
	if err != nil {
		return nil, err
	}
	return protoutil.BlockHeaderHash(block.Header), nil
}

func (c *ConsensusLedger) Iterator(startType *ab.SeekPosition) (blockledger.Iterator, uint64) {
	return c.ledger.Iterator(startType)
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
