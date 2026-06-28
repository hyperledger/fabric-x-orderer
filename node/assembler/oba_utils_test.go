/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
)

type OrderedBatchAttestationCreator struct {
	prevBa     *state.AvailableBatchOrdered
	headerHash []byte
}

// NewOrderedBatchAttestationCreatorWithGenesis seeds the hash chain from the provided genesis
// block so that subsequent Append calls produce blocks whose PreviousHash matches the real
// ledger (e.g. an armageddon-generated genesis block).
func NewOrderedBatchAttestationCreatorWithGenesis(genesisBlock *common.Block) (*OrderedBatchAttestationCreator, *state.AvailableBatchOrdered) {
	genesisDigest := protoutil.ComputeBlockDataHash(genesisBlock.GetData())
	ba := &state.AvailableBatchOrdered{
		AvailableBatch: state.NewAvailableBatch(0, types.ShardIDConsensus, 0, []byte{}),
		OrderingInformation: &state.OrderingInformation{
			CommonBlock: &common.Block{Header: &common.BlockHeader{Number: 0, PreviousHash: nil, DataHash: genesisDigest}},
			DecisionNum: 0,
			BatchIndex:  0,
			BatchCount:  1,
		},
	}
	return &OrderedBatchAttestationCreator{
		prevBa:     ba,
		headerHash: protoutil.BlockHeaderHash(genesisBlock.Header),
	}, ba
}

func NewOrderedBatchAttestationCreator() (*OrderedBatchAttestationCreator, *state.AvailableBatchOrdered) {
	genesisBlock := utils.EmptyGenesisBlock("arma")
	genesisDigest := protoutil.ComputeBlockDataHash(genesisBlock.GetData())

	ba := &state.AvailableBatchOrdered{
		AvailableBatch: state.NewAvailableBatch(0, types.ShardIDConsensus, 0, []byte{}),
		OrderingInformation: &state.OrderingInformation{
			CommonBlock: &common.Block{Header: &common.BlockHeader{Number: 0, PreviousHash: nil, DataHash: genesisDigest}},
			DecisionNum: 0,
			BatchIndex:  0,
			BatchCount:  1,
		},
	}
	orderedBatchAttestationCreator := &OrderedBatchAttestationCreator{
		prevBa:     ba,
		headerHash: protoutil.BlockHeaderHash(ba.OrderingInformation.CommonBlock.Header),
	}
	return orderedBatchAttestationCreator, ba
}

// CurrentHeaderHash returns the header hash of the last block the creator produced (or the
// genesis block it was seeded with). It is the correct PreviousHash for the next block,
// including a config block built outside the creator (e.g. via CreateConfigCommonBlock).
func (obac *OrderedBatchAttestationCreator) CurrentHeaderHash() []byte {
	return obac.headerHash
}

// AdvancePastConfigBlock fast-forwards the creator's hash chain past a config block that was
// built outside the creator, so that a subsequent Append produces a block chaining from it.
// It updates the tracked header hash and resets prevBa so the consecutive-block check in Append
// accepts the config block number + 1 as the next decision number.
func (obac *OrderedBatchAttestationCreator) AdvancePastConfigBlock(configBlock *common.Block) {
	obac.headerHash = protoutil.BlockHeaderHash(configBlock.Header)
	obac.prevBa = &state.AvailableBatchOrdered{
		OrderingInformation: &state.OrderingInformation{
			CommonBlock: &common.Block{Header: &common.BlockHeader{Number: configBlock.GetHeader().GetNumber()}},
			DecisionNum: types.DecisionNum(configBlock.GetHeader().GetNumber()),
		},
	}
}

func (obac *OrderedBatchAttestationCreator) Append(batchId types.BatchID, decisionNum types.DecisionNum, batchIndex, batchCount int) *state.AvailableBatchOrdered {
	if decisionNum-types.DecisionNum(obac.prevBa.OrderingInformation.CommonBlock.Header.Number) > 1 {
		panic("Cannot create non-consecutive BA")
	}
	ba := &state.AvailableBatchOrdered{
		AvailableBatch: state.NewAvailableBatch(batchId.Primary(), batchId.Shard(), batchId.Seq(), batchId.Digest()),
		OrderingInformation: &state.OrderingInformation{
			CommonBlock: &common.Block{Header: &common.BlockHeader{Number: uint64(decisionNum), PreviousHash: obac.headerHash, DataHash: batchId.Digest()}},
			DecisionNum: decisionNum,
			BatchIndex:  batchIndex,
			BatchCount:  batchCount,
		},
	}
	blockMetadata, err := ledger.AssemblerBlockMetadataToBytes(batchId, &state.OrderingInformation{DecisionNum: decisionNum, BatchCount: batchCount, BatchIndex: batchIndex}, 0)
	if err != nil {
		panic("Failed to invoke AssemblerBlockMetadataToBytes")
	}
	protoutil.InitBlockMetadata(ba.OrderingInformation.CommonBlock)
	ba.OrderingInformation.CommonBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = blockMetadata
	obac.headerHash = protoutil.BlockHeaderHash(ba.OrderingInformation.CommonBlock.Header)
	obac.prevBa = ba
	return ba
}
