/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"math"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/protoutil"
)

type OrderedBatchAttestationCreator struct {
	prevBa     *state.AvailableBatchOrdered
	headerHash []byte
}

func NewOrderedBatchAttestationCreator() (*OrderedBatchAttestationCreator, *state.AvailableBatchOrdered) {
	genesisBlock := utils.EmptyGenesisBlock("arma")
	genesisDigest := protoutil.ComputeBlockDataHash(genesisBlock.GetData())

	ba := &state.AvailableBatchOrdered{
		AvailableBatch: state.NewAvailableBatch(0, math.MaxUint16, 0, genesisDigest),
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

func (obac *OrderedBatchAttestationCreator) Append(batchId types.BatchID, decisionNum types.DecisionNum, batchIndex, batchCount int) types.OrderedBatchAttestation {
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
