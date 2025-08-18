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
			BlockHeader: &state.BlockHeader{
				Number:   0,
				PrevHash: nil,
				Digest:   genesisDigest,
			},
			DecisionNum: 0,
			BatchIndex:  0,
			BatchCount:  1,
		},
	}
	orderedBatchAttestationCreator := &OrderedBatchAttestationCreator{
		prevBa:     ba,
		headerHash: calculateHeaderHash(ba.OrderingInformation.BlockHeader),
	}
	return orderedBatchAttestationCreator, ba
}

func (obac *OrderedBatchAttestationCreator) Append(batchId types.BatchID, decisionNum types.DecisionNum, batchIndex, batchCount int) types.OrderedBatchAttestation {
	if decisionNum-types.DecisionNum(obac.prevBa.OrderingInformation.Number) > 1 {
		panic("Cannot create non-consecutive BA")
	}
	ba := &state.AvailableBatchOrdered{
		AvailableBatch: state.NewAvailableBatch(batchId.Primary(), batchId.Shard(), batchId.Seq(), batchId.Digest()),
		OrderingInformation: &state.OrderingInformation{
			BlockHeader: &state.BlockHeader{
				Number:   uint64(decisionNum),
				PrevHash: obac.headerHash,
				Digest:   batchId.Digest(),
			},
			DecisionNum: decisionNum,
			BatchIndex:  batchIndex,
			BatchCount:  batchCount,
		},
	}
	obac.headerHash = calculateHeaderHash(ba.OrderingInformation.BlockHeader)
	obac.prevBa = ba
	return ba
}

func calculateHeaderHash(bh *state.BlockHeader) []byte {
	header := &common.BlockHeader{
		Number:       bh.Number,
		PreviousHash: bh.PrevHash,
		DataHash:     bh.Digest,
	}
	return protoutil.BlockHeaderHash(header)
}
