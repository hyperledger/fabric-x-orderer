/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

func toBeSignedBAF(baf arma_types.BatchAttestationFragment) []byte {
	simpleBAF, ok := baf.(*arma_types.SimpleBatchAttestationFragment)
	if !ok {
		return nil
	}
	return simpleBAF.ToBeSigned()
}

func printEvent(event []byte) string {
	var ce state.ControlEvent
	bafd := &state.BAFDeserialize{}
	if err := ce.FromBytes(event, bafd.Deserialize); err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return ce.String()
}

func CreateDataCommonBlock(blockNum uint64, prevHash []byte, batchID arma_types.BatchID, decisionNum arma_types.DecisionNum, batchCount, batchIndex int, lastConfigBlockNum uint64) (*common.Block, error) {
	block := protoutil.NewBlock(blockNum, prevHash)
	block.Header.DataHash = batchID.Digest()
	blockMetadata, err := ledger.AssemblerBlockMetadataToBytes(batchID, &state.OrderingInformation{DecisionNum: decisionNum, BatchCount: batchCount, BatchIndex: batchIndex}, 0)
	if err != nil {
		return nil, errors.Errorf("Failed to invoke AssemblerBlockMetadataToBytes: %s", err)
	}
	block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = blockMetadata
	block.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
		Value: protoutil.MarshalOrPanic(&common.LastConfig{Index: lastConfigBlockNum}),
	})
	return block, err
}

func CreateConfigCommonBlock(blockNum uint64, prevHash []byte, decisionNum arma_types.DecisionNum, batchCount, batchIndex int, configReq []byte) (*common.Block, error) {
	configBlock := protoutil.NewBlock(blockNum, prevHash)
	configBlock.Data = &common.BlockData{Data: [][]byte{configReq}}
	configBlock.Header.DataHash = protoutil.ComputeBlockDataHash(configBlock.Data)
	blockMetadata, err := ledger.AssemblerBlockMetadataToBytes(state.NewAvailableBatch(0, arma_types.ShardIDConsensus, 0, []byte{}), &state.OrderingInformation{DecisionNum: decisionNum, BatchCount: batchCount, BatchIndex: batchIndex}, 0) // TODO fix batch count in all?
	if err != nil {
		return nil, errors.Errorf("Failed to invoke AssemblerBlockMetadataToBytes: %s", err)
	}
	configBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = blockMetadata
	configBlock.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
		Value: protoutil.MarshalOrPanic(&common.LastConfig{Index: configBlock.Header.Number}),
	})
	return configBlock, nil
}
