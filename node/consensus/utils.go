/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
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

func VerifyDataCommonBlock(block *common.Block, blockNum uint64, prevHash []byte, batchID arma_types.BatchID, decisionNum arma_types.DecisionNum, batchCount, batchIndex int, lastConfigBlockNum uint64) error {
	// verify hash chain
	if hex.EncodeToString(block.Header.PreviousHash) != hex.EncodeToString(prevHash) {
		return errors.Errorf("proposed block header prev hash %s isn't equal to computed prev hash %s", hex.EncodeToString(block.Header.PreviousHash), hex.EncodeToString(prevHash))
	}

	// verify block number
	if block.Header.Number != blockNum {
		return errors.Errorf("proposed block header number %d isn't equal to computed number %d", block.Header.Number, blockNum)
	}

	// verify orderer metadata
	computedBlockMetadata, err := ledger.AssemblerBlockMetadataToBytes(batchID, &state.OrderingInformation{DecisionNum: decisionNum, BatchCount: batchCount, BatchIndex: batchIndex}, 0)
	if err != nil {
		panic(fmt.Errorf("failed to invoke AssemblerBlockMetadataToBytes: %s", err))
	}

	if block.Metadata == nil || block.Metadata.Metadata == nil {
		return errors.Errorf("proposed block metadata is nil")
	}

	if !bytes.Equal(computedBlockMetadata, block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER]) {
		return errors.Errorf("proposed block metadata isn't equal to computed metadata")
	}

	// verify last config
	rawLastConfig, err := protoutil.GetMetadataFromBlock(block, common.BlockMetadataIndex_LAST_CONFIG)
	if err != nil {
		return errors.Wrap(err, "failed getting proposed block metadata last config")
	}
	lastConf := &common.LastConfig{}
	if err := proto.Unmarshal(rawLastConfig.Value, lastConf); err != nil {
		return errors.Wrap(err, "failed unmarshaling proposed block metadata last config")
	}
	if lastConf.Index != lastConfigBlockNum {
		return errors.Errorf("last config in block metadata points to %d but our persisted last config is %d", lastConf.Index, lastConfigBlockNum)
	}

	return nil
}
