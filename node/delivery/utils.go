/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery

import (
	"time"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/pkg/errors"
)

func extractHeaderFromBlock(block *common.Block, logger *flogging.FabricLogger) *state.Header {
	if block == nil {
		logger.Panic("Block is nil")
	}
	if block.GetData() == nil {
		logger.Panicf("Block data is nil for block: %d", block.GetHeader().GetNumber())
	}
	if len(block.GetData().GetData()) == 0 {
		logger.Panicf("Block data is empty for block: %d", block.GetHeader().GetNumber())
	}

	proposal, err := state.BytesToProposal(block.GetData().GetData()[0])
	if err != nil {
		logger.Panicf("Failed deserializing consenter block: %s", err)
	}

	header := &state.Header{}
	if err := header.Deserialize(proposal.Header); err != nil {
		logger.Panicf("Failed deserializing proposal header: %s", err)
	}
	return header
}

func extractHeaderAndSigsFromBlock(block *common.Block) (*state.Header, [][]smartbft_types.Signature, error) {
	if block == nil {
		return nil, nil, errors.New("Block is nil")
	}
	if block.GetData() == nil {
		return nil, nil, errors.Errorf("Block data is nil for block: %d", block.GetHeader().GetNumber())
	}
	if len(block.GetData().GetData()) == 0 {
		return nil, nil, errors.Errorf("Block data is empty for block: %d", block.GetHeader().GetNumber())
	}

	// An optimization would be to unmarshal just the header and sigs, skipping the proposal payload and metadata which we don't need here.
	// An even better optimization would be to ask for content type that does not include the proposal payload and metadata.
	proposal, err := state.BytesToProposal(block.GetData().GetData()[0])
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to extract proposal from block: %d", block.GetHeader().GetNumber())
	}

	stateHeader := &state.Header{}
	if err := stateHeader.Deserialize(proposal.Header); err != nil {
		return nil, nil, errors.Wrapf(err, "failed parsing consensus/state.Header from block: %d", block.GetHeader().GetNumber())
	}

	if stateHeader.Num == 0 { // this is the genesis block
		sigs := make([][]smartbft_types.Signature, 1) // no signatures
		return stateHeader, sigs, nil
	}

	// Check if block metadata is nil
	if block.GetMetadata() == nil {
		return nil, nil, errors.Errorf("block metadata is nil for block: %d", block.GetHeader().GetNumber())
	}

	// Check if metadata array is nil or index is out of range
	metadata := block.GetMetadata().GetMetadata()
	if metadata == nil {
		return nil, nil, errors.Errorf("block metadata array is nil for block: %d", block.GetHeader().GetNumber())
	}
	if int(common.BlockMetadataIndex_SIGNATURES) >= len(metadata) {
		return nil, nil, errors.Errorf("block metadata index %d is out of range (length: %d) for block: %d",
			common.BlockMetadataIndex_SIGNATURES, len(metadata), block.GetHeader().GetNumber())
	}

	compoundSigs, err := state.BytesToDecisionSignatures(metadata[common.BlockMetadataIndex_SIGNATURES])
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to extract signatures from block: %d", block.GetHeader().GetNumber())
	}

	sigs, err := state.UnpackBlockHeaderSigs(compoundSigs, len(stateHeader.AvailableCommonBlocks))
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to extract header signatures from compound signature, block %d", block.GetHeader().GetNumber())
	}

	return stateHeader, sigs, nil
}

func signersFromSigs(sigs []smartbft_types.Signature) []uint64 {
	var signers []uint64
	for _, sig := range sigs {
		signers = append(signers, sig.ID)
	}
	return signers
}

func clientConfig(TLSCACerts []config.RawBytes, tlsKey, tlsCert []byte) comm.ClientConfig {
	var tlsCAs [][]byte
	for _, cert := range TLSCACerts {
		tlsCAs = append(tlsCAs, cert)
	}

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               tlsKey,
			Certificate:       tlsCert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}

func createAssemblerConsensusPosition(oi *state.OrderingInformation) types.AssemblerConsensusPosition {
	// if we start with an empty ledger, and last config block is not the genesis block, we have oi=nil
	if oi == nil {
		return types.AssemblerConsensusPosition{
			DecisionNum: 0,
		}
	}

	if oi.BatchIndex != oi.BatchCount-1 {
		return types.AssemblerConsensusPosition{
			DecisionNum: oi.DecisionNum,
			BatchIndex:  oi.BatchIndex + 1,
		}
	}
	return types.AssemblerConsensusPosition{
		DecisionNum: oi.DecisionNum + 1,
	}
}
