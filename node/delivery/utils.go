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
	if block.GetHeader() == nil {
		logger.Panic("Block header is nil")
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
		return nil, nil, errors.New("block is nil")
	}
	if block.GetHeader() == nil {
		return nil, nil, errors.New("block header is nil")
	}
	decision, err := state.ConsenterBlockToDecision(block)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to extract decision from consenter block: %d", block.GetHeader().GetNumber())
	}

	header := &state.Header{}
	if err := header.Deserialize(decision.Proposal.Header); err != nil {
		return nil, nil, errors.Wrapf(err, "failed to deserialize proposal header from block: %d", block.GetHeader().GetNumber())
	}

	if block.Header.Number == 0 { // this is the genesis block
		sigs := make([][]smartbft_types.Signature, 1) // no signatures
		return header, sigs, nil
	}

	sigs, err := state.UnpackBlockHeaderSigs(decision.Signatures, len(header.AvailableCommonBlocks))
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to extract header signatures from compound signature, block %d", block.GetHeader().GetNumber())
	}

	return header, sigs, nil
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
