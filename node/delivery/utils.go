/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery

import (
	"encoding/binary"
	"time"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/pkg/errors"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/comm"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"
)

func extractHeaderFromBlock(block *common.Block, logger types.Logger) *state.Header {
	decisionAsBytes := block.Data.Data[0]

	headerSize := decisionAsBytes[:4]

	rawHeader := decisionAsBytes[12 : 12+binary.BigEndian.Uint32(headerSize)]

	header := &state.Header{}
	if err := header.Deserialize(rawHeader); err != nil {
		logger.Panicf("Failed parsing rawHeader")
	}
	return header
}

func extractHeaderAndSigsFromBlock(block *common.Block) (*state.Header, [][]smartbft_types.Signature, error) {
	if len(block.GetData().GetData()) == 0 {
		return nil, nil, errors.New("missing data in block")
	}

	// An optimization would be to unmarshal just the header and sigs, skipping the proposal payload and metadata which we don't need here.
	// An even better optimization would be to ask for content type that does not include the proposal payload and metadata.
	proposal, compoundSigs, err := state.BytesToDecision(block.GetData().GetData()[0])
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to extract decision from block: %d", block.GetHeader().GetNumber())
	}

	stateHeader := &state.Header{}
	if err := stateHeader.Deserialize(proposal.Header); err != nil {
		return nil, nil, errors.Wrapf(err, "failed parsing consensus/state.Header from block: %d", block.GetHeader().GetNumber())
	}

	if stateHeader.Num == 0 { // this is the genesis block
		sigs := make([][]smartbft_types.Signature, 1) // no signatures
		return stateHeader, sigs, nil
	}

	sigs, err := state.UnpackBlockHeaderSigs(compoundSigs, len(stateHeader.AvailableBlocks))
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

func createAssemblerConsensusPosition(oi *state.OrderingInformation) core.AssemblerConsensusPosition {
	if oi.BatchIndex != oi.BatchCount-1 {
		return core.AssemblerConsensusPosition{
			DecisionNum: oi.DecisionNum,
			BatchIndex:  oi.BatchIndex + 1,
		}
	}
	return core.AssemblerConsensusPosition{
		DecisionNum: oi.DecisionNum + 1,
	}
}
