/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"encoding/asn1"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/pkg/errors"
)

type SigVerifierFunc func(block *common.Block, verifyData bool) error

func createErrorFunc(err error) SigVerifierFunc {
	return func(_ *common.Block, _ bool) error {
		return errors.Wrap(err, "failed to initialize sig verifier function")
	}
}

// SigVerifierCreator creates a SigVerifier out of a config envelope
type SigVerifierCreator struct {
	Logger *flogging.FabricLogger
	BCCSP  bccsp.BCCSP
}

// SigVerifierFromConfig creates a SigVerifier from the given configuration.
func (svc *SigVerifierCreator) SigVerifierFromConfig(configuration *common.ConfigEnvelope, channel string) (SigVerifierFunc, error) {
	bundle, err := channelconfig.NewBundle(channel, configuration.Config, svc.BCCSP)
	if err != nil {
		return createErrorFunc(err), err
	}

	policy, exists := bundle.PolicyManager().GetPolicy(policies.BlockValidation)
	if !exists {
		err := errors.Errorf("no `%s` policy in config block", policies.BlockValidation)
		return createErrorFunc(err), err
	}

	bftEnabled := bundle.ChannelConfig().Capabilities().ConsensusTypeBFT()
	if !bftEnabled {
		err := errors.New("consensus type is not BFT") // TODO: should I leave this?
		return createErrorFunc(err), err
	}

	var consenters []*common.Consenter
	// if bftEnabled {
	cfg, ok := bundle.OrdererConfig()
	if !ok {
		err := errors.New("no orderer section in config block")
		return createErrorFunc(err), err
	}
	consenters = cfg.Consenters()
	// }

	bsv := &BlockSigVerifier{
		Policy:     policy,
		Consenters: consenters,
		Logger:     svc.Logger,
	}

	return bsv.Verify, nil
}

type policy interface { // copied from common.policies to avoid circular import.
	// EvaluateSignedData takes a set of SignedData and evaluates whether
	// 1) the signatures are valid over the related message
	// 2) the signing identities satisfy the policy
	EvaluateSignedData(signatureSet []*protoutil.SignedData) error
}

// BlockSigVerifier can be used to verify signatures over blocks using the given policy.
type BlockSigVerifier struct {
	Policy     policy
	Consenters []*common.Consenter
	Logger     *flogging.FabricLogger
}

// Verify verifies a block's signatures using the verifier's policy.
func (v *BlockSigVerifier) Verify(block *common.Block, verifyData bool) error {
	sigsBytes := block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES]
	sigs, err := state.BytesToDecisionSignatures(sigsBytes)
	if err != nil {
		return errors.Wrap(err, "failed to deserialize signatures from metadata")
	}

	// Pre-calculate the header bytes for proposal signature
	blockHeaderBytes := protoutil.BlockHeaderBytes(block.GetHeader())

	// Pre-calculate orderer block metadata for proposal signature
	proposalMetadata := block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER]
	lastConfigIndex, err := state.GetLastConfigIndexFromConsenterBlock(block)
	if err != nil {
		return errors.Wrap(err, "failed to get last config index from consenter block")
	}
	proposalOrdererBlockMetadata := protoutil.MarshalOrPanic(&common.OrdererBlockMetadata{
		LastConfig:        &common.LastConfig{Index: lastConfigIndex},
		ConsenterMetadata: proposalMetadata,
	})

	var hdr state.Header
	var availableCommonBlocksHeaderBytes [][]byte
	if verifyData {
		proposalBytes := block.Data.Data[0]
		proposal, err := state.BytesToProposal(proposalBytes)
		if err != nil {
			return errors.Wrap(err, "failed to deserialize proposal from block data")
		}
		if err := hdr.Deserialize(proposal.Header); err != nil {
			return errors.Wrap(err, "failed deserializing proposal header")
		}
		// Pre-calculate the available common blocks block header bytes
		availableCommonBlocksHeaderBytes = make([][]byte, 0, len(hdr.AvailableCommonBlocks))
		for _, availableBlock := range hdr.AvailableCommonBlocks {
			availableCommonBlocksHeaderBytes = append(availableCommonBlocksHeaderBytes, protoutil.BlockHeaderBytes(availableBlock.GetHeader()))
		}
	}
	// TODO: shouldn't we check that the message contains the correct block header bytes and metadata?

	signatureSets := make([][]*protoutil.SignedData, 0, len(hdr.AvailableCommonBlocks)+1)
	for range len(hdr.AvailableCommonBlocks) + 1 {
		signatureSets = append(signatureSets, make([]*protoutil.SignedData, 0, len(sigs)))
	}
	for _, sig := range sigs {
		var values [][]byte
		if _, err := asn1.Unmarshal(sig.Value, &values); err != nil {
			v.Logger.Warnf("failed to unmarshal signature values with id %d; err: %s", sig.ID, err)
			continue
		}
		var msgs [][]byte
		if _, err := asn1.Unmarshal(sig.Msg, &msgs); err != nil {
			v.Logger.Warnf("failed to unmarshal signature msgs with id %d; err: %s", sig.ID, err)
			continue
		}
		if len(msgs) != len(values) {
			v.Logger.Warnf("signature with id %d has different number of values and msgs", sig.ID)
			continue
		}
		if len(msgs) == 0 {
			v.Logger.Warnf("signature with id %d has no messages", sig.ID)
			continue
		}
		if verifyData {
			if len(msgs) != len(hdr.AvailableCommonBlocks)+1 {
				v.Logger.Warnf("signature with id %d has different number of messages than available common blocks", sig.ID)
				continue
			}
		}
		proposalMsg := &protoutil.MessageToSign{}
		if err := proposalMsg.ASN1Unmarshal(msgs[0]); err != nil {
			v.Logger.Warnf("failed to unmarshal the first signature msg with id %d; err: %s", sig.ID, err)
			continue
		}
		// verify proposal message identifier header
		idHeader, err := protoutil.UnmarshalIdentifierHeader(proposalMsg.IdentifierHeader)
		if err != nil {
			v.Logger.Warnf("failed to unmarshal identifier header from proposal message; err: %s", err)
			continue
		}
		if uint64(idHeader.GetIdentifier()) != sig.ID {
			v.Logger.Warnf("signature ID %d does not match identifier header ID %d in proposal message", sig.ID, idHeader.GetIdentifier())
			continue
		}
		signerIdentity := v.searchConsenterIdentityByID(idHeader.GetIdentifier())
		if signerIdentity == nil {
			v.Logger.Warnf("signature with id %d has unknown consenter identity", sig.ID)
			continue
		}

		if verifyData {
			signatures, err := v.collectBlocksSignatures(msgs[1:], values[1:], availableCommonBlocksHeaderBytes, sig.ID)
			if err != nil {
				v.Logger.Warnf("failed to collect signatures for all blocks with signer id %d; err: %s", sig.ID, err)
				continue
			}
			for i := range len(hdr.AvailableCommonBlocks) {
				signatureSets[i+1] = append(signatureSets[i+1], signatures[i])
			}
		}

		computedMsg := &protoutil.MessageToSign{
			IdentifierHeader:     proposalMsg.IdentifierHeader,
			BlockHeader:          blockHeaderBytes,
			OrdererBlockMetadata: proposalOrdererBlockMetadata,
		}
		signatureSets[0] = append(signatureSets[0], &protoutil.SignedData{
			Identity:  signerIdentity,
			Data:      computedMsg.ASN1MarshalOrPanic(),
			Signature: values[0],
		})
		v.Logger.Infof("Appended signature from ID %d to signature set for block ID %d", sig.ID, block.GetHeader().GetNumber())
	}

	if err := v.Policy.EvaluateSignedData(signatureSets[0]); err != nil {
		return errors.Wrapf(err, "failed to evaluate signature set of the first (proposal) signature for block ID %d", block.GetHeader().GetNumber())
	}

	if verifyData {
		for i := range len(hdr.AvailableCommonBlocks) {
			if err := v.Policy.EvaluateSignedData(signatureSets[i+1]); err != nil {
				return errors.Wrapf(err, "failed to evaluate signature set in index %d for block ID %d", i, block.GetHeader().GetNumber())
			}
		}
	}

	return nil
}

func (v *BlockSigVerifier) searchConsenterIdentityByID(identifier uint32) *msppb.Identity {
	for _, consenter := range v.Consenters {
		if consenter.Id == identifier {
			return msppb.NewIdentity(consenter.MspId, consenter.Identity)
		}
	}
	return nil
}

func (v *BlockSigVerifier) collectBlocksSignatures(msgs [][]byte, values [][]byte, blockHeaderBytes [][]byte, id uint64) ([]*protoutil.SignedData, error) {
	if len(msgs) != len(values) || len(msgs) != len(blockHeaderBytes) {
		return nil, errors.Errorf("the length of the msgs, values and blockHeaderBytes slices are not equal for signature id %d", id)
	}
	signatureSet := make([]*protoutil.SignedData, 0, len(msgs))
	for i, msg := range msgs {
		signedMsg := &protoutil.MessageToSign{}
		if err := signedMsg.ASN1Unmarshal(msg); err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal the signature msg in index %d with id %d", i, id)
		}
		// verify message identifier header
		idHeader, err := protoutil.UnmarshalIdentifierHeader(signedMsg.IdentifierHeader)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal identifier header from the signature msg in index %d with id %d", i, id)
		}
		if uint64(idHeader.GetIdentifier()) != id {
			return nil, errors.Errorf("signature ID %d does not match identifier header ID %d in signature msg in index %d", id, idHeader.GetIdentifier(), i)
		}
		signerIdentity := v.searchConsenterIdentityByID(idHeader.GetIdentifier())
		if signerIdentity == nil {
			return nil, errors.Errorf("signature with id %d has unknown consenter identity", id)
		}
		computedMsg := &protoutil.MessageToSign{
			IdentifierHeader:     signedMsg.IdentifierHeader,
			BlockHeader:          blockHeaderBytes[i],
			OrdererBlockMetadata: signedMsg.OrdererBlockMetadata, // TODO: calculate this
		}
		signatureSet = append(signatureSet, &protoutil.SignedData{
			Identity:  signerIdentity,
			Data:      computedMsg.ASN1MarshalOrPanic(),
			Signature: values[i],
		})
	}
	return signatureSet, nil
}
