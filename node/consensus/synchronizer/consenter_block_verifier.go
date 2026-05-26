/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"bytes"
	"encoding/hex"
	"sync"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/configtx"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/verifier_factory.go --fake-name VerifierFactory . VerifierFactory

type VerifierFactory interface {
	CreateBlockVerifier(
		configBlock *common.Block,
		lastBlock *common.Block,
		cryptoProvider bccsp.BCCSP,
		lg *flogging.FabricLogger,
	) (deliverclient.CloneableUpdatableBlockVerifier, error)
}

// ConsenterBlockVerifierCreator creates a block verifier that verifies consenter blocks and can by used by the synchronizer
type ConsenterBlockVerifierCreator struct{}

// CreateBlockVerifier creates a block verifier that verifies consenter blocks and can be used by the synchronizer
// configBlock is the config block that is used to create the verifier
// lastBlock is the last **consenter** block that was processed by the synchronizer
func (*ConsenterBlockVerifierCreator) CreateBlockVerifier(
	configBlock *common.Block,
	lastBlock *common.Block,
	cryptoProvider bccsp.BCCSP,
	lg *flogging.FabricLogger,
) (deliverclient.CloneableUpdatableBlockVerifier, error) {
	if configBlock == nil {
		return nil, errors.Errorf("config block is nil")
	}
	if configBlock.Header == nil {
		return nil, errors.Errorf("config block header is nil")
	}
	if !protoutil.IsConfigBlock(configBlock) {
		return nil, errors.New("config block parameter does not carry a config block")
	}

	if lastBlock == nil {
		return nil, errors.New("last block is nil")
	}
	if lastBlock.Header == nil {
		return nil, errors.New("last verified block header is nil")
	}

	// TODO verify config number / index against last block

	configTx, err := protoutil.ExtractEnvelope(configBlock, 0)
	if err != nil {
		return nil, errors.WithMessage(err, "error extracting envelope")
	}
	payload, err := protoutil.UnmarshalPayload(configTx.Payload)
	if err != nil {
		return nil, errors.WithMessage(err, "error unmarshaling envelope to payload")
	}

	if payload.Header == nil {
		return nil, errors.New("payload header is nil")
	}

	if payload.Header.ChannelHeader == nil {
		return nil, errors.New("payload header channel header is nil")
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, errors.WithMessage(err, "error unmarshaling channel header")
	}

	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return nil, errors.WithMessage(err, "error unmarshaling config envelope from payload data")
	}

	svc := &SigVerifierCreator{
		Logger: lg,
		BCCSP:  cryptoProvider,
	}
	sigVerifierFunc, err := svc.SigVerifierFromConfig(configEnvelope, chdr.GetChannelId(), configBlock.Header.Number)
	if err != nil {
		return nil, errors.WithMessage(err, "error creating sig verifier function")
	}

	cbv := &ConsenterBlockVerifier{
		channelID:          chdr.GetChannelId(),
		sigVerifierCreator: svc,
		sigVerifierFunc:    sigVerifierFunc,
		lastBlockHeader:    lastBlock.Header,
		logger:             lg,
	}

	return cbv, nil
}

type ConsenterBlockVerifier struct {
	channelID          string
	lastBlockHeader    *common.BlockHeader
	logger             *flogging.FabricLogger
	sigVerifierCreator *SigVerifierCreator
	sigVerifierFunc    SigVerifierFunc
	lock               sync.RWMutex
}

// Clone returns a copy of the verifier
func (c *ConsenterBlockVerifier) Clone() deliverclient.CloneableUpdatableBlockVerifier {
	c.lock.RLock()
	defer c.lock.RUnlock()
	cbv := &ConsenterBlockVerifier{
		channelID:          c.channelID,
		sigVerifierCreator: c.sigVerifierCreator,
		sigVerifierFunc:    c.sigVerifierFunc,
		lastBlockHeader:    c.lastBlockHeader,
		logger:             c.logger,
	}
	return cbv
}

// UpdateBlockHeader saves the last block header that was verified and handled successfully.
func (c *ConsenterBlockVerifier) UpdateBlockHeader(block *common.Block) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.lastBlockHeader = block.Header
}

// UpdateConfig sets the config by which blocks are verified. It is assumed that this config block had already been
// verified using the VerifyBlock method immediately prior to calling this method.
// The configBlock parameter is a consenter decision block that contains an embedded Fabric config block.
func (c *ConsenterBlockVerifier) UpdateConfig(configBlock *common.Block) error {
	blockOps := &state.ConsenterConfigBlockOperations{}
	configEnvelope, err := blockOps.ConfigFromBlock(configBlock)
	if err != nil {
		return errors.Wrap(err, "failed to extract config from block")
	}

	configBlockNum, err := blockOps.ConfigBlockNumFromBlock(configBlock)
	if err != nil {
		return errors.Wrap(err, "failed to extract config block number from block")
	}

	channelID := c.channelID
	if channelID == "" {
		return errors.New("channel ID is empty")
	}

	sigVerifierFunc, err := c.sigVerifierCreator.SigVerifierFromConfig(configEnvelope, channelID, configBlockNum)
	if err != nil {
		return errors.WithMessage(err, "error creating verifier function")
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.lastBlockHeader = configBlock.Header
	c.sigVerifierFunc = sigVerifierFunc

	c.logger.Infof("Updated config successfully from consenter block %d", configBlock.Header.Number)
	return nil
}

// VerifyBlock checks block integrity and its relation to the chain, and verifies the signatures.
func (c *ConsenterBlockVerifier) VerifyBlock(block *common.Block) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if err := c.verifyHeader(block); err != nil {
		return err
	}

	if err := c.verifyMetadata(block); err != nil {
		return err
	}

	if block.Data == nil {
		return errors.Errorf("block with id [%d] on channel [%s] does not have data", block.Header.Number, c.channelID)
	}
	if len(block.Data.Data) == 0 {
		return errors.Errorf("block with id [%d] on channel [%s] data length is zero", block.Header.Number, c.channelID)
	}
	// TODO something like protoutil.VerifyTransactionsAreWellFormed that is called by the Fabric block verifier

	// Verify that Header.DataHash is equal to the hash of block.Data
	// This is to ensure that the header is consistent with the data carried by this block
	dataHash := protoutil.ComputeBlockDataHash(block.Data)
	if !bytes.Equal(dataHash, block.Header.DataHash) {
		return errors.Errorf("Header.DataHash is different from Hash(block.Data) for block with id [%d] on channel [%s]; Header: %s, Data: %s",
			block.Header.Number, c.channelID, hex.EncodeToString(block.Header.DataHash), hex.EncodeToString(dataHash))
	}

	err := c.sigVerifierFunc(block, true)
	if err != nil {
		return errors.Wrapf(err, "failed to verify signatures of block with id [%d] on channel [%s]", block.Header.Number, c.channelID)
	}

	c.lastBlockHeader = block.Header

	c.logger.Infof("verified block with id [%d] on channel [%s]", block.Header.Number, c.channelID)
	return nil
}

// VerifyBlockAttestation does the same as VerifyBlock, except it assumes block.Data = nil. It therefore does not
// compute the block.Data.Hash() and compare it to the block.Header.DataHash. This is used when the orderer
// delivers a block with header & metadata only, as an attestation of block existence.
func (c *ConsenterBlockVerifier) VerifyBlockAttestation(block *common.Block) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if err := c.verifyHeader(block); err != nil {
		return err
	}

	if err := c.verifyMetadata(block); err != nil {
		return err
	}

	err := c.sigVerifierFunc(block, false)
	if err != nil {
		return errors.Wrapf(err, "failed to verify signatures of block attestation with id [%d] on channel [%s]", block.Header.Number, c.channelID)
	}

	c.lastBlockHeader = block.Header
	return nil
}

func (c *ConsenterBlockVerifier) verifyMetadata(block *common.Block) error {
	if block.Metadata == nil || len(block.Metadata.Metadata) < len(common.BlockMetadataIndex_name) {
		return errors.Errorf("block with id [%d] on channel [%s] does not have metadata or contains too few entries", block.Header.Number, c.channelID)
	}

	return nil
}

func (c *ConsenterBlockVerifier) verifyHeader(block *common.Block) error {
	if block == nil {
		return errors.Errorf("block must be different from nil, channel=%s", c.channelID)
	}
	if block.Header == nil {
		return errors.Errorf("invalid block, header must be different from nil, channel=%s", c.channelID)
	}

	expectedBlockNum := c.lastBlockHeader.Number + 1
	if expectedBlockNum != block.Header.Number {
		return errors.Errorf("expected block number is [%d] but actual block number inside block is [%d]", expectedBlockNum, block.Header.Number)
	}

	lastBlockHeaderHash := protoutil.BlockHeaderHash(c.lastBlockHeader)

	if !bytes.Equal(block.Header.PreviousHash, lastBlockHeaderHash) {
		return errors.Errorf("Header.PreviousHash of block [%d] is different from Hash(block.Header) of previous block, on channel [%s], received: %s, expected: %s",
			block.Header.Number, c.channelID, hex.EncodeToString(block.Header.PreviousHash), hex.EncodeToString(lastBlockHeaderHash))
	}

	return nil
}
