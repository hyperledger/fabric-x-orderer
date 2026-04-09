/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/pkg/errors"
)

// ConsenterSupportAdapter adapts a Consenter to satisfy the
// synchronizer.ConsenterSupport interface, which combines identity.SignerSerializer
// (Sign + Serialize) with the channel-support methods required by the BFT synchronizer.
//
// TODO all access to the consensus Config Bundle should be thread safe.
// Currently the Bundle is only read from the adapter, and only updated in the main consensus loop.
// However, once we write the config block to the ledger in WriteConfigBlock, we also need to update the Bundle with the new config.
type ConsenterSupportAdapter struct {
	consensus *Consensus
}

// Sign signs the given message bytes and returns the signature.
// Implements identity.SignerSerializer (identity.Signer).
func (c *ConsenterSupportAdapter) Sign(message []byte) ([]byte, error) {
	return c.consensus.Signer.Sign(message)
}

// Serialize converts the local identity to bytes so it can be embedded in
// a signature header.
// Implements identity.SignerSerializer (identity.Serializer).
func (c *ConsenterSupportAdapter) Serialize() ([]byte, error) {
	return c.consensus.Signer.Serialize()
}

// SignatureVerifier returns a function that verifies a block's signature
// against the current channel configuration.
// Implements synchronizer.ConsenterSupport.
func (c *ConsenterSupportAdapter) SignatureVerifier() protoutil.BlockVerifierFunc {
	// TODO: implement actual signature verification for blocks, currently we return a dummy function.
	return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
		return nil
	}
}

// Block returns the block with the given number from the ledger,
// or nil if no such block exists.
// Implements synchronizer.ConsenterSupport.
func (c *ConsenterSupportAdapter) Block(number uint64) *common.Block {
	block, err := c.consensus.Storage.RetrieveBlockByNumber(number)
	if err != nil {
		c.consensus.Logger.Errorf("Failed to retrieve block %d: %v", number, err)
		return nil
	}
	return block
}

// LastConfigBlock returns the most recent (fabric) config block at or before the given decision block,
// or an error if it cannot be retrieved.
// Implements synchronizer.ConsenterSupport.
func (c *ConsenterSupportAdapter) LastConfigBlock(block *common.Block) (*common.Block, error) {
	if block == nil {
		return nil, errors.New("input block is nil")
	}
	return config.GetLastConfigBlockUsingBlockFromConsensusLedger(block, c.consensus.Storage, c.consensus.Logger)
}

// Height returns the current ledger height (number of committed blocks).
// Implements synchronizer.ConsenterSupport.
func (c *ConsenterSupportAdapter) Height() uint64 {
	return c.consensus.Storage.Height()
}

// ChannelID returns the identifier of the channel this adapter is associated
// with.
// Implements synchronizer.ConsenterSupport.
func (c *ConsenterSupportAdapter) ChannelID() string {
	channelID := c.consensus.Config.Bundle.ConfigtxValidator().ChannelID()
	// TODO remove the replacement once we harmonize the channel ID in the config with the one used in the consensus code (and everywhere else).
	c.consensus.Logger.Debugf("Retrieving channel ID from config bundle: '%s',replacing with 'consensus'", channelID)
	return "consensus"
}

// Sequence returns the current configuration sequence number for the channel.
// Implements synchronizer.ConsenterSupport.
func (c *ConsenterSupportAdapter) Sequence() uint64 {
	return c.consensus.Config.Bundle.ConfigtxValidator().Sequence()
}

// SharedConfig returns the orderer section of the channel's current config
// bundle.
// Implements synchronizer.ConsenterSupport.
func (c *ConsenterSupportAdapter) SharedConfig() channelconfig.Orderer {
	ordererConfig, ok := c.consensus.Config.Bundle.OrdererConfig()
	if !ok {
		c.consensus.Logger.Panic("Orderer config not found in config bundle")
	}
	return ordererConfig
}

// WriteBlockSync commits a regular (non-config) block to the ledger together
// with its encoded metadata value, blocking until the write is complete.
// Implements synchronizer.ConsenterSupport.
func (c *ConsenterSupportAdapter) WriteBlockSync(block *common.Block, encodedMetadataValue []byte) {
	// Index the batches before writing the block to the ledger, like we do in Consensus.Deliver.
	hdr := c.indexAndWrite(block)

	c.consensus.Logger.Infof("BFT Synchronizer wrote consenter block number: %d", hdr.Num)
	// The state is updated with the call to OnCommit from the BFT synchronizer.
}

// WriteConfigBlock commits a config block to the ledger and applies the
// configuration update contained within it.
// Implements synchronizer.ConsenterSupport.
func (c *ConsenterSupportAdapter) WriteConfigBlock(block *common.Block, encodedMetadataValue []byte) {
	// Index the batches before writing the block to the ledger, like we do in Consensus.Deliver.
	hdr := c.indexAndWrite(block)

	if len(hdr.AvailableCommonBlocks) == 0 {
		c.consensus.Logger.Panicf("Block %d does not contain a config block", hdr.Num)
	}
	configBlock := hdr.AvailableCommonBlocks[len(hdr.AvailableCommonBlocks)-1]

	c.consensus.Logger.Infof("BFT Synchronizer wrote consenter block number: %d, which includes a config block number: %d", hdr.Num, configBlock.GetHeader().Number)
	// The state is updated and the config update is applied with the call to OnCommit from the BFT synchronizer.
}

func (c *ConsenterSupportAdapter) indexAndWrite(block *common.Block) *state.Header {
	proposal, err := state.ConsenterBlockToProposal(block)
	if err != nil {
		c.consensus.Logger.Panicf("Failed to create proposal from block: %v", err)
	}
	hdr, digests := c.consensus.headerAndDigestsFromProposal(*proposal)
	c.consensus.Arma.Index(digests)

	c.consensus.Storage.WriteBlock(block)

	return hdr
}
