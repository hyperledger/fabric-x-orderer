/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/pkg/errors"
)

// ConsenterConfigBlockOperations implements ConfigBlockOperations for consenter decision blocks.
// These blocks contain consensus decisions that may include one or more Fabric common blocks.
type ConsenterConfigBlockOperations struct{}

// IsConfigBlock checks if a consenter decision block contains a config block.
// It does this by comparing the block number with the last config index in the metadata.
// If they match, this decision block contains a config block.
func (c *ConsenterConfigBlockOperations) IsConfigBlock(block *common.Block) bool {
	if block == nil || block.Header == nil {
		return false
	}

	// Genesis block is always a config block
	if block.Header.Number == 0 {
		return true
	}

	lastConfigIndex, err := GetLastConfigIndexFromConsenterBlock(block)
	if err != nil {
		return false
	}

	return lastConfigIndex == block.Header.Number
}

// extractFabricConfigBlock is a helper function that extracts the embedded Fabric config block
// from a consenter decision block. The last AvailableCommonBlock in a config decision is the Fabric config block.
func (c *ConsenterConfigBlockOperations) extractFabricConfigBlock(block *common.Block) (*common.Block, error) {
	if block == nil {
		return nil, errors.New("block is nil")
	}

	if block.Header == nil {
		return nil, errors.New("block header is nil")
	}

	if block.Data == nil || len(block.Data.Data) == 0 {
		return nil, errors.New("block data is empty")
	}

	// Extract the proposal from the consenter block
	proposal, err := ConsenterBlockToProposal(block)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract proposal from consenter block")
	}

	// Deserialize the header to access AvailableCommonBlocks
	header := &Header{}
	if err := header.Deserialize(proposal.Header); err != nil {
		return nil, errors.Wrap(err, "failed to deserialize header")
	}

	// The last available common block in a config decision is the Fabric config block
	if len(header.AvailableCommonBlocks) == 0 {
		return nil, errors.New("no available common blocks in consenter block")
	}

	fabricConfigBlock := header.AvailableCommonBlocks[len(header.AvailableCommonBlocks)-1]

	return fabricConfigBlock, nil
}

// ConfigFromBlock extracts the Fabric config envelope from a consenter decision block.
// For consenter blocks, the config is embedded within the decision's AvailableCommonBlocks.
// The last AvailableCommonBlock in a config decision is the Fabric config block.
func (c *ConsenterConfigBlockOperations) ConfigFromBlock(block *common.Block) (*common.ConfigEnvelope, error) {
	fabricConfigBlock, err := c.extractFabricConfigBlock(block)
	if err != nil {
		return nil, err
	}

	commonOps := &utils.CommonConfigBlockOperations{}
	if !commonOps.IsConfigBlock(fabricConfigBlock) {
		return nil, errors.New("Consenter decision block does not contain a config block")
	}
	// Now extract config from the Fabric block using common block operations
	return commonOps.ConfigFromBlock(fabricConfigBlock)
}

// ConfigBlockNumFromBlock extracts the Fabric config block number from a consenter decision block.
// For consenter blocks, the config is embedded within the decision's AvailableCommonBlocks.
// The last AvailableCommonBlock in a config decision is the Fabric config block.
func (c *ConsenterConfigBlockOperations) ConfigBlockNumFromBlock(block *common.Block) (uint64, error) {
	fabricConfigBlock, err := c.extractFabricConfigBlock(block)
	if err != nil {
		return 0, err
	}

	commonOps := &utils.CommonConfigBlockOperations{}
	if !commonOps.IsConfigBlock(fabricConfigBlock) {
		return 0, errors.New("Consenter decision block does not contain a config block")
	}

	if fabricConfigBlock.Header == nil {
		return 0, errors.New("fabric config block header is nil")
	}

	return fabricConfigBlock.Header.Number, nil
}
