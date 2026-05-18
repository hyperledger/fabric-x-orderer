/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/configtx"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/pkg/errors"
)

// ConfigBlockOperations provides block-type-specific operations for config block handling.
// This interface allows to work with different block types
// without knowing the specific implementation details.
type ConfigBlockOperations interface {
	// IsConfigBlock checks if a block is a config block
	IsConfigBlock(block *common.Block) bool

	// ConfigFromBlock extracts config envelope from a config block
	ConfigFromBlock(block *common.Block) (*common.ConfigEnvelope, error)
}

// CommonBlockOperations implements ConfigBlockOperations for regular Fabric common blocks.
// These are the standard blocks used also by Fabric.
type CommonBlockOperations struct{}

// IsConfigBlock checks if a common block is a config block by examining its transaction type.
func (c *CommonBlockOperations) IsConfigBlock(block *common.Block) bool {
	if block == nil {
		return false
	}
	return protoutil.IsConfigBlock(block)
}

// ConfigFromBlock extracts the config envelope from a common config block.
// It returns an error if the block is not a config block or if extraction fails.
func (c *CommonBlockOperations) ConfigFromBlock(block *common.Block) (*common.ConfigEnvelope, error) {
	if block == nil || block.Header == nil || block.Data == nil || len(block.Data.Data) == 0 {
		return nil, errors.New("empty block")
	}

	txn := block.Data.Data[0]
	env, err := protoutil.GetEnvelopeFromBlock(txn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Genesis block (block 0) has config directly
	if block.Header.Number == 0 {
		configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
		if err != nil {
			return nil, errors.Wrap(err, "invalid config envelope")
		}
		return configEnvelope, nil
	}

	// For non-genesis blocks, check the header type
	if payload.Header == nil {
		return nil, errors.New("nil header in payload")
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if common.HeaderType(chdr.Type) != common.HeaderType_CONFIG {
		return nil, errors.New("not a config block")
	}

	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return nil, errors.Wrap(err, "invalid config envelope")
	}

	return configEnvelope, nil
}
