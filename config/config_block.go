/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/pkg/errors"
)

// ReadSharedConfigFromBootstrapConfigBlock reads the shared configuration which is encoded in the consensus metadata in a block.
func ReadSharedConfigFromBootstrapConfigBlock(configBlock *common.Block) ([]byte, error) {
	consensusMetadata, err := ReadConsensusMetadataFromConfigBlock(configBlock)
	if err != nil {
		return nil, err
	}

	return consensusMetadata, nil
}

func ReadChannelIdFromConfigBlock(configBlock *common.Block) (string, error) {
	env, err := protoutil.GetEnvelopeFromBlock(configBlock.Data.Data[0])
	if err != nil {
		return "", err
	}
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return "", err
	}
	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return "", err
	}
	return chdr.ChannelId, nil
}

func ReadConsensusMetadataFromConfigBlock(configBlock *common.Block) ([]byte, error) {
	envelope, err := protoutil.ExtractEnvelope(configBlock, 0)
	if err != nil {
		return nil, err
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(envelope, factory.GetDefault())
	if err != nil {
		return nil, err
	}
	orderer, exists := bundle.OrdererConfig()
	if !exists {
		return nil, errors.Wrapf(err, "orderer entry in the config block is empty")
	}

	return orderer.ConsensusMetadata(), nil
}
