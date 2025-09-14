/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package policy

import (
	"github.com/hyperledger/fabric-lib-go/bccsp"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/common/configtx"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/pkg/errors"
)

// BuildBundleFromBlock builds a bundle from block.
// This bundle supplies all resources needed for verification, e.g. policy manager, config tx validator etc.
func BuildBundleFromBlock(configTX *cb.Envelope, bccsp bccsp.BCCSP) (*channelconfig.Bundle, error) {
	payload, err := protoutil.UnmarshalPayload(configTX.Payload)
	if err != nil {
		return nil, errors.WithMessage(err, "error unmarshalling envelope to payload")
	}

	if payload.Header == nil {
		return nil, errors.New("envelope payload header is nil")
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, errors.WithMessage(err, "error unmarshalling channel header")
	}

	if chdr == nil {
		return nil, errors.New("envelope payload header channel header is nil")
	}

	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return nil, errors.WithMessage(err, "error unmarshalling config envelope from payload data")
	}

	if configEnvelope == nil {
		return nil, errors.New("config envelope is nil")
	}

	bundle, err := channelconfig.NewBundle(chdr.ChannelId, configEnvelope.Config, bccsp)
	if err != nil {
		return nil, errors.WithMessage(err, "error creating channelconfig bundle")
	}

	err = checkResources(bundle)
	if err != nil {
		return nil, errors.WithMessagef(err, "error checking bundle for channel: %s", chdr.ChannelId)
	}
	return bundle, nil
}

// checkResources makes sure that the channel config is compatible with this binary and logs sanity checks
func checkResources(res channelconfig.Resources) error {
	channelconfig.LogSanityChecks(res)
	oc, ok := res.OrdererConfig()
	if !ok {
		return errors.New("config does not contain orderer config")
	}
	if err := oc.Capabilities().Supported(); err != nil {
		return errors.WithMessagef(err, "config requires unsupported orderer capabilities: %s", err)
	}
	if err := res.ChannelConfig().Capabilities().Supported(); err != nil {
		return errors.WithMessagef(err, "config requires unsupported channel capabilities: %s", err)
	}
	return nil
}
