/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

//go:generate counterfeiter -o mocks/config_applier.go . ConfigApplier
type ConfigApplier interface {
	// ApplyConfigToState update the current state based on the config request
	ApplyConfigToState(state *state.State, configRequest *state.ConfigRequest) (*state.State, error)
}

type DefaultConfigApplier struct{}

func (ca *DefaultConfigApplier) ApplyConfigToState(state *state.State, configRequest *state.ConfigRequest) (*state.State, error) {
	bundle, err := channelconfig.NewBundleFromEnvelope(configRequest.Envelope, factory.GetDefault())
	if err != nil {
		return nil, err
	}
	ordererConfig, exists := bundle.OrdererConfig()
	if !exists {
		return nil, errors.Wrapf(err, "orderer entry in the config block is empty")
	}

	consensusMetadata := ordererConfig.ConsensusMetadata()
	sharedConfig := &config_protos.SharedConfig{}
	err = proto.Unmarshal(consensusMetadata, sharedConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal consensus metadata to a shared configuration")
	}

	newState := state.Clone()
	newState.N = uint16(len(sharedConfig.PartiesConfig))
	_, T, Q := utils.ComputeFTQ(newState.N)
	newState.Threshold = T
	newState.Quorum = Q

	return newState, nil
}
