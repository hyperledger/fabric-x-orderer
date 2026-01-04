/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"math"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
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
	F := (uint16(newState.N) - 1) / 3
	newState.Threshold = F + 1
	newState.Quorum = uint16(math.Ceil((float64(newState.N) + float64(F) + 1) / 2.0))

	return newState, nil
}
