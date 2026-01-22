/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

//go:generate counterfeiter -o mocks/config_applier.go . ConfigApplier
type ConfigApplier interface {
	// ApplyConfigToState update the current state based on the config request
	ApplyConfigToState(state *state.State, configRequest *state.ConfigRequest) (*state.State, error)
	// ExtractConfigFromConfigBlock extracts the config from a config block
	ExtractConfigFromConfigBlock(configBlock *common.Block, selfID arma_types.PartyID) ([]uint64, smartbft_types.Configuration, error)
}

type DefaultConfigApplier struct{}

func (ca *DefaultConfigApplier) ApplyConfigToState(state *state.State, configRequest *state.ConfigRequest) (*state.State, error) {
	bundle, err := channelconfig.NewBundleFromEnvelope(configRequest.Envelope, factory.GetDefault())
	if err != nil {
		return nil, err
	}
	ordererConfig, exists := bundle.OrdererConfig()
	if !exists {
		return nil, errors.New("orderer entry in the config block is empty")
	}

	consensusMetadata := ordererConfig.ConsensusMetadata()
	sharedConfig := &config_protos.SharedConfig{}
	if err := proto.Unmarshal(consensusMetadata, sharedConfig); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal consensus metadata to a shared configuration")
	}

	newState := state.Clone()
	newState.N = uint16(len(sharedConfig.PartiesConfig))
	_, T, Q := utils.ComputeFTQ(newState.N)
	newState.Threshold = T
	newState.Quorum = Q

	if newState.N != state.N {
		// there was a party change, explicitly change term on all shards
		for i := range newState.Shards {
			newState.Shards[i].Term++
		}
	}

	return newState, nil
}

func (ca *DefaultConfigApplier) ExtractConfigFromConfigBlock(configBlock *common.Block, selfID arma_types.PartyID) ([]uint64, smartbft_types.Configuration, error) {
	if configBlock == nil {
		return nil, smartbft_types.Configuration{}, errors.New("block is nil")
	}
	if !protoutil.IsConfigBlock(configBlock) {
		return nil, smartbft_types.Configuration{}, errors.New("not a config block")
	}
	configReqBytes := configBlock.GetData().GetData()[0]
	configReqEnv, err := protoutil.UnmarshalEnvelope(configReqBytes)
	if err != nil {
		return nil, smartbft_types.Configuration{}, errors.Wrap(err, "unable to unmarshal config req")
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(configReqEnv, factory.GetDefault())
	if err != nil {
		return nil, smartbft_types.Configuration{}, errors.Wrap(err, "unable to create bundle from the config req env")
	}
	ordererConfig, exists := bundle.OrdererConfig()
	if !exists {
		return nil, smartbft_types.Configuration{}, errors.New("orderer entry in the config block is empty")
	}
	consensusMetadata := ordererConfig.ConsensusMetadata()
	sharedConfig := &config_protos.SharedConfig{}
	if err := proto.Unmarshal(consensusMetadata, sharedConfig); err != nil {
		return nil, smartbft_types.Configuration{}, errors.Wrapf(err, "failed to unmarshal consensus metadata to a shared configuration")
	}
	conf := &config.Configuration{SharedConfig: sharedConfig}
	smartBFTConfig, err := conf.GetBFTConfig(selfID)
	if err != nil {
		return nil, smartbft_types.Configuration{}, errors.Wrapf(err, "failed to extract bft config")
	}
	var partyIDs []uint64
	for _, party := range sharedConfig.PartiesConfig {
		partyIDs = append(partyIDs, uint64(party.GetPartyID()))
	}

	return partyIDs, smartBFTConfig, nil
}
