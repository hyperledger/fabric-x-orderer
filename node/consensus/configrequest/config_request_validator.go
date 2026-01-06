/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configrequest

import (
	"fmt"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

//go:generate counterfeiter -o mocks/config_request_validator.go . ConfigRequestValidator
type ConfigRequestValidator interface {
	ValidateConfigRequest(envelope *common.Envelope) error
	ValidateNewConfig(envelope *common.Envelope) error
}

type DefaultValidateConfigRequest struct {
	ConfigUpdateProposer policy.ConfigUpdateProposer
	Bundle               channelconfig.Resources
}

func (vc *DefaultValidateConfigRequest) ValidateConfigRequest(envelope *common.Envelope) error {
	// extract from envelope the config envelope (from envelope.payload.Data)
	// 1. take configEnvelope.LastUpdate which is the CONFIG_UPDATE and produce the corresponding configEnvelope, which is the expected ConfigEnvelope produced by the update
	// 2. check that original configEnvelope.Config == expectedConfigEnvelope.Config
	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
	if err != nil {
		return err
	}

	configEnvelope := &common.ConfigEnvelope{}
	if err = proto.Unmarshal(payload.Data, configEnvelope); err != nil {
		return fmt.Errorf("payload data unmarshalling error: %s", err)
	}

	if configEnvelope == nil || configEnvelope.LastUpdate == nil || configEnvelope.Config == nil {
		return errors.New("invalid config envelope")
	}

	var expectedConfigEnvelope *common.ConfigEnvelope
	expectedConfigEnvelope, err = vc.ConfigUpdateProposer.AuthorizeAndVerifyConfigUpdate(configEnvelope.LastUpdate, vc.Bundle)
	if err != nil {
		return errors.Errorf("falied to propose a new config update")
	}

	if !proto.Equal(configEnvelope.Config, expectedConfigEnvelope.Config) {
		return errors.Errorf("pending last update does not match calculated expected config")
	}

	return nil
}

// ValidateNewConfig validates that a new configuration is valid before it is applied.
func (vc *DefaultValidateConfigRequest) ValidateNewConfig(envelope *common.Envelope) error {
	bundle, err := channelconfig.NewBundleFromEnvelope(envelope, factory.GetDefault())
	if err != nil {
		return err
	}

	ordererConfig, exists := bundle.OrdererConfig()
	if !exists {
		return errors.Errorf("orderer entry in the config block is empty")
	}

	sharedConfig := &config_protos.SharedConfig{}
	if err := proto.Unmarshal(ordererConfig.ConsensusMetadata(), sharedConfig); err != nil {
		return errors.Wrap(err, "failed to unmarshal consensus metadata")
	}

	// 1. Validate batch timeouts
	if err := validateBatchTimeout(sharedConfig.BatchingConfig.BatchTimeouts); err != nil {
		return err
	}

	// 2. Validate that BatchSize AbsoluteMaxBytes is consistent between shared and orderer config
	if sharedConfig.BatchingConfig.BatchSize.AbsoluteMaxBytes != ordererConfig.BatchSize().AbsoluteMaxBytes {
		return errors.Errorf("batch size differs between shared and orderer config")
	}

	// TODO: Validate BFT request max bytes.
	// TODO: Validate orderer endpoints.
	// TODO: Validate BFT parameters.
	// TODO: Validate certificates.
	// TODO: Validate consenter mapping.
	// TODO: Validate block validation policy.

	return nil
}

func validateBatchTimeout(bt *config_protos.BatchTimeouts) error {
	batchCreation, err := time.ParseDuration(bt.BatchCreationTimeout)
	if err != nil {
		return err
	}
	if batchCreation <= 0 {
		return errors.Errorf("attempted to set the batch creation timeout to a non-positive value: %s", batchCreation)
	}

	firstStrike, err := time.ParseDuration(bt.FirstStrikeThreshold)
	if err != nil {
		return err
	}
	if firstStrike <= 0 {
		return errors.Errorf("attempted to set the first strike threshold to a non-positive value: %s", firstStrike)
	}

	secondStrike, err := time.ParseDuration(bt.SecondStrikeThreshold)
	if err != nil {
		return err
	}
	if secondStrike <= 0 {
		return errors.Errorf("attempted to set the second strike threshold to a non-positive value: %s", secondStrike)
	}

	autoRemove, err := time.ParseDuration(bt.AutoRemoveTimeout)
	if err != nil {
		return err
	}
	if autoRemove <= 0 {
		return errors.Errorf("attempted to set auto remove timeout to a non-positive value: %s", autoRemove)
	}

	return nil
}
