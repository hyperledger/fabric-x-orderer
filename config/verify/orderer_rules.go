/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

//go:generate counterfeiter -o mocks/orderer_rules.go . OrdererRules
type OrdererRules interface {
	ValidateNewConfig(envelope *common.Envelope) error
	// TODO: add ValidateTransition method to verify config transitions from current to next.
}

type DefaultOrdererRules struct{}

// ValidateNewConfig validates that the rules of the new config are valid before it is applied.
func (or *DefaultOrdererRules) ValidateNewConfig(envelope *common.Envelope) error {
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
	// TODO: Validate endpoints in the Orderer Org definitions.
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
