/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configrequest

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type ConfigRequestValidator interface {
	ValidateConfigRequest(envelope *common.Envelope) error
}

type DefaultValidateConfigRequest struct {
	ConfigUpdateProposer policy.ConfigUpdateProposer
	Bundle               channelconfig.Resources
}

//go:generate counterfeiter -o mocks/config_request_validator.go . ConfigRequestValidator
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
