/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configrequest

import (
	"fmt"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type ConfigRequestValidator interface {
	ValidateConfigRequest(envelope *common.Envelope) error
}

type DefaultValidateConfigRequest struct {
	ConfigUpdateProposer policy.ConfigUpdateProposer
	Bundle               channelconfig.Resources
	BCCSP                bccsp.BCCSP
	// RequestVerifier checks the structure, TxID and signature of the config envelope and of its LastUpdate,
	// as the consenter does for a submitted config update.
	RequestVerifier *requestfilter.RulesVerifier
}

//go:generate counterfeiter -o mocks/config_request_validator.go . ConfigRequestValidator
func (vc *DefaultValidateConfigRequest) ValidateConfigRequest(envelope *common.Envelope) error {
	// extract from envelope the config envelope (from envelope.payload.Data)
	// 1. take configEnvelope.LastUpdate which is the CONFIG_UPDATE and produce the corresponding configEnvelope, which is the expected ConfigEnvelope produced by the update
	// 2. check that original configEnvelope.Config == expectedConfigEnvelope.Config
	if err := vc.verifyRequest(envelope, common.HeaderType_CONFIG); err != nil {
		return errors.Wrap(err, "config envelope")
	}
	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
	if err != nil {
		return err
	}

	configEnvelope := &common.ConfigEnvelope{}
	if err = proto.Unmarshal(payload.Data, configEnvelope); err != nil {
		return fmt.Errorf("payload data unmarshalling error: %s", err)
	}

	if configEnvelope.LastUpdate == nil || configEnvelope.Config == nil {
		return errors.New("invalid config envelope")
	}

	// The leader proposes the LastUpdate, so verify it as a submitted config update, signature included: otherwise a
	// Byzantine leader could put another client's creator, nonce and TxID on it, and have that client's update dropped
	// as a duplicate.
	if err = vc.verifyRequest(configEnvelope.LastUpdate, common.HeaderType_CONFIG_UPDATE); err != nil {
		return errors.Wrap(err, "last update")
	}

	var expectedConfigEnvelope *common.ConfigEnvelope
	expectedConfigEnvelope, err = vc.ConfigUpdateProposer.AuthorizeAndVerifyConfigUpdate(configEnvelope.LastUpdate, vc.Bundle, vc.BCCSP)
	if err != nil {
		return errors.Errorf("falied to propose a new config update")
	}

	if !proto.Equal(configEnvelope.Config, expectedConfigEnvelope.Config) {
		return errors.Errorf("pending last update does not match calculated expected config")
	}

	return nil
}

// verifyRequest verifies the envelope with the RequestVerifier, and checks that it is of the expected type.
func (vc *DefaultValidateConfigRequest) verifyRequest(envelope *common.Envelope, expectedType common.HeaderType) error {
	reqType, err := vc.RequestVerifier.VerifyStructureAndClassify(&comm.Request{Payload: envelope.Payload, Signature: envelope.Signature})
	if err != nil {
		return err
	}
	if reqType != expectedType {
		return errors.Errorf("unexpected type %s, expected %s", reqType, expectedType)
	}
	return nil
}
