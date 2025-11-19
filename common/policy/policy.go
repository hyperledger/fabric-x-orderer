/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package policy

import (
	"fmt"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/common/configtx"
	"github.com/hyperledger/fabric-x-common/internaltools/pkg/identity"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/config_update_proposer.go . ConfigUpdateProposer

// ConfigUpdateProposer defines how to handle config update authorization and verification.
type ConfigUpdateProposer interface {
	ProposeConfigUpdate(request *protos.Request, bundle channelconfig.Resources, signer identity.SignerSerializer, verifier *requestfilter.RulesVerifier) (*protos.Request, error)
}

type DefaultConfigUpdateProposer struct{}

// ProposeConfigUpdate validates a new config request against the current config state and validates that all modified config has the corresponding modification policies satisfied by the signature set.
// It translates the config update request (Envelope of type CONFIG_UPDATE) and produces a ConfigEnvelope to be used as the Envelope Payload Data of a CONFIG message.
// It creates a signed envelope that wraps the config envelope and re-validate it. This Re-validation is mainly intended to apply the size filtering and it is a good sanity check.
// When a config update is sent to the Router, ProposeConfigUpdate is called and the Router drops the signed envelope and forward the original request to the consensus.
// The consensus nodes apply the same validation checks and the leader proposes the config transaction signed by himself.
func (cp *DefaultConfigUpdateProposer) ProposeConfigUpdate(request *protos.Request, bundle channelconfig.Resources, signer identity.SignerSerializer, verifier *requestfilter.RulesVerifier) (*protos.Request, error) {
	configRequest, err := AuthorizeAndVerifyConfigUpdateRequest(request, bundle, signer, verifier)
	if err != nil {
		return configRequest, fmt.Errorf("failed authorizing and verifying config update reqest, err: %s", err)
	}
	return configRequest, nil
}

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

func AuthorizeAndVerifyConfigUpdateRequest(request *protos.Request, bundle channelconfig.Resources, signer identity.SignerSerializer, verifier *requestfilter.RulesVerifier) (*protos.Request, error) {
	// validates that all modified config has the corresponding modification policies satisfied by the signature set
	configEnvelope, err := bundle.ConfigtxValidator().ProposeConfigUpdate(&cb.Envelope{
		Payload:   request.Payload,
		Signature: request.Signature,
	})
	if err != nil {
		return nil, fmt.Errorf("error applying config update to %s", bundle.ConfigtxValidator().ChannelID())
	}

	// Apply validation checks of new bundle against old bundle
	err = validateNewBundleAgainstOldBundle(configEnvelope, bundle)
	if err != nil {
		return nil, fmt.Errorf("error validating new bundle against old bundle to %s", bundle.ConfigtxValidator().ChannelID())
	}

	// TODO: validate old orderer config against new orderer config
	// TODO: validate ConsensusMetadata (shared config)
	// TODO: validate old channel/application against old channel/application

	// Wrap the config envelope and sign, prepare a matching request
	config, err := protoutil.CreateSignedEnvelope(cb.HeaderType_CONFIG, bundle.ConfigtxValidator().ChannelID(), signer, configEnvelope, int32(0), 0)
	if err != nil {
		return nil, fmt.Errorf("error creating a signed envelope, err: %s", err)
	}

	configRequest := &protos.Request{
		Payload:    config.Payload,
		Signature:  config.Signature,
		Identity:   request.Identity,
		IdentityId: request.IdentityId,
		TraceId:    request.TraceId,
	}

	// Recall verifiers
	// We re-call The verifiers, especially for the size filter, to ensure that the transaction we
	// just constructed is not too large for our consenter. It additionally reapplies the signature
	// check, which although not strictly necessary, is a good sanity check, in case the router
	// has not been configured with the right cert material. The additional overhead of the signature
	// check is negligible, as this is the re-config path and not the normal path.
	reqType, err := verifier.VerifyStructureAndClassify(configRequest)
	if reqType != cb.HeaderType_CONFIG {
		return nil, fmt.Errorf("config request header should be %s but got %s", cb.HeaderType_CONFIG, reqType)
	}
	if err != nil {
		return nil, fmt.Errorf("config update did not pass signature filter final checks, err: %s", err)
	}
	err = verifier.Verify(configRequest)
	if err != nil {
		return nil, fmt.Errorf("config update did not pass filter final checks, err: %s", err)
	}

	return configRequest, nil
}

// TODO: revisit capabilities
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

func validateNewBundleAgainstOldBundle(configEnvelope *cb.ConfigEnvelope, bundle channelconfig.Resources) error {
	cryptoProvider := factory.GetDefault()
	channelID := bundle.ConfigtxValidator().ChannelID()
	newBundle, err := channelconfig.NewBundle(channelID, configEnvelope.Config, cryptoProvider)
	if err != nil {
		return fmt.Errorf("error creating bundle %s", channelID)
	}

	if err = checkResources(newBundle); err != nil {
		return errors.WithMessage(err, "config update is not compatible")
	}

	if err = bundle.ValidateNew(newBundle); err != nil {
		return err
	}

	return nil
}
