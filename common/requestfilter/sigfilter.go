/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/configtx"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type SigFilter struct {
	clientSignatureVerificationRequired bool
	channelID                           string
	policyManager                       policies.Manager
	configTxValidator                   configtx.Validator
	signer                              protoutil.Signer
}

func NewSigFilter(config FilterConfig) *SigFilter {
	return &SigFilter{
		clientSignatureVerificationRequired: config.ClientSignatureVerificationRequired(),
		channelID:                           config.ChannelID(),
		policyManager:                       config.PolicyManager(),
		configTxValidator:                   config.ConfigTxValidator(),
		signer:                              config.Signer(),
	}
}

func (sf *SigFilter) Verify(request *comm.Request) error {
	signedData, isConfig, err := sf.requestToSignedData(request)
	if err != nil {
		return fmt.Errorf("failed to convert request to signedData : %s", err)
	}

	if sf.clientSignatureVerificationRequired {
		policy, exists := sf.policyManager.GetPolicy(policies.ChannelWriters)
		if !exists {
			return fmt.Errorf("no policies in config block")
		}
		err = policy.EvaluateSignedData([]*protoutil.SignedData{signedData})
		if err != nil {
			return fmt.Errorf("signature did not satisfy policy %s", policies.ChannelWriters)
		}

		if isConfig {
			// validate config envelope
			configEnvelope, err := sf.configTxValidator.ProposeConfigUpdate(&common.Envelope{
				Payload:   request.Payload,
				Signature: request.Signature,
			})
			if err != nil {
				return errors.WithMessagef(err, "error applying config update to existing channel '%s'", sf.configTxValidator.ChannelID())
			}

			config, err := protoutil.CreateSignedEnvelope(common.HeaderType_CONFIG, sf.configTxValidator.ChannelID(), sf.signer, configEnvelope, int32(0), 0)
			if err != nil {
				return err
			}

			envelopeAsSignedData, err := protoutil.EnvelopeAsSignedData(config)
			if err != nil {
				return fmt.Errorf("could not convert message to signedData: %s", err)
			}

			// We re-call EvaluateSignedData, especially for the size filter, to ensure that the transaction we
			// just constructed is not too large for our consenter. It additionally reapplies the signature
			// check, which although not strictly necessary, is a good sanity check, in case the router
			// has not been configured with the right cert material. The additional overhead of the signature
			// check is negligible, as this is the re-config path and not the normal path.
			err = policy.EvaluateSignedData(envelopeAsSignedData)
			if err != nil {
				return fmt.Errorf("signature did not satisfy policy %s", policies.ChannelWriters)
			}
		}

		// TODO: to be removed when policy and signatures are fully integrated
		return fmt.Errorf("error: signature validation is not implemented")
	}
	return nil
}

// requestToSignedData verifies the request structure and returns the payload, identity and signature in a SignedData and if the request is a transaction request.
func (sf *SigFilter) requestToSignedData(request *comm.Request) (*protoutil.SignedData, bool, error) {
	if request == nil {
		return nil, false, fmt.Errorf("nil request")
	}

	payload := &common.Payload{}
	err := proto.Unmarshal(request.Payload, payload)
	if err != nil {
		return nil, false, err
	}

	if payload.Header == nil {
		return nil, false, fmt.Errorf("missing header in request's payload")
	}

	if payload.Header.SignatureHeader == nil {
		return nil, false, fmt.Errorf("missing signature header in payload's header")
	}

	shdr := &common.SignatureHeader{}
	err = proto.Unmarshal(payload.Header.SignatureHeader, shdr)
	if err != nil {
		return nil, false, fmt.Errorf("failed unmarshalling signature header, err %s", err)
	}

	if payload.Header.ChannelHeader == nil {
		return nil, false, fmt.Errorf("missing channel header in request's payload")
	}

	chdr := &common.ChannelHeader{}
	err = proto.Unmarshal(payload.Header.ChannelHeader, chdr)
	if err != nil {
		return nil, false, fmt.Errorf("failed unmarshalling channel header, err %s", err.Error())
	}
	// TODO: check channel ID
	// if sf.channelID != chdr.ChannelId {
	// 	return nil, fmt.Errorf("channelID is incorrect. expected: %s, actual: %s", sf.channelID, chdr.ChannelId)
	// }

	return &protoutil.SignedData{
		Data:      request.Payload,
		Identity:  shdr.Creator,
		Signature: request.Signature,
	}, chdr.Type == int32(common.HeaderType_CONFIG_UPDATE), nil
}

func (sf *SigFilter) Update(config FilterConfig) error {
	sf.clientSignatureVerificationRequired = config.ClientSignatureVerificationRequired()
	sf.channelID = config.ChannelID()
	sf.policyManager = config.PolicyManager()
	sf.configTxValidator = config.ConfigTxValidator()
	sf.signer = config.Signer()
	return nil
}
