/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"google.golang.org/protobuf/proto"
)

type SigFilter struct {
	clientSignatureVerificationRequired bool
	channelID                           string
	policyName                          string
	policyManager                       policies.Manager
}

func NewSigFilter(config FilterConfig, policyName string) *SigFilter {
	return &SigFilter{
		clientSignatureVerificationRequired: config.GetClientSignatureVerificationRequired(),
		channelID:                           config.GetChannelID(),
		policyName:                          policyName,
		policyManager:                       config.GetPolicyManager(),
	}
}

func (sf *SigFilter) VerifyAndClassify(request *comm.Request) (common.HeaderType, error) {
	// extract signedData, while verifying the structure of the request
	signedData, reqType, err := sf.requestToSignedData(request)
	if err != nil {
		return reqType, fmt.Errorf("failed to convert request to signedData : %w", err)
	}

	if sf.clientSignatureVerificationRequired || reqType == common.HeaderType_CONFIG_UPDATE {
		policy, exists := sf.policyManager.GetPolicy(sf.policyName)
		if !exists {
			return reqType, fmt.Errorf("no policies in config block")
		}
		err = policy.EvaluateSignedData([]*protoutil.SignedData{signedData})
		if err != nil {
			return reqType, fmt.Errorf("signature did not satisfy policy %s", sf.policyName)
		}
	}
	return reqType, nil
}

// requestToSignedData verifies the request structure, including its TxID and channel ID, and returns the payload,
// identity and signature in a SignedData. Additionally, the request type is extracted and returned.
func (sf *SigFilter) requestToSignedData(request *comm.Request) (*protoutil.SignedData, common.HeaderType, error) {
	var reqType common.HeaderType
	if request == nil {
		return nil, reqType, fmt.Errorf("nil request")
	}

	payload := &common.Payload{}
	err := proto.Unmarshal(request.Payload, payload)
	if err != nil {
		return nil, reqType, err
	}

	if payload.Header == nil {
		return nil, reqType, fmt.Errorf("missing header in request's payload")
	}

	if payload.Header.SignatureHeader == nil {
		return nil, reqType, fmt.Errorf("missing signature header in payload's header")
	}

	shdr := &common.SignatureHeader{}
	err = proto.Unmarshal(payload.Header.SignatureHeader, shdr)
	if err != nil {
		return nil, reqType, fmt.Errorf("failed unmarshalling signature header, err %s", err)
	}

	if payload.Header.ChannelHeader == nil {
		return nil, reqType, fmt.Errorf("missing channel header in request's payload")
	}

	chdr := &common.ChannelHeader{}
	err = proto.Unmarshal(payload.Header.ChannelHeader, chdr)
	if err != nil {
		return nil, reqType, fmt.Errorf("failed unmarshalling channel header, err %s", err.Error())
	}

	if err = verifyTxID(chdr, shdr); err != nil {
		return nil, reqType, err
	}

	if err = sf.verifyChannelID(chdr); err != nil {
		return nil, reqType, err
	}

	id, err := protoutil.UnmarshalIdentity(shdr.Creator)
	if err != nil {
		return nil, reqType, err
	}

	return &protoutil.SignedData{
		Data:      request.Payload,
		Identity:  id,
		Signature: request.Signature,
	}, common.HeaderType(chdr.Type), nil
}

// verifyChannelID checks the channel header's ChannelId against the node's channel ID.
// A data transaction (MESSAGE) may leave the channel ID empty, since a deployment may bind its data transactions to
// the network through their namespaces instead; a non-empty one must match.
// Any other request, a config update in particular, must carry the node's channel ID.
// The errors never echo the request's (unbounded) channel ID into errors and logs.
func (sf *SigFilter) verifyChannelID(chdr *common.ChannelHeader) error {
	channelID := chdr.GetChannelId()
	if channelID == "" {
		if common.HeaderType(chdr.GetType()) == common.HeaderType_MESSAGE {
			return nil
		}
		return errMissingChannelID
	}
	if channelID != sf.channelID {
		return fmt.Errorf("%w, expected %q", errWrongChannelID, sf.channelID)
	}
	return nil
}

func (sf *SigFilter) Update(config FilterConfig) error {
	sf.clientSignatureVerificationRequired = config.GetClientSignatureVerificationRequired()
	sf.channelID = config.GetChannelID()
	sf.policyManager = config.GetPolicyManager()
	return nil
}

var (
	errMissingNonce     = errors.New("missing nonce in signature header")
	errInvalidTxID      = errors.New("invalid tx id")
	errMissingChannelID = errors.New("missing channel id in channel header")
	errWrongChannelID   = errors.New("wrong channel id in channel header")
)

// verifyTxID checks that the signature header carries a nonce, and that the channel header's TxId
// is the one derived from the signature header's nonce and creator.
// It returns constant errors, so a rejected request does not echo its (unbounded) TxID into errors and logs.
// It rejects a TxID of the wrong length before calling protoutil.CheckTxID, which formats the TxID into its error.
func verifyTxID(chdr *common.ChannelHeader, shdr *common.SignatureHeader) error {
	if len(shdr.GetNonce()) == 0 {
		return errMissingNonce
	}
	if len(chdr.GetTxId()) != 2*sha256.Size {
		return errInvalidTxID
	}
	if protoutil.CheckTxID(chdr.GetTxId(), shdr.GetNonce(), shdr.GetCreator()) != nil {
		return errInvalidTxID
	}
	return nil
}
