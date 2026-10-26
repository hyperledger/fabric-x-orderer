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
		return reqType, fmt.Errorf("failed to convert request to signedData : %s", err)
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

// requestToSignedData verifies the request structure and returns the payload, identity and signature in a SignedData.
// additionally, the request tyoe is extracted and returned.
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

	// TODO: check channel ID
	// if sf.channelID != chdr.ChannelId {
	// 	return nil, fmt.Errorf("channelID is incorrect. expected: %s, actual: %s", sf.channelID, chdr.ChannelId)
	// }

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

func (sf *SigFilter) Update(config FilterConfig) error {
	sf.clientSignatureVerificationRequired = config.GetClientSignatureVerificationRequired()
	sf.channelID = config.GetChannelID()
	sf.policyManager = config.GetPolicyManager()
	return nil
}

var (
	errMissingNonce = errors.New("missing nonce in signature header")
	errInvalidTxID  = errors.New("invalid tx id")
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
