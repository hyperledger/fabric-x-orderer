/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configrequest_test

import (
	"errors"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMock "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/configrequest"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestValidateConfigRequestTxID(t *testing.T) {
	config := &common.Config{Sequence: 1}

	for _, tc := range []struct {
		name             string
		configTxID       func(nonce, creator []byte) string
		lastUpdateTxID   func(nonce, creator []byte) string
		lastUpdateNonce  []byte
		lastUpdateType   common.HeaderType
		lastUpdateSigErr error
		errMsg           string
	}{
		{
			name:            "valid tx ids",
			configTxID:      protoutil.ComputeTxID,
			lastUpdateTxID:  protoutil.ComputeTxID,
			lastUpdateNonce: []byte("nonce"),
		},
		{
			name:            "bad config envelope tx id",
			configTxID:      func(_, _ []byte) string { return "junk" },
			lastUpdateTxID:  protoutil.ComputeTxID,
			lastUpdateNonce: []byte("nonce"),
			errMsg:          "config envelope: failed to convert request to signedData : invalid tx id",
		},
		{
			name:            "bad last update tx id",
			configTxID:      protoutil.ComputeTxID,
			lastUpdateTxID:  func(_, _ []byte) string { return "junk" },
			lastUpdateNonce: []byte("nonce"),
			errMsg:          "last update: failed to convert request to signedData : invalid tx id",
		},
		{
			name:           "empty last update nonce",
			configTxID:     protoutil.ComputeTxID,
			lastUpdateTxID: protoutil.ComputeTxID,
			errMsg:         "last update: failed to convert request to signedData : missing nonce in signature header",
		},
		{
			name:             "last update signature does not satisfy policy",
			configTxID:       protoutil.ComputeTxID,
			lastUpdateTxID:   protoutil.ComputeTxID,
			lastUpdateNonce:  []byte("nonce"),
			lastUpdateSigErr: errors.New("bad signature"),
			errMsg:           "last update: signature did not satisfy policy",
		},
		{
			name:            "last update is not a config update",
			configTxID:      protoutil.ComputeTxID,
			lastUpdateTxID:  protoutil.ComputeTxID,
			lastUpdateNonce: []byte("nonce"),
			lastUpdateType:  common.HeaderType_ENDORSER_TRANSACTION,
			errMsg:          "last update: unexpected type ENDORSER_TRANSACTION, expected CONFIG_UPDATE",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lastUpdateType := common.HeaderType_CONFIG_UPDATE
			if tc.lastUpdateType != 0 {
				lastUpdateType = tc.lastUpdateType
			}
			lastUpdate := &common.Envelope{
				Payload:   marshalPayload(t, lastUpdateType, tc.lastUpdateNonce, tc.lastUpdateTxID, &common.ConfigUpdateEnvelope{}),
				Signature: []byte("signature"),
			}
			envelope := &common.Envelope{
				Payload:   marshalPayload(t, common.HeaderType_CONFIG, []byte("nonce"), tc.configTxID, &common.ConfigEnvelope{Config: config, LastUpdate: lastUpdate}),
				Signature: []byte("signature"),
			}

			// with client signature verification off, the policy is evaluated only for the CONFIG_UPDATE.
			policy := &policyMock.FakePolicyEvaluator{}
			policy.EvaluateSignedDataReturns(tc.lastUpdateSigErr)
			policyManager := &policyMock.FakePolicyManager{}
			policyManager.GetPolicyReturns(policy, true)
			fc := &mocks.FakeFilterConfig{}
			fc.GetPolicyManagerReturns(policyManager)
			verifier := requestfilter.NewRulesVerifier(nil)
			verifier.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

			proposer := &policyMock.FakeConfigUpdateProposer{}
			proposer.AuthorizeAndVerifyConfigUpdateReturns(&common.ConfigEnvelope{Config: config}, nil)
			validator := &configrequest.DefaultValidateConfigRequest{ConfigUpdateProposer: proposer, RequestVerifier: verifier}

			err := validator.ValidateConfigRequest(envelope)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
				require.Zero(t, proposer.AuthorizeAndVerifyConfigUpdateCallCount())
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, proposer.AuthorizeAndVerifyConfigUpdateCallCount())
			require.Equal(t, 1, policy.EvaluateSignedDataCallCount())
		})
	}
}

func marshalPayload(t *testing.T, headerType common.HeaderType, nonce []byte, txID func(nonce, creator []byte) string, data proto.Message) []byte {
	creator, err := msp.NewSerializedIdentity("org1", []byte("cert"))
	require.NoError(t, err)
	chdr := protoutil.MakeChannelHeader(headerType, 0, "arma", 0)
	chdr.TxId = txID(nonce, creator)
	shdr := protoutil.MakeSignatureHeader(creator, nonce)
	return protoutil.MarshalOrPanic(&common.Payload{
		Header: protoutil.MakePayloadHeader(chdr, shdr),
		Data:   protoutil.MarshalOrPanic(data),
	})
}
