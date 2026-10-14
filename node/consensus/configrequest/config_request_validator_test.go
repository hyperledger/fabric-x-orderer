/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configrequest_test

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMock "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/configrequest"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestValidateConfigRequestTxID(t *testing.T) {
	config := &common.Config{Sequence: 1}

	for _, tc := range []struct {
		name            string
		configTxID      func(nonce, creator []byte) string
		lastUpdateTxID  func(nonce, creator []byte) string
		lastUpdateNonce []byte
		errMsg          string
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
			errMsg:          "config envelope: invalid tx id",
		},
		{
			name:            "bad last update tx id",
			configTxID:      protoutil.ComputeTxID,
			lastUpdateTxID:  func(_, _ []byte) string { return "junk" },
			lastUpdateNonce: []byte("nonce"),
			errMsg:          "last update: invalid tx id",
		},
		{
			name:           "empty last update nonce",
			configTxID:     protoutil.ComputeTxID,
			lastUpdateTxID: protoutil.ComputeTxID,
			errMsg:         "last update: missing nonce in signature header",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lastUpdate := &common.Envelope{
				Payload: marshalPayload(t, common.HeaderType_CONFIG_UPDATE, tc.lastUpdateNonce, tc.lastUpdateTxID, &common.ConfigUpdateEnvelope{}),
			}
			envelope := &common.Envelope{
				Payload: marshalPayload(t, common.HeaderType_CONFIG, []byte("nonce"), tc.configTxID, &common.ConfigEnvelope{Config: config, LastUpdate: lastUpdate}),
			}

			proposer := &policyMock.FakeConfigUpdateProposer{}
			proposer.AuthorizeAndVerifyConfigUpdateReturns(&common.ConfigEnvelope{Config: config}, nil)
			validator := &configrequest.DefaultValidateConfigRequest{ConfigUpdateProposer: proposer}

			err := validator.ValidateConfigRequest(envelope)
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
				require.Zero(t, proposer.AuthorizeAndVerifyConfigUpdateCallCount())
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, proposer.AuthorizeAndVerifyConfigUpdateCallCount())
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
