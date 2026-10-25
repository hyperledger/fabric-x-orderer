/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hyperledger/fabric-x-common/msp"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMock "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestSigVerifyFilter(t *testing.T) {
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))
	_, err := v.VerifyStructureAndClassify(nil)
	require.EqualError(t, err, "failed to convert request to signedData : nil request")

	req := &comm.Request{}
	_, err = v.VerifyStructureAndClassify(req)
	require.EqualError(t, err, "failed to convert request to signedData : missing header in request's payload")

	payload := &common.Payload{Header: &common.Header{ChannelHeader: make([]byte, 10), SignatureHeader: nil}}
	p, err := proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	_, err = v.VerifyStructureAndClassify(req)
	require.EqualError(t, err, "failed to convert request to signedData : missing signature header in payload's header")

	payload = &common.Payload{Header: &common.Header{ChannelHeader: make([]byte, 10), SignatureHeader: make([]byte, 10)}}
	p, err = proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	_, err = v.VerifyStructureAndClassify(req)
	require.ErrorContains(t, err, "failed unmarshalling signature header")

	id, err := msp.NewSerializedIdentity("org1", []byte("cert"))
	require.NoError(t, err)

	sigheader, err := proto.Marshal(&common.SignatureHeader{
		Creator: id,
		Nonce:   []byte("nonce"),
	})
	require.NoError(t, err)

	payload = &common.Payload{Header: &common.Header{ChannelHeader: make([]byte, 10), SignatureHeader: sigheader}}
	p, err = proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	_, err = v.VerifyStructureAndClassify(req)
	require.ErrorContains(t, err, "failed unmarshalling channel header")

	chdr := &common.ChannelHeader{ChannelId: "ChannelId", Type: int32(common.HeaderType_MESSAGE)}
	chdrBytes, err := proto.Marshal(chdr)
	require.NoError(t, err)
	payload = &common.Payload{Header: &common.Header{ChannelHeader: chdrBytes, SignatureHeader: sigheader}}
	p, err = proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	_, err = v.VerifyStructureAndClassify(req)
	require.EqualError(t, err, "failed to convert request to signedData : invalid tx id")

	chdr.TxId = protoutil.ComputeTxID([]byte("nonce"), id)
	chdrBytes, err = proto.Marshal(chdr)
	require.NoError(t, err)
	payload = &common.Payload{Header: &common.Header{ChannelHeader: chdrBytes, SignatureHeader: sigheader}}
	p, err = proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	reqType, err := v.VerifyStructureAndClassify(req)
	require.NoError(t, err)
	require.Equal(t, common.HeaderType_MESSAGE, reqType)
}

func TestSigVerifyConfigUpdate(t *testing.T) {
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}

	policy := &policyMock.FakePolicyEvaluator{}
	policy.EvaluateSignedDataReturns(nil)
	policyManager := &policyMock.FakePolicyManager{}
	policyManager.GetPolicyReturns(policy, true)
	fc.GetPolicyManagerReturns(policyManager)
	fc.GetChannelIDReturns("arma")
	fc.GetClientSignatureVerificationRequiredReturns(false)

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))
	_, err := v.VerifyStructureAndClassify(nil)
	require.EqualError(t, err, "failed to convert request to signedData : nil request")

	id, err := msp.NewSerializedIdentity("org1", []byte("cert"))
	require.NoError(t, err)

	chdr := &common.ChannelHeader{ChannelId: "arma", Type: int32(common.HeaderType_CONFIG_UPDATE), TxId: protoutil.ComputeTxID([]byte("nonce"), id)}
	chdrBytes, err := proto.Marshal(chdr)
	require.NoError(t, err)

	sigheader, err := proto.Marshal(&common.SignatureHeader{
		Creator: id,
		Nonce:   []byte("nonce"),
	})
	require.NoError(t, err)
	payload := &common.Payload{Header: &common.Header{ChannelHeader: chdrBytes, SignatureHeader: sigheader}}
	p, err := proto.Marshal(payload)
	require.NoError(t, err)
	req := &comm.Request{Payload: p}
	req.Payload = p
	reqType, err := v.VerifyStructureAndClassify(req)
	require.NoError(t, err)
	require.Equal(t, common.HeaderType_CONFIG_UPDATE, reqType)
}

func TestSigVerifyTxID(t *testing.T) {
	id, err := msp.NewSerializedIdentity("org1", []byte("cert"))
	require.NoError(t, err)
	otherID, err := msp.NewSerializedIdentity("org2", []byte("cert"))
	require.NoError(t, err)
	nonce := []byte("nonce")

	for _, tc := range []struct {
		name   string
		nonce  []byte
		txID   string
		errMsg string
	}{
		{name: "correct tx id", nonce: nonce, txID: protoutil.ComputeTxID(nonce, id)},
		{name: "empty tx id", nonce: nonce, txID: "", errMsg: "invalid tx id"},
		{name: "wrong tx id", nonce: nonce, txID: "junk", errMsg: "invalid tx id"},
		{name: "tx id of another nonce", nonce: nonce, txID: protoutil.ComputeTxID([]byte("other"), id), errMsg: "invalid tx id"},
		{name: "tx id of wrong length", nonce: nonce, txID: protoutil.ComputeTxID(nonce, id) + "0", errMsg: "invalid tx id"},
		{name: "upper-case tx id", nonce: nonce, txID: strings.ToUpper(protoutil.ComputeTxID(nonce, id)), errMsg: "invalid tx id"},
		{name: "tx id of another creator", nonce: nonce, txID: protoutil.ComputeTxID(nonce, otherID), errMsg: "invalid tx id"},
		{name: "empty nonce", nonce: nil, txID: protoutil.ComputeTxID(nil, id), errMsg: "missing nonce in signature header"},
	} {
		for _, reqType := range []common.HeaderType{common.HeaderType_MESSAGE, common.HeaderType_CONFIG_UPDATE} {
			for _, sigVerify := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/sigVerify=%t", tc.name, reqType, sigVerify), func(t *testing.T) {
					policy := &policyMock.FakePolicyEvaluator{}
					policyManager := &policyMock.FakePolicyManager{}
					policyManager.GetPolicyReturns(policy, true)
					fc := &mocks.FakeFilterConfig{}
					fc.GetPolicyManagerReturns(policyManager)
					fc.GetClientSignatureVerificationRequiredReturns(sigVerify)

					var v requestfilter.RulesVerifier
					v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

					chdr, err := proto.Marshal(&common.ChannelHeader{ChannelId: "arma", Type: int32(reqType), TxId: tc.txID})
					require.NoError(t, err)
					shdr, err := proto.Marshal(&common.SignatureHeader{Creator: id, Nonce: tc.nonce})
					require.NoError(t, err)
					p, err := proto.Marshal(&common.Payload{Header: &common.Header{ChannelHeader: chdr, SignatureHeader: shdr}})
					require.NoError(t, err)

					classified, err := v.VerifyStructureAndClassify(&comm.Request{Payload: p})
					if tc.errMsg != "" {
						require.ErrorContains(t, err, tc.errMsg)
						return
					}
					require.NoError(t, err)
					require.Equal(t, reqType, classified)
				})
			}
		}
	}
}

func TestSigValidationFlag(t *testing.T) {
	var v requestfilter.RulesVerifier
	req := tx.CreateStructuredRequest([]byte("data"))
	fc := &mocks.FakeFilterConfig{}
	pm := &policyMock.FakePolicyManager{}
	p := &policyMock.FakePolicyEvaluator{}

	pm.GetPolicyReturns(p, false)
	fc.GetPolicyManagerReturns(pm)
	fc.GetClientSignatureVerificationRequiredReturns(true)

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

	_, err := v.VerifyStructureAndClassify(req)
	require.ErrorContains(t, err, "no policies in config block")

	pm.GetPolicyReturns(p, true)
	_, err = v.VerifyStructureAndClassify(req)
	require.NoError(t, err)

	p.EvaluateSignedDataReturns(errors.New("error"))
	_, err = v.VerifyStructureAndClassify(req)
	require.ErrorContains(t, err, "signature did not satisfy policy")

	p.EvaluateSignedDataReturns(nil)
	_, err = v.VerifyStructureAndClassify(req)
	require.NoError(t, err)

	fc.GetClientSignatureVerificationRequiredReturns(false)
	err = v.Update(fc)
	require.NoError(t, err)
	_, err = v.VerifyStructureAndClassify(req)
	require.NoError(t, err)
}

func TestSigFilterType(t *testing.T) {
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

	t.Run("data request", func(t *testing.T) {
		dataReq := tx.CreateStructuredRequest([]byte("123"))
		reqType, err := v.VerifyStructureAndClassify(dataReq)
		require.NoError(t, err)
		require.Equal(t, common.HeaderType_MESSAGE, reqType)
	})
}
