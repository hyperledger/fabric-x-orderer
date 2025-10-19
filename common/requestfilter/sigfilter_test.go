/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter_test

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/policies"
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
	_, err := v.Verify(nil)
	require.EqualError(t, err, "failed to convert request to signedData : nil request")

	req := &comm.Request{}
	_, err = v.Verify(req)
	require.EqualError(t, err, "failed to convert request to signedData : missing header in request's payload")

	payload := &common.Payload{Header: &common.Header{ChannelHeader: make([]byte, 10), SignatureHeader: nil}}
	p, err := proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	_, err = v.Verify(req)
	require.EqualError(t, err, "failed to convert request to signedData : missing signature header in payload's header")

	payload = &common.Payload{Header: &common.Header{ChannelHeader: make([]byte, 10), SignatureHeader: make([]byte, 10)}}
	p, err = proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	_, err = v.Verify(req)
	require.ErrorContains(t, err, "failed unmarshalling signature header")

	sigheader, err := proto.Marshal(&common.SignatureHeader{
		Creator: []byte("user"),
		Nonce:   []byte("nonce"),
	})
	require.NoError(t, err)

	payload = &common.Payload{Header: &common.Header{ChannelHeader: make([]byte, 10), SignatureHeader: sigheader}}
	p, err = proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	_, err = v.Verify(req)
	require.ErrorContains(t, err, "failed unmarshalling channel header")

	chdr := &common.ChannelHeader{ChannelId: "ChannelId"}
	chdrBytes, err := proto.Marshal(chdr)
	require.NoError(t, err)
	payload = &common.Payload{Header: &common.Header{ChannelHeader: chdrBytes, SignatureHeader: sigheader}}
	p, err = proto.Marshal(payload)
	require.NoError(t, err)
	req.Payload = p
	_, err = v.Verify(req)
	require.NoError(t, err)
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

	_, err := v.Verify(req)
	require.ErrorContains(t, err, "no policies in config block")

	pm.GetPolicyReturns(p, true)
	_, err = v.Verify(req)
	require.NoError(t, err)

	p.EvaluateSignedDataReturns(errors.New("error"))
	_, err = v.Verify(req)
	require.ErrorContains(t, err, "signature did not satisfy policy")

	p.EvaluateSignedDataReturns(nil)
	_, err = v.Verify(req)
	require.NoError(t, err)

	fc.GetClientSignatureVerificationRequiredReturns(false)
	err = v.Update(fc)
	require.NoError(t, err)
	_, err = v.Verify(req)
	require.NoError(t, err)
}

func TestSigFilterType(t *testing.T) {
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

	t.Run("data request", func(t *testing.T) {
		dataReq := tx.CreateStructuredRequest([]byte("123"))
		reqType, err := v.Verify(dataReq)
		require.NoError(t, err)
		require.Equal(t, "data", reqType)
	})

	t.Run("config request", func(t *testing.T) {
		sigheader, err := proto.Marshal(&common.SignatureHeader{
			Creator: []byte("user"),
			Nonce:   []byte("nonce"),
		})
		require.NoError(t, err)
		chdr := &common.ChannelHeader{Type: int32(common.HeaderType_CONFIG), ChannelId: "ChannelId"}
		chdrBytes, err := proto.Marshal(chdr)
		require.NoError(t, err)
		payload := &common.Payload{Header: &common.Header{ChannelHeader: chdrBytes, SignatureHeader: sigheader}}
		p, err := proto.Marshal(payload)
		require.NoError(t, err)
		configReq := &comm.Request{Payload: p}
		reqType, err := v.Verify(configReq)
		require.NoError(t, err)
		require.Equal(t, "config", reqType)
	})

	t.Run("config-update request", func(t *testing.T) {
		sigheader, err := proto.Marshal(&common.SignatureHeader{
			Creator: []byte("user"),
			Nonce:   []byte("nonce"),
		})
		require.NoError(t, err)
		chdr := &common.ChannelHeader{Type: int32(common.HeaderType_CONFIG_UPDATE), ChannelId: "ChannelId"}
		chdrBytes, err := proto.Marshal(chdr)
		require.NoError(t, err)
		payload := &common.Payload{Header: &common.Header{ChannelHeader: chdrBytes, SignatureHeader: sigheader}}
		p, err := proto.Marshal(payload)
		require.NoError(t, err)
		configReq := &comm.Request{Payload: p}
		reqType, err := v.Verify(configReq)
		require.NoError(t, err)
		require.Equal(t, "config_update", reqType)
	})

	t.Run("unsupported request type", func(t *testing.T) {
		sigheader, err := proto.Marshal(&common.SignatureHeader{
			Creator: []byte("user"),
			Nonce:   []byte("nonce"),
		})
		require.NoError(t, err)
		chdr := &common.ChannelHeader{Type: 777, ChannelId: "ChannelId"}
		chdrBytes, err := proto.Marshal(chdr)
		require.NoError(t, err)
		payload := &common.Payload{Header: &common.Header{ChannelHeader: chdrBytes, SignatureHeader: sigheader}}
		p, err := proto.Marshal(payload)
		require.NoError(t, err)
		configReq := &comm.Request{Payload: p}
		reqType, err := v.Verify(configReq)
		require.ErrorContains(t, err, "request header type is not config or data")
		require.Equal(t, "", reqType)
	})
}
