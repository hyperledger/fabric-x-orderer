/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/msp"

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

	chdr := &common.ChannelHeader{ChannelId: "arma", Type: int32(common.HeaderType_CONFIG_UPDATE)}
	chdrBytes, err := proto.Marshal(chdr)
	require.NoError(t, err)

	id, err := msp.NewSerializedIdentity("org1", []byte("cert"))
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

// stubDeserializer records how many times a signature set is converted into identities.
type stubDeserializer struct {
	calls int
}

func (s *stubDeserializer) DeserializeIdentity(identity *msppb.Identity) (msp.Identity, error) {
	s.calls++
	return &stubIdentity{}, nil
}

func (s *stubDeserializer) GetKnownDeserializedIdentity(id msp.IdentityIdentifier) msp.Identity {
	return nil
}

func (s *stubDeserializer) IsWellFormed(identity *msppb.Identity) error {
	return nil
}

// stubIdentity is the identity a stubDeserializer hands back; only Verify is exercised.
type stubIdentity struct {
	msp.Identity
}

func (s *stubIdentity) GetIdentifier() *msp.IdentityIdentifier {
	return &msp.IdentityIdentifier{Mspid: "org1", Id: "client"}
}

func (s *stubIdentity) Verify(msg []byte, sig []byte) error {
	return nil
}

func TestSigVerifyEvaluatesIdentitiesOnce(t *testing.T) {
	// Scenario:
	// 1. A filter config carries both a policy manager and an MSP manager.
	// 2. A structured request signed by org1 is verified.
	// 3. The signer is deserialized exactly once, not once per policy the tree tries.
	// 4. The policy is handed the resulting identity, so no sub-policy re-checks the signature.
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}
	pm := &policyMock.FakePolicyManager{}
	p := &policyMock.FakePolicyEvaluator{}
	deserializer := &stubDeserializer{}

	pm.GetPolicyReturns(p, true)
	fc.GetPolicyManagerReturns(pm)
	fc.GetMSPManagerReturns(deserializer)
	fc.GetClientSignatureVerificationRequiredReturns(true)

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

	_, err := v.VerifyStructureAndClassify(tx.CreateStructuredRequest([]byte("data")))
	require.NoError(t, err)
	require.Equal(t, 1, p.EvaluateIdentitiesCallCount())
	require.Equal(t, 0, p.EvaluateSignedDataCallCount())
	require.Equal(t, 1, deserializer.calls)
}

func TestSigVerifyWithoutMSPManagerEvaluatesSignedData(t *testing.T) {
	// Scenario:
	// 1. A filter config carries a policy manager but no MSP manager.
	// 2. A structured request signed by org1 is verified.
	// 3. The signature set cannot be converted, so the policy evaluates the set itself.
	var v requestfilter.RulesVerifier
	fc := &mocks.FakeFilterConfig{}
	pm := &policyMock.FakePolicyManager{}
	p := &policyMock.FakePolicyEvaluator{}

	pm.GetPolicyReturns(p, true)
	fc.GetPolicyManagerReturns(pm)
	fc.GetClientSignatureVerificationRequiredReturns(true)

	v.AddStructureRule(requestfilter.NewSigFilter(fc, policies.ChannelWriters))

	_, err := v.VerifyStructureAndClassify(tx.CreateStructuredRequest([]byte("data")))
	require.NoError(t, err)
	require.Equal(t, 1, p.EvaluateSignedDataCallCount())
	require.Equal(t, 0, p.EvaluateIdentitiesCallCount())
}
