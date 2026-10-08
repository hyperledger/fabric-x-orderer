/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver_test

import (
	"bytes"
	"errors"
	"testing"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/stretchr/testify/require"
)

// An access control is the policy checker of a deliver service.
var _ deliver.PolicyChecker = (*deliver.AccessControl)(nil)

// Scenario:
// 1. Build an access control from no bundle, and from a bundle that holds no policy of that name.
// 2. Expect an error that names the reason, and no access control.
func TestNewAccessControl(t *testing.T) {
	accessControl, err := deliver.NewAccessControl(nil, policies.ChannelOrdererReaders)
	require.ErrorContains(t, err, "no configuration bundle")
	require.Nil(t, accessControl)

	policyManager := &policyMocks.FakePolicyManager{}
	policyManager.GetPolicyReturns(nil, false)
	bundle := &mocks.FakeConfigResources{}
	bundle.PolicyManagerReturns(policyManager)

	accessControl, err = deliver.NewAccessControl(bundle, policies.ChannelOrdererReaders)
	require.ErrorContains(t, err, "the configuration holds no policy /Channel/Orderer/Readers")
	require.Nil(t, accessControl)
}

// Scenario:
//  1. Build an access control over a bundle whose policy admits only a request signed with one
//     certificate.
//  2. Expect the policy to be taken from the bundle by its name.
//  3. Check a request signed with that certificate, and expect it to be authorized, also when it
//     names another channel.
//  4. Check a request signed with another certificate, a request that cannot be parsed, and no
//     request, and expect each to be refused with an error that names the reason.
func TestAccessControlCheckPolicy(t *testing.T) {
	admittedSigner, admittedCert := signutil.NewSelfSignedSigner(t, "org")
	otherSigner, _ := signutil.NewSelfSignedSigner(t, "org")

	policy := &policyMocks.FakePolicyEvaluator{}
	policy.EvaluateSignedDataStub = func(signedData []*protoutil.SignedData) error {
		if len(signedData) == 1 && bytes.Equal(signedData[0].Identity.GetCertificate(), admittedCert) {
			return nil
		}
		return errors.New("signature set did not satisfy policy")
	}
	policyManager := &policyMocks.FakePolicyManager{}
	policyManager.GetPolicyReturns(policy, true)
	bundle := &mocks.FakeConfigResources{}
	bundle.PolicyManagerReturns(policyManager)

	accessControl, err := deliver.NewAccessControl(bundle, policies.ChannelOrdererReaders)
	require.NoError(t, err)
	require.Equal(t, policies.ChannelOrdererReaders, policyManager.GetPolicyArgsForCall(0))

	require.NoError(t, accessControl.CheckPolicy(deliverRequest(t, admittedSigner, "arma"), "arma"))
	require.NoError(t, accessControl.CheckPolicy(deliverRequest(t, admittedSigner, "other"), "other"))

	tests := []struct {
		name    string
		request *cb.Envelope
		error   string
	}{
		{
			name:    "a request signed with another certificate",
			request: deliverRequest(t, otherSigner, "arma"),
			error:   "the request does not satisfy /Channel/Orderer/Readers: signature set did not satisfy policy",
		},
		{
			name:    "a request that cannot be parsed",
			request: &cb.Envelope{Payload: []byte("not a payload"), Signature: []byte("signature")},
			error:   "could not convert the request to signed data",
		},
		{
			name:    "no request",
			request: nil,
			error:   "could not convert the request to signed data",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorContains(t, accessControl.CheckPolicy(test.request, "arma"), test.error)
		})
	}
}

// deliverRequest is a deliver request for the channel, signed by the signer.
func deliverRequest(t *testing.T, signer identity.SignerSerializer, channelID string) *cb.Envelope {
	t.Helper()

	request, err := protoutil.CreateSignedEnvelope(
		cb.HeaderType_DELIVER_SEEK_INFO,
		channelID,
		signer,
		&orderer.SeekInfo{
			Start: &orderer.SeekPosition{Type: &orderer.SeekPosition_Oldest{Oldest: &orderer.SeekOldest{}}},
			Stop:  &orderer.SeekPosition{Type: &orderer.SeekPosition_Newest{Newest: &orderer.SeekNewest{}}},
		},
		int32(0),
		uint64(0),
	)
	require.NoError(t, err)

	return request
}
