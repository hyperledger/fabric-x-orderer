/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver_test

import (
	"errors"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	policymocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	testmocks "github.com/hyperledger/fabric-x-orderer/test/mocks"

	"github.com/stretchr/testify/require"
)

// checkerOverPolicy returns a checker whose bundle holds a single policy, reachable under any
// name when found is true, along with the evaluator that policy delegates to.
func checkerOverPolicy(found bool, evaluationErr error) (*deliver.ChannelReadersChecker, *policymocks.FakePolicyManager, *policymocks.FakePolicyEvaluator) {
	evaluator := &policymocks.FakePolicyEvaluator{}
	evaluator.EvaluateSignedDataReturns(evaluationErr)

	policyManager := &policymocks.FakePolicyManager{}
	if found {
		policyManager.GetPolicyReturns(evaluator, true)
	} else {
		policyManager.GetPolicyReturns(nil, false)
	}

	bundle := &testmocks.FakeConfigResources{}
	bundle.PolicyManagerReturns(policyManager)

	return deliver.NewChannelReadersChecker(bundle), policyManager, evaluator
}

// Scenario:
//  1. Check a well formed envelope against a policy that accepts it, and expect no error, the
//     channel's Readers policy to be the one looked up, and the envelope's payload, signature
//     and creator to be the signed data the policy was given.
//  2. Check an envelope whose payload cannot be converted to signed data, and expect a
//     conversion error and no policy evaluation.
//  3. Check an envelope while the bundle holds no Readers policy, and expect an error.
//  4. Check an envelope against a policy that rejects it, and expect the rejection to surface.
func TestChannelReadersChecker_CheckPolicy(t *testing.T) {
	creator := []byte("creator-identity")
	env, err := protoutil.CreateSignedEnvelope(common.HeaderType_DELIVER_SEEK_INFO, "arma",
		&fixedSigner{creator: creator}, &common.ConfigUpdate{}, int32(0), uint64(0))
	require.NoError(t, err)

	t.Run("Authorized", func(t *testing.T) {
		checker, policyManager, evaluator := checkerOverPolicy(true, nil)

		require.NoError(t, checker.CheckPolicy(env, "arma"))

		require.Equal(t, 1, policyManager.GetPolicyCallCount())
		require.Equal(t, policies.ChannelReaders, policyManager.GetPolicyArgsForCall(0))

		require.Equal(t, 1, evaluator.EvaluateSignedDataCallCount())
		signedData := evaluator.EvaluateSignedDataArgsForCall(0)
		require.Len(t, signedData, 1)
		require.Equal(t, env.Payload, signedData[0].Data)
		require.Equal(t, env.Signature, signedData[0].Signature)
		require.Equal(t, creator, signedData[0].Identity.GetCertificate())
	})

	t.Run("EnvelopeAsSignedDataFailure", func(t *testing.T) {
		checker, _, evaluator := checkerOverPolicy(true, nil)

		err := checker.CheckPolicy(&common.Envelope{Payload: []byte("not a payload")}, "arma")
		require.ErrorContains(t, err, "could not convert message to signedData")
		require.Zero(t, evaluator.EvaluateSignedDataCallCount())
	})

	t.Run("PolicyNotFound", func(t *testing.T) {
		checker, _, _ := checkerOverPolicy(false, nil)

		err := checker.CheckPolicy(env, "arma")
		require.ErrorContains(t, err, "could not find policy "+policies.ChannelReaders)
	})

	t.Run("Unauthorized", func(t *testing.T) {
		checker, _, _ := checkerOverPolicy(true, errors.New("signature set did not satisfy policy"))

		err := checker.CheckPolicy(env, "arma")
		require.ErrorContains(t, err, "evaluation failed")
		require.ErrorContains(t, err, "signature set did not satisfy policy")
	})
}

// fixedSigner serializes a caller supplied identity, so a test can assert which identity reached
// the policy. It does not produce a verifiable signature.
type fixedSigner struct {
	creator []byte
}

func (s *fixedSigner) Sign(message []byte) ([]byte, error) {
	return []byte("signature"), nil
}

func (s *fixedSigner) Serialize() ([]byte, error) {
	return protoutil.MarshalOrPanic(&msppb.Identity{
		MspId:   "org1",
		Creator: &msppb.Identity_Certificate{Certificate: s.creator},
	}), nil
}
