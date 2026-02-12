/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler_test

import (
	"errors"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	policymocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	testmocks "github.com/hyperledger/fabric-x-orderer/test/mocks"

	"github.com/stretchr/testify/require"
)

func TestAssemblerDeliverServiceSigFilter_Apply(t *testing.T) {
	// shared setup
	cfg := &testmocks.FakeConfigResources{}
	pm := &policymocks.FakePolicyManager{}

	validEnv, err := protoutil.CreateSignedEnvelope(common.HeaderType_MESSAGE, "", nil, &common.ConfigUpdate{}, int32(0), uint64(0))
	require.NoError(t, err)
	invalidEnv := &common.Envelope{Payload: []byte("not a valid payload")}

	t.Run("EnvelopeAsSignedDataFailure", func(t *testing.T) {
		// return a default evaluator that succeeds unless configured otherwise
		pm.GetPolicyReturns(&policymocks.FakePolicyEvaluator{}, true)
		cfg.PolicyManagerReturns(pm)

		asf := assembler.NewAssemblerSigFilter(cfg)

		err := asf.Apply(invalidEnv)
		require.ErrorContains(t, err, "could not convert message to signedData")
	})

	t.Run("PolicyNotFound", func(t *testing.T) {
		pm.GetPolicyReturns(nil, false)
		cfg.PolicyManagerReturns(pm)

		asf := assembler.NewAssemblerSigFilter(cfg)

		err := asf.Apply(validEnv)
		require.ErrorContains(t, err, "could not find policy")
	})

	t.Run("PolicyEvaluationFailure", func(t *testing.T) {
		evaluator := &policymocks.FakePolicyEvaluator{}
		evaluator.EvaluateSignedDataReturns(errors.New("bad evaluate"))
		pm.GetPolicyReturns(evaluator, true)
		cfg.PolicyManagerReturns(pm)

		asf := assembler.NewAssemblerSigFilter(cfg)

		err := asf.Apply(validEnv)
		require.ErrorContains(t, err, "AssemblerSigFilter evaluation failed")
	})

	t.Run("Success", func(t *testing.T) {
		evaluator := &policymocks.FakePolicyEvaluator{}
		evaluator.EvaluateSignedDataReturns(nil)
		pm.GetPolicyReturns(evaluator, true)
		cfg.PolicyManagerReturns(pm)

		asf := assembler.NewAssemblerSigFilter(cfg)

		err := asf.Apply(validEnv)
		require.NoError(t, err)
		require.Equal(t, 1, evaluator.EvaluateSignedDataCallCount())
	})
}
