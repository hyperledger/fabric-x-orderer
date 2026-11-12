/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Build a consenter on channel "arma", with the rules verifier CreateConsensusRulesVerifier builds.
//  2. Verify config requests whose channel header names "arma", another channel, or none, as SubmitConfig does.
//  3. Expect only the requests of channel "arma" to reach the config update proposer.
func TestVerifyAndClassifyRequestChecksChannelID(t *testing.T) {
	configtxValidator := &policyMocks.FakeConfigtxValidator{}
	configtxValidator.ChannelIDReturns("arma")
	policyManager := &policyMocks.FakePolicyManager{}
	policyManager.GetPolicyReturns(&policyMocks.FakePolicyEvaluator{}, true)
	bundle := &configMocks.FakeConfigResources{}
	bundle.ConfigtxValidatorReturns(configtxValidator)
	bundle.PolicyManagerReturns(policyManager)
	config := &nodeconfig.ConsenterNodeConfig{Bundle: bundle, RequestMaxBytes: 1 << 10}

	const (
		missing = "missing channel id in channel header"
		wrong   = "wrong channel id in channel header"
	)
	for _, tc := range []struct {
		name      string
		reqType   common.HeaderType
		channelID string
		errMsg    string
	}{
		{name: "config update of the channel", reqType: common.HeaderType_CONFIG_UPDATE, channelID: "arma"},
		{name: "config update with no channel", reqType: common.HeaderType_CONFIG_UPDATE, channelID: "", errMsg: missing},
		{
			name: "config update of another channel", reqType: common.HeaderType_CONFIG_UPDATE, channelID: "other",
			errMsg: wrong,
		},
		{name: "config of the channel", reqType: common.HeaderType_CONFIG, channelID: "arma"},
		{name: "config with no channel", reqType: common.HeaderType_CONFIG, channelID: "", errMsg: missing},
		{name: "config of another channel", reqType: common.HeaderType_CONFIG, channelID: "other", errMsg: wrong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proposer := &policyMocks.FakeConfigUpdateProposer{}
			proposer.ProposeConfigUpdateReturns(&protos.Request{}, nil)
			c := &Consensus{
				Logger:               testutil.CreateLogger(t, 1),
				Config:               config,
				RequestVerifier:      CreateConsensusRulesVerifier(config),
				ConfigUpdateProposer: proposer,
			}

			_, err := c.verifyAndClassifyRequest(tx.CreateStructuredRequestOfChannel(tc.reqType, tc.channelID, []byte("data")))
			if tc.errMsg != "" {
				require.ErrorContains(t, err, tc.errMsg)
				require.Zero(t, proposer.ProposeConfigUpdateCallCount())
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, proposer.ProposeConfigUpdateCallCount())
		})
	}
}
