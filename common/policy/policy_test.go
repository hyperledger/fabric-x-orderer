/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package policy_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/policy"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/fabric"
	"github.com/stretchr/testify/require"
)

func TestBundleAndResourcesFromBlock(t *testing.T) {
	dir := t.TempDir()

	sharedConfigYaml, sharedConfigPath := testutil.PrepareSharedConfigBinary(t, dir)

	block, err := generate.CreateGenesisBlock(dir, sharedConfigYaml, sharedConfigPath, fabric.GetDevConfigDir())
	require.NoError(t, err)
	require.NotNil(t, block)

	require.True(t, block.Data != nil)
	require.NotEqual(t, len(block.Data.Data), 0)

	env, err := protoutil.GetEnvelopeFromBlock(block.Data.Data[0])
	require.NoError(t, err)
	require.NotNil(t, env)

	bundle, err := policy.BuildBundleFromBlock(env, factory.GetDefault())
	require.NoError(t, err)
	require.NotNil(t, bundle)

	policyManager := bundle.PolicyManager()
	require.NotNil(t, policyManager)

	policy, exists := policyManager.GetPolicy(policies.ChannelWriters)
	require.True(t, exists)
	require.NotNil(t, policy)

	policy, exists = policyManager.GetPolicy(policies.ChannelReaders)
	require.True(t, exists)
	require.NotNil(t, policy)

	policy, exists = policyManager.GetPolicy(policies.ChannelOrdererAdmins)
	require.True(t, exists)
	require.NotNil(t, policy)

	configtxValidator := bundle.ConfigtxValidator()
	require.NotNil(t, configtxValidator)

	ordererConfig, exists := bundle.OrdererConfig()
	require.True(t, exists)
	require.NotNil(t, ordererConfig)
	require.Equal(t, ordererConfig.ConsensusType(), "arma")
	require.NotNil(t, ordererConfig.ConsensusMetadata())
	require.NotNil(t, ordererConfig.Organizations())
	require.NotNil(t, ordererConfig.BatchSize())
	require.NotNil(t, ordererConfig.BatchTimeout())
	require.NotNil(t, ordererConfig.Consenters())
}
