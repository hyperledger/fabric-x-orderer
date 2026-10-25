/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package generate_test

import (
	"os"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/hyperledger/fabric-x-orderer/config/generate"

	"github.com/stretchr/testify/require"
)

func TestSharedConfigGeneration(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	networkConfig := testutil.GenerateNetworkConfig(t, "none", "none")

	// 2.
	networkLocalConfig, err := generate.CreateArmaLocalConfig(networkConfig, dir, dir, false)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)

	// 3.
	networkSharedConfig, err := generate.CreateArmaSharedConfig(networkConfig, networkLocalConfig, dir, dir)
	require.NoError(t, err)
	require.NotNil(t, networkSharedConfig)
}

func TestSharedConfigGeneration_MaxPartyID(t *testing.T) {
	dir := t.TempDir()

	networkConfig := testutil.GenerateNetworkConfig(t, "none", "none")
	networkLocalConfig, err := generate.CreateArmaLocalConfig(networkConfig, dir, dir, false)
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		maxPartyID types.PartyID
		expected   types.PartyID
	}{
		{name: "unset", maxPartyID: 0, expected: 4},
		{name: "lower than the highest party ID", maxPartyID: 2, expected: 4},
		{name: "higher than the highest party ID", maxPartyID: 10, expected: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			network := networkConfig
			network.MaxPartyID = tc.maxPartyID

			sharedConfig, err := generate.CreateArmaSharedConfig(network, networkLocalConfig, dir, dir)
			require.NoError(t, err)
			require.Equal(t, tc.expected, sharedConfig.MaxPartyID)
		})
	}
}

func TestSharedConfigExtension_MaxPartyID(t *testing.T) {
	dir := t.TempDir()
	networkConfig := testutil.GenerateNetworkConfig(t, "none", "none")

	// Create a config with parties 1-3
	initialNetwork := networkConfig
	initialNetwork.Parties = networkConfig.Parties[:3]
	initialLocalConfig, err := generate.CreateArmaLocalConfig(initialNetwork, dir, dir, false)
	require.NoError(t, err)
	sharedConfig, err := generate.CreateArmaSharedConfig(initialNetwork, initialLocalConfig, dir, dir)
	require.NoError(t, err)
	require.Equal(t, types.PartyID(3), sharedConfig.MaxPartyID)

	// Extend the config with party 4
	addedNetwork := generate.Network{
		Parties:         networkConfig.Parties[3:],
		UseTLSRouter:    networkConfig.UseTLSRouter,
		UseTLSAssembler: networkConfig.UseTLSAssembler,
	}
	addedLocalConfig, err := generate.CreateArmaLocalConfig(addedNetwork, dir, dir, false)
	require.NoError(t, err)
	sharedConfig, err = generate.ExtendArmaSharedConfig(addedNetwork, addedLocalConfig, dir, dir)
	require.NoError(t, err)
	require.Len(t, sharedConfig.PartiesConfig, 4)
	require.Equal(t, types.PartyID(4), sharedConfig.MaxPartyID)

	// Recreate the config with parties 1-3 and MaxPartyID set to 10
	initialNetwork.MaxPartyID = 10
	sharedConfig, err = generate.CreateArmaSharedConfig(initialNetwork, initialLocalConfig, dir, dir)
	require.NoError(t, err)
	require.Equal(t, types.PartyID(10), sharedConfig.MaxPartyID)

	// Extend with party 4 without decreasing MaxPartyID
	sharedConfig, err = generate.ExtendArmaSharedConfig(addedNetwork, addedLocalConfig, dir, dir)
	require.NoError(t, err)
	require.Len(t, sharedConfig.PartiesConfig, 4)
	require.Equal(t, types.PartyID(10), sharedConfig.MaxPartyID)
}
