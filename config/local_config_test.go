/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"fmt"
	"path"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/require"
)

func TestLocalConfigLoadSingleYaml(t *testing.T) {
	dir := t.TempDir()
	require.DirExists(t, dir)

	routerLocalConfig := testutil.CreateTestRouterLocalConfig()

	configPath := path.Join(dir, "local_router_config.yaml")
	err := utils.WriteToYAML(routerLocalConfig, configPath)
	require.NoError(t, err)

	routerLocalConfigLoaded, err := config.LoadLocalConfigYaml(configPath)
	require.NoError(t, err)
	require.Equal(t, *routerLocalConfigLoaded, *routerLocalConfig)
}

func TestLoadARMALocalConfigAndCrypto(t *testing.T) {
	dir := t.TempDir()
	require.DirExists(t, dir)

	// 1.
	networkConfig := testutil.GenerateNetworkConfig(t, "mTLS", "mTLS")
	err := armageddon.GenerateCryptoConfig(&networkConfig, dir)
	require.NoError(t, err)

	// 2.
	networkLocalConfig, err := generate.CreateArmaLocalConfig(networkConfig, dir, dir)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)

	// 3.
	for i := 1; i <= len(networkLocalConfig.PartiesLocalConfig); i++ {
		configPath := path.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_router.yaml")
		routerLocalConfigLoaded, role, err := config.LoadLocalConfig(configPath)
		require.NoError(t, err)
		require.NotNil(t, routerLocalConfigLoaded)
		require.NotNil(t, routerLocalConfigLoaded.NodeLocalConfig)
		require.NotNil(t, routerLocalConfigLoaded.TLSConfig)
		require.NotNil(t, routerLocalConfigLoaded.ClusterConfig)
		require.Equal(t, routerLocalConfigLoaded.NodeLocalConfig.GeneralConfig.TLSConfig.Enabled, true)
		require.Equal(t, routerLocalConfigLoaded.NodeLocalConfig.GeneralConfig.TLSConfig.ClientAuthRequired, true)
		require.Equal(t, role, "Router")

		for j := 1; j <= len(networkLocalConfig.PartiesLocalConfig[i-1].BatchersLocalConfig); j++ {
			configPath = path.Join(dir, "config", fmt.Sprintf("party%d", i), fmt.Sprintf("local_config_batcher%d.yaml", j))
			batcherLocalConfigLoaded, role, err := config.LoadLocalConfig(configPath)
			require.NoError(t, err)
			require.NotNil(t, batcherLocalConfigLoaded.NodeLocalConfig)
			require.NotNil(t, batcherLocalConfigLoaded.TLSConfig)
			require.NotNil(t, batcherLocalConfigLoaded.ClusterConfig)
			require.Equal(t, role, "Batcher")
		}

		configPath = path.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_consenter.yaml")
		consenterLocalConfigLoaded, role, err := config.LoadLocalConfig(configPath)
		require.NoError(t, err)
		require.NotNil(t, consenterLocalConfigLoaded.NodeLocalConfig)
		require.NotNil(t, consenterLocalConfigLoaded.TLSConfig)
		require.NotNil(t, consenterLocalConfigLoaded.ClusterConfig)
		require.Equal(t, role, "Consensus")

		configPath = path.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_assembler.yaml")
		assemblerLocalConfigLoaded, role, err := config.LoadLocalConfig(configPath)
		require.NoError(t, err)
		require.NotNil(t, assemblerLocalConfigLoaded.NodeLocalConfig)
		require.NotNil(t, assemblerLocalConfigLoaded.TLSConfig)
		require.NotNil(t, assemblerLocalConfigLoaded.ClusterConfig)
		require.Equal(t, assemblerLocalConfigLoaded.NodeLocalConfig.GeneralConfig.TLSConfig.Enabled, true)
		require.Equal(t, assemblerLocalConfigLoaded.NodeLocalConfig.GeneralConfig.TLSConfig.ClientAuthRequired, true)
		require.Equal(t, role, "Assembler")
	}
}

func TestLoadLocalConfigYaml_Errors(t *testing.T) {
	res, err := config.LoadLocalConfigYaml("")
	require.Nil(t, res)
	require.EqualError(t, err, "cannot load local node configuration, path:  is empty")

	res, err = config.LoadLocalConfigYaml("File_not_exists")
	require.Nil(t, res)
	require.EqualError(t, err, "open File_not_exists: no such file or directory")
	require.Error(t, err)
}
