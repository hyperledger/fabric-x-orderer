package config_test

import (
	"fmt"
	"path"
	"testing"

	"github.ibm.com/decentralized-trust-research/arma/cmd/armageddon"
	"github.ibm.com/decentralized-trust-research/arma/common/utils"
	"github.ibm.com/decentralized-trust-research/arma/config"
	"github.ibm.com/decentralized-trust-research/arma/config/generate"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.com/stretchr/testify/require"
)

func TestLocalConfigLoadYaml(t *testing.T) {
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

func TestLoadLocalConfigAndCrypto(t *testing.T) {
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
		routerLocalConfigLoaded, tlsConfig, clusterConfig, err := config.Load(configPath)
		require.NoError(t, err)
		require.NotNil(t, routerLocalConfigLoaded)
		require.NotNil(t, tlsConfig)
		require.NotNil(t, clusterConfig)
		require.Equal(t, routerLocalConfigLoaded.GeneralConfig.TLSConfig.Enabled, true)
		require.Equal(t, routerLocalConfigLoaded.GeneralConfig.TLSConfig.ClientAuthRequired, true)

		for j := 1; j <= len(networkLocalConfig.PartiesLocalConfig[i-1].BatchersLocalConfig); j++ {
			configPath = path.Join(dir, "config", fmt.Sprintf("party%d", i), fmt.Sprintf("local_config_batcher%d.yaml", j))
			batcherLocalConfigLoaded, tlsConfig, clusterConfig, err := config.Load(configPath)
			require.NoError(t, err)
			require.NotNil(t, batcherLocalConfigLoaded)
			require.NotNil(t, tlsConfig)
			require.NotNil(t, clusterConfig)
		}

		configPath = path.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_consenter.yaml")
		consenterLocalConfigLoaded, tlsConfig, clusterConfig, err := config.Load(configPath)
		require.NoError(t, err)
		require.NotNil(t, consenterLocalConfigLoaded)
		require.NotNil(t, tlsConfig)
		require.NotNil(t, clusterConfig)

		configPath = path.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_assembler.yaml")
		assemblerLocalConfigLoaded, tlsConfig, clusterConfig, err := config.Load(configPath)
		require.NoError(t, err)
		require.NotNil(t, assemblerLocalConfigLoaded)
		require.NotNil(t, tlsConfig)
		require.NotNil(t, clusterConfig)
		require.Equal(t, assemblerLocalConfigLoaded.GeneralConfig.TLSConfig.Enabled, true)
		require.Equal(t, assemblerLocalConfigLoaded.GeneralConfig.TLSConfig.ClientAuthRequired, true)
	}
}
