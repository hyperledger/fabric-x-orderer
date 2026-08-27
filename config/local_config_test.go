/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
	_, err := armageddon.GenerateCryptoConfigWithProfile(&networkConfig, dir)
	require.NoError(t, err)

	// 2.
	networkLocalConfig, err := generate.CreateArmaLocalConfig(networkConfig, dir, dir, false)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)

	// 3.
	logger := testutil.CreateLoggerForModule(t, "LoadLocalConfig", zap.DebugLevel)
	for i := 1; i <= len(networkLocalConfig.PartiesLocalConfig); i++ {
		configPath := path.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_router.yaml")
		routerLocalConfigLoaded, role, err := config.LoadLocalConfig(configPath, logger)
		require.NoError(t, err)
		require.NotNil(t, routerLocalConfigLoaded)
		require.NotNil(t, routerLocalConfigLoaded.NodeLocalConfig)
		require.NotNil(t, routerLocalConfigLoaded.TLSConfig)
		require.NotNil(t, routerLocalConfigLoaded.ClusterConfig)
		require.Equal(t, routerLocalConfigLoaded.NodeLocalConfig.GeneralConfig.TLSConfig.Enabled, true)
		require.Equal(t, routerLocalConfigLoaded.NodeLocalConfig.GeneralConfig.TLSConfig.ClientAuthRequired, true)
		require.Equal(t, role, config.RouterStr)

		for j := 1; j <= len(networkLocalConfig.PartiesLocalConfig[i-1].BatchersLocalConfig); j++ {
			configPath = path.Join(dir, "config", fmt.Sprintf("party%d", i), fmt.Sprintf("local_config_batcher%d.yaml", j))
			batcherLocalConfigLoaded, role, err := config.LoadLocalConfig(configPath, logger)
			require.NoError(t, err)
			require.NotNil(t, batcherLocalConfigLoaded.NodeLocalConfig)
			require.NotNil(t, batcherLocalConfigLoaded.TLSConfig)
			require.NotNil(t, batcherLocalConfigLoaded.ClusterConfig)
			require.Equal(t, role, config.BatcherStr)
		}

		configPath = path.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_consenter.yaml")
		consenterLocalConfigLoaded, role, err := config.LoadLocalConfig(configPath, logger)
		require.NoError(t, err)
		require.NotNil(t, consenterLocalConfigLoaded.NodeLocalConfig)
		require.NotNil(t, consenterLocalConfigLoaded.TLSConfig)
		require.NotNil(t, consenterLocalConfigLoaded.ClusterConfig)
		require.Equal(t, role, config.ConsensusStr)

		configPath = path.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_assembler.yaml")
		assemblerLocalConfig := networkLocalConfig.PartiesLocalConfig[i-1].AssemblerLocalConfig
		assemblerLocalConfig.GeneralConfig.Cluster.ReplicationPolicy = ""
		err = utils.WriteToYAML(assemblerLocalConfig, configPath)
		require.NoError(t, err)

		assemblerLocalConfigLoaded, role, err := config.LoadLocalConfig(configPath, logger)
		require.NoError(t, err)
		require.NotNil(t, assemblerLocalConfigLoaded.NodeLocalConfig)
		require.NotNil(t, assemblerLocalConfigLoaded.TLSConfig)
		require.NotNil(t, assemblerLocalConfigLoaded.ClusterConfig)
		require.Equal(t, assemblerLocalConfigLoaded.NodeLocalConfig.GeneralConfig.TLSConfig.Enabled, true)
		require.Equal(t, assemblerLocalConfigLoaded.NodeLocalConfig.GeneralConfig.TLSConfig.ClientAuthRequired, true)
		require.Equal(t, role, config.AssemblerStr)
		require.Equal(t, config.ReplicationPolicyAssembler, assemblerLocalConfigLoaded.ClusterConfig.ReplicationPolicy)
		require.Equal(t, "", consenterLocalConfigLoaded.ClusterConfig.ReplicationPolicy)
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

func TestLoadLocalConfigYaml_MultipleOrMissingRoles(t *testing.T) {
	dir := t.TempDir()
	require.DirExists(t, dir)

	// Create local config files
	networkConfig := testutil.GenerateNetworkConfig(t, "mTLS", "mTLS")
	_, err := armageddon.GenerateCryptoConfigWithProfile(&networkConfig, dir)
	require.NoError(t, err)

	networkLocalConfig, err := generate.CreateArmaLocalConfig(networkConfig, dir, dir, false)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)

	// Override consenter params - add router params
	consenterLocalConfig := networkLocalConfig.PartiesLocalConfig[0].ConsenterLocalConfig
	consenterLocalConfig.RouterParams = &config.RouterParams{
		NumberOfConnectionsPerBatcher: 5,
		NumberOfStreamsPerConnection:  5,
	}

	configPath := path.Join(dir, "config", "party1", "local_config_consenter.yaml")
	err = utils.WriteToYAML(consenterLocalConfig, configPath)
	require.NoError(t, err)

	logger := testutil.CreateLoggerForModule(t, "LoadLocalConfig", zap.DebugLevel)
	consenterLocalConfigLoaded, role, err := config.LoadLocalConfig(configPath, logger)
	require.Error(t, err)
	require.Contains(t, err.Error(), "node local config is not valid, multiple params were set: [Router Consensus]")
	require.Equal(t, role, "")
	require.Nil(t, consenterLocalConfigLoaded)

	// Override consenter params - clean router params and add empty consensus params
	networkLocalConfig.PartiesLocalConfig[0].ConsenterLocalConfig.ConsensusParams = &config.ConsensusParams{}
	networkLocalConfig.PartiesLocalConfig[0].ConsenterLocalConfig.RouterParams = nil

	err = utils.WriteToYAML(consenterLocalConfig, configPath)
	require.NoError(t, err)

	consenterLocalConfigLoaded, role, err = config.LoadLocalConfig(configPath, logger)

	require.NoError(t, err)
	require.Equal(t, role, config.ConsensusStr)
	require.NotNil(t, consenterLocalConfigLoaded)
	require.Equal(t, config.DefaultConsenterNodeConfigParams(consenterLocalConfig.FileStore.Path).WALDir, consenterLocalConfigLoaded.NodeLocalConfig.ConsensusParams.WALDir)

	// Override consenter params - nil params
	networkLocalConfig.PartiesLocalConfig[0].ConsenterLocalConfig.ConsensusParams = nil
	networkLocalConfig.PartiesLocalConfig[0].ConsenterLocalConfig.RouterParams = nil

	err = utils.WriteToYAML(consenterLocalConfig, configPath)
	require.NoError(t, err)

	consenterLocalConfigLoaded, role, err = config.LoadLocalConfig(configPath, logger)

	require.Error(t, err)
	require.Contains(t, err.Error(), "node local config is not valid, node params are missing")
	require.Equal(t, role, "")
	require.Nil(t, consenterLocalConfigLoaded)
}

func TestLoadLocalConfigAppliesGeneralDefaults(t *testing.T) {
	dir := t.TempDir()
	require.DirExists(t, dir)

	networkConfig := testutil.GenerateNetworkConfig(t, "mTLS", "mTLS")
	_, err := armageddon.GenerateCryptoConfigWithProfile(&networkConfig, dir)
	require.NoError(t, err)

	networkLocalConfig, err := generate.CreateArmaLocalConfig(networkConfig, dir, dir, false)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)

	localConfig := networkLocalConfig.PartiesLocalConfig[0].ConsenterLocalConfig
	localConfig.GeneralConfig.LogSpec = ""
	localConfig.GeneralConfig.Cluster.SendBufferSize = 0
	localConfig.GeneralConfig.Cluster.ReplicationPolicy = ""
	localConfig.GeneralConfig.Cluster.ClientCertificate = ""
	localConfig.GeneralConfig.Cluster.ClientPrivateKey = ""

	configPath := path.Join(dir, "config", "party1", "local_config_consenter.yaml")
	err = utils.WriteToYAML(localConfig, configPath)
	require.NoError(t, err)

	logger := testutil.CreateLoggerForModule(t, "LoadLocalConfig", zap.DebugLevel)
	loadedConfig, role, err := config.LoadLocalConfig(configPath, logger)
	require.NoError(t, err)
	require.Equal(t, role, config.ConsensusStr)

	require.Equal(t, config.DefaultNodeLocalConfig.GeneralConfig.LogSpec, loadedConfig.NodeLocalConfig.GeneralConfig.LogSpec)
	require.Equal(t, config.DefaultNodeLocalConfig.GeneralConfig.Cluster.SendBufferSize, loadedConfig.ClusterConfig.SendBufferSize)
	require.Equal(t, config.DefaultNodeLocalConfig.GeneralConfig.Cluster.ReplicationPolicy, loadedConfig.ClusterConfig.ReplicationPolicy)
	require.Equal(t, config.DefaultNodeLocalConfig.GeneralConfig.Cluster.RPCTimeout, loadedConfig.ClusterConfig.RPCTimeout)
	require.Equal(t, config.DefaultNodeLocalConfig.GeneralConfig.Cluster.ReplicationBufferSize, loadedConfig.ClusterConfig.ReplicationBufferSize)
	require.Equal(t, config.DefaultNodeLocalConfig.GeneralConfig.Cluster.ReplicationMaxRetryInterval, loadedConfig.ClusterConfig.ReplicationMaxRetryInterval)
	require.Equal(t, config.DefaultNodeLocalConfig.GeneralConfig.Cluster.ReplicationMinRetryInterval, loadedConfig.ClusterConfig.ReplicationMinRetryInterval)
	require.Equal(t, config.DefaultNodeLocalConfig.GeneralConfig.Cluster.ReplicationMaxRetryDuration, loadedConfig.ClusterConfig.ReplicationMaxRetryDuration)
	require.Equal(t, config.DefaultNodeLocalConfig.GeneralConfig.Cluster.CertExpirationWarningThreshold, loadedConfig.ClusterConfig.CertExpirationWarningThreshold)
	require.Equal(t, loadedConfig.TLSConfig.Certificate, loadedConfig.ClusterConfig.ClientCertificate)
	require.Equal(t, loadedConfig.TLSConfig.PrivateKey, loadedConfig.ClusterConfig.ClientPrivateKey)
}

func TestLoadLocalConfigRejectsNegativeValues(t *testing.T) {
	tests := []struct {
		name         string
		createConfig func() *config.NodeLocalConfig
		update       func(*config.NodeLocalConfig)
		expectedErr  string
	}{
		{
			name:         "negative send buffer size",
			createConfig: testutil.CreateTestConsensusLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.GeneralConfig.Cluster.SendBufferSize = -1
			},
			expectedErr: "node local config is not valid, General.Cluster.SendBufferSize must not be negative",
		},
		{
			name:         "negative RPC timeout",
			createConfig: testutil.CreateTestConsensusLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.GeneralConfig.Cluster.RPCTimeout = -time.Millisecond
			},
			expectedErr: "node local config is not valid, General.Cluster.RPCTimeout must not be negative",
		},
		{
			name:         "negative replication buffer size",
			createConfig: testutil.CreateTestConsensusLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.GeneralConfig.Cluster.ReplicationBufferSize = -1
			},
			expectedErr: "node local config is not valid, General.Cluster.ReplicationBufferSize must not be negative",
		},
		{
			name:         "negative replication min retry interval",
			createConfig: testutil.CreateTestConsensusLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.GeneralConfig.Cluster.ReplicationMinRetryInterval = -time.Millisecond
			},
			expectedErr: "node local config is not valid, General.Cluster.ReplicationMinRetryInterval must not be negative",
		},
		{
			name:         "negative replication max retry interval",
			createConfig: testutil.CreateTestConsensusLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.GeneralConfig.Cluster.ReplicationMaxRetryInterval = -time.Millisecond
			},
			expectedErr: "node local config is not valid, General.Cluster.ReplicationMaxRetryInterval must not be negative",
		},
		{
			name:         "negative replication max retry duration",
			createConfig: testutil.CreateTestConsensusLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.GeneralConfig.Cluster.ReplicationMaxRetryDuration = -time.Millisecond
			},
			expectedErr: "node local config is not valid, General.Cluster.ReplicationMaxRetryDuration must not be negative",
		},
		{
			name:         "negative certificate expiration warning threshold",
			createConfig: testutil.CreateTestConsensusLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.GeneralConfig.Cluster.CertExpirationWarningThreshold = -time.Millisecond
			},
			expectedErr: "node local config is not valid, General.Cluster.CertExpirationWarningThreshold must not be negative",
		},
		{
			name:         "negative TLS handshake time shift",
			createConfig: testutil.CreateTestConsensusLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.GeneralConfig.Cluster.TLSHandshakeTimeShift = -time.Millisecond
			},
			expectedErr: "node local config is not valid, General.Cluster.TLSHandshakeTimeShift must not be negative",
		},
		{
			name:         "negative metrics log interval",
			createConfig: testutil.CreateTestConsensusLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.MetricsConfig = &config.Metrics{
					MetricsLogInterval: -time.Millisecond,
				}
			},
			expectedErr: "node local config is not valid, Metrics.MetricsLogInterval must not be negative",
		},
		{
			name:         "negative router connections per batcher",
			createConfig: testutil.CreateTestRouterLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.RouterParams.NumberOfConnectionsPerBatcher = -1
			},
			expectedErr: "node local config is not valid, Router.NumberOfConnectionsPerBatcher must not be negative",
		},
		{
			name:         "negative router streams per connection",
			createConfig: testutil.CreateTestRouterLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.RouterParams.NumberOfStreamsPerConnection = -1
			},
			expectedErr: "node local config is not valid, Router.NumberOfStreamsPerConnection must not be negative",
		},
		{
			name:         "negative batcher submit timeout",
			createConfig: testutil.CreateTestBatcherLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.BatcherParams.SubmitTimeout = -time.Millisecond
			},
			expectedErr: "node local config is not valid, Batcher.SubmitTimeout must not be negative",
		},
		{
			name:         "negative assembler prefetch buffer size",
			createConfig: testutil.CreateTestAssemblerLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.AssemblerParams.PrefetchBufferMemoryBytes = -1
			},
			expectedErr: "node local config is not valid, Assembler.PrefetchBufferMemoryBytes must not be negative",
		},
		{
			name:         "negative assembler restart ledger scan timeout",
			createConfig: testutil.CreateTestAssemblerLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.AssemblerParams.RestartLedgerScanTimeout = -time.Millisecond
			},
			expectedErr: "node local config is not valid, Assembler.RestartLedgerScanTimeout must not be negative",
		},
		{
			name:         "negative assembler prefetch eviction TTL",
			createConfig: testutil.CreateTestAssemblerLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.AssemblerParams.PrefetchEvictionTtl = -time.Millisecond
			},
			expectedErr: "node local config is not valid, Assembler.PrefetchEvictionTtl must not be negative",
		},
		{
			name:         "negative assembler pop wait monitor timeout",
			createConfig: testutil.CreateTestAssemblerLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.AssemblerParams.PopWaitMonitorTimeout = -time.Millisecond
			},
			expectedErr: "node local config is not valid, Assembler.PopWaitMonitorTimeout must not be negative",
		},
		{
			name:         "negative assembler replication channel size",
			createConfig: testutil.CreateTestAssemblerLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.AssemblerParams.ReplicationChannelSize = -1
			},
			expectedErr: "node local config is not valid, Assembler.ReplicationChannelSize must not be negative",
		},
		{
			name:         "negative assembler batch requests channel size",
			createConfig: testutil.CreateTestAssemblerLocalConfig,
			update: func(c *config.NodeLocalConfig) {
				c.AssemblerParams.BatchRequestsChannelSize = -1
			},
			expectedErr: "node local config is not valid, Assembler.BatchRequestsChannelSize must not be negative",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			require.DirExists(t, dir)

			localConfig := test.createConfig()
			localConfig.GeneralConfig.LocalMSPDir = "path/to/msp"
			localConfig.GeneralConfig.LocalMSPID = "org1"

			test.update(localConfig)

			configPath := path.Join(dir, "local_config.yaml")
			err := utils.WriteToYAML(localConfig, configPath)
			require.NoError(t, err)

			logger := testutil.CreateLoggerForModule(t, "LoadLocalConfig", zap.DebugLevel)
			loadedConfig, role, err := config.LoadLocalConfig(configPath, logger)

			require.Error(t, err)
			require.Contains(t, err.Error(), test.expectedErr)
			require.Equal(t, "", role)
			require.Nil(t, loadedConfig)
		})
	}
}

func TestLoadLocalConfigRejectsInvertedRetryIntervals(t *testing.T) {
	dir := t.TempDir()
	require.DirExists(t, dir)

	localConfig := testutil.CreateTestConsensusLocalConfig()
	localConfig.GeneralConfig.LocalMSPDir = "path/to/msp"
	localConfig.GeneralConfig.LocalMSPID = "org1"
	localConfig.GeneralConfig.Cluster.ReplicationMinRetryInterval = 10 * time.Second
	localConfig.GeneralConfig.Cluster.ReplicationMaxRetryInterval = time.Second

	configPath := path.Join(dir, "local_config_consenter.yaml")
	err := utils.WriteToYAML(localConfig, configPath)
	require.NoError(t, err)

	logger := testutil.CreateLoggerForModule(t, "LoadLocalConfig", zap.DebugLevel)
	loadedConfig, role, err := config.LoadLocalConfig(configPath, logger)

	require.Error(t, err)
	require.Contains(t, err.Error(), "node local config is not valid, General.Cluster.ReplicationMinRetryInterval must be less than or equal to General.Cluster.ReplicationMaxRetryInterval")
	require.Equal(t, "", role)
	require.Nil(t, loadedConfig)
}
