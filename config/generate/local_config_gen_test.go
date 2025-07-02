/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package generate_test

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/utils"

	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	"github.com/stretchr/testify/require"
)

func TestRouterLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "router-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	routerLocalConfig := testutil.CreateTestRouterLocalConfig()

	path := path.Join(dir, "local_router_config.yaml")
	err = utils.WriteToYAML(routerLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = utils.ReadFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *routerLocalConfig)
}

func TestBatcherLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "batcher-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	batcherLocalConfig := testutil.CreateTestBatcherLocalConfig()
	path := path.Join(dir, "local_batcher_config.yaml")
	err = utils.WriteToYAML(batcherLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = utils.ReadFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *batcherLocalConfig)
}

func TestConsensusLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "consensus-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	consensusLocalConfig := testutil.CreateTestConsensusLocalConfig()

	path := path.Join(dir, "local_consensus_config.yaml")
	err = utils.WriteToYAML(consensusLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = utils.ReadFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *consensusLocalConfig)
}

func TestAssemblerLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "assembler-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	assemblerLocalConfig := testutil.CreateTestAssemblerLocalConfig()

	path := path.Join(dir, "local_assembler_config.yaml")
	err = utils.WriteToYAML(assemblerLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = utils.ReadFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *assemblerLocalConfig)
}

func TestARMALocalConfigGeneration(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	networkConfig := testutil.GenerateNetworkConfig(t, "none", "none")

	networkLocalConfig, err := generate.CreateArmaLocalConfig(networkConfig, dir, dir)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)

	configDir := filepath.Join(dir, "/config")
	res, err := loadArmaLocalConfig(configDir)
	require.NoError(t, err)
	require.NotNil(t, res)
	for i, party := range res.PartiesLocalConfig {
		require.Equal(t, networkLocalConfig.PartiesLocalConfig[i].RouterLocalConfig, party.RouterLocalConfig)
		for j, batcher := range party.BatchersLocalConfig {
			require.Equal(t, batcher, party.BatchersLocalConfig[j])
		}
		require.Equal(t, networkLocalConfig.PartiesLocalConfig[i].ConsenterLocalConfig, party.ConsenterLocalConfig)
		require.Equal(t, networkLocalConfig.PartiesLocalConfig[i].AssemblerLocalConfig, party.AssemblerLocalConfig)
	}
}

// loadArmaLocalConfig loads the local configuration of all nodes for all parties.
// this function is used for testing that a local config was generated successfully.
func loadArmaLocalConfig(path string) (*generate.NetworkLocalConfig, error) {
	if path == "" {
		return nil, fmt.Errorf("load arma config failed, path is empty")
	}

	partiesDirs, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read arma config failed, err: %s", err)
	}

	var partiesLocalConfig []generate.PartyLocalConfig

	for _, party := range partiesDirs {
		partyPath := filepath.Join(path, party.Name())

		nodesYamls, err := os.ReadDir(partyPath)
		if err != nil {
			return nil, fmt.Errorf("read arma config failed, err: %s", err)
		}

		numOfShards := len(nodesYamls) - 3

		routerConfigPath := filepath.Join(partyPath, "local_config_router.yaml")
		routerLocalConfig, err := config.LoadLocalConfigYaml(routerConfigPath)
		if err != nil {
			return nil, err
		}

		var batchersLocalConfig []*config.NodeLocalConfig
		for j := 1; j <= numOfShards; j++ {
			batcherConfigPath := filepath.Join(partyPath, fmt.Sprintf("local_config_batcher%d.yaml", j))
			batcherLocalConfig, err := config.LoadLocalConfigYaml(batcherConfigPath)
			if err != nil {
				return nil, err
			}
			batchersLocalConfig = append(batchersLocalConfig, batcherLocalConfig)
		}

		consenterConfigPath := filepath.Join(partyPath, "local_config_consenter.yaml")
		consenterLocalConfig, err := config.LoadLocalConfigYaml(consenterConfigPath)
		if err != nil {
			return nil, err
		}

		assemblerConfigPath := filepath.Join(partyPath, "local_config_assembler.yaml")
		assemblerLocalConfig, err := config.LoadLocalConfigYaml(assemblerConfigPath)
		if err != nil {
			return nil, err
		}

		partyLocalConfig := generate.PartyLocalConfig{
			RouterLocalConfig:    routerLocalConfig,
			BatchersLocalConfig:  batchersLocalConfig,
			ConsenterLocalConfig: consenterLocalConfig,
			AssemblerLocalConfig: assemblerLocalConfig,
		}

		partiesLocalConfig = append(partiesLocalConfig, partyLocalConfig)
	}
	return &generate.NetworkLocalConfig{PartiesLocalConfig: partiesLocalConfig}, nil
}
