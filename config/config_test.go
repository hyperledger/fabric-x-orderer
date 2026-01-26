/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/msputils/mock"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestExtractAppTrustedRootsFromConfigBlock(t *testing.T) {
	t.Run("no application config", func(t *testing.T) {
		bundle := &mocks.FakeConfigResources{}
		bundle.ApplicationConfigReturns(nil, false)
		mockMSPManager := &mock.MSPManager{}
		fakeMsp := &mock.MSP{}
		mockMSPManager.GetMSPsReturns(
			map[string]msp.MSP{
				"test-member-role": fakeMsp,
			},
			nil,
		)
		bundle.MSPManagerReturns(mockMSPManager)
		res := config.ExtractAppTrustedRootsFromConfigBlock(bundle)
		require.Equal(t, res, [][]byte{})
	})

	t.Run("real envelope", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config.yaml")
		_ = testutil.CreateNetwork(t, configPath, 4, 2, "mTLS", "mTLS")
		armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

		genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
		data, err := os.ReadFile(genesisBlockPath)
		require.NoError(t, err)
		genesisBlock, err := protoutil.UnmarshalBlock(data)
		require.NoError(t, err)

		env, err := protoutil.ExtractEnvelope(genesisBlock, 0)
		require.NoError(t, err)
		bundle, err := channelconfig.NewBundleFromEnvelope(env, factory.GetDefault())
		require.NoError(t, err)

		res := config.ExtractAppTrustedRootsFromConfigBlock(bundle)
		require.Equal(t, len(res), 4)
	})
}

func TestConfigurationCheckIfRouterNodeExistsInSharedConfig(t *testing.T) {
	dir := t.TempDir()
	numOfParties := 4
	numOfShards := 2
	configPath := filepath.Join(dir, "config.yaml")
	_ = testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel)

	// choose local config for party1
	localConfigPathRouter := filepath.Join(dir, "config", "party1", "local_config_router.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathRouter, filepath.Join(dir, "storage"))

	fullConfig, genesisBlock, err := config.ReadConfig(localConfigPathRouter, testLogger)
	require.NoError(t, err)
	require.NotNil(t, genesisBlock)

	// router party1 exists in shared config, should succeed
	err = fullConfig.CheckIfRouterNodeExistsInSharedConfig()
	require.NoError(t, err)

	// change router1 cert
	fullConfig.SharedConfig.PartiesConfig[0].RouterConfig.TlsCert = []byte("FakeCert")
	err = fullConfig.CheckIfRouterNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "TLS certificate mismatch")

	// remove router config from party1
	fullConfig.SharedConfig.PartiesConfig[0].RouterConfig = nil
	err = fullConfig.CheckIfRouterNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "router with partyID 1 is not present in the shared configuration's party1 list")

	// remove router1 from shared config, expect for error
	fullConfig.SharedConfig.PartiesConfig = fullConfig.SharedConfig.PartiesConfig[1:3]
	err = fullConfig.CheckIfRouterNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "partyID 1 is not present in the shared configuration's party list")
}

func TestConfigurationCheckIfBatcherNodeExistsInSharedConfig(t *testing.T) {
	dir := t.TempDir()
	numOfParties := 4
	numOfShards := 2
	configPath := filepath.Join(dir, "config.yaml")
	_ = testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "ReadConfigBatcher", zap.DebugLevel)

	// choose local config for party1 shard1
	localConfigPathBacther := filepath.Join(dir, "config", "party1", "local_config_batcher1.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathBacther, filepath.Join(dir, "storage"))

	fullConfig, genesisBlock, err := config.ReadConfig(localConfigPathBacther, testLogger)
	require.NoError(t, err)
	require.NotNil(t, genesisBlock)

	// batcher11 exists in shared config, should succeed
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig()
	require.NoError(t, err)

	// change batcher11 sign cert
	fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig[0].SignCert = []byte("FakeCert")
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "sign certificate mismatch")

	// change batcher11 TLS cert
	fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig[0].TlsCert = []byte("FakeCert")
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "TLS certificate mismatch")

	// remove shard1 from shared config, expect for error
	fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig = fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig[1:]
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "batcher in shard1 does not exist for party1 in the shared config")

	// remove batchers config from party1
	fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig = nil
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "batcher in shard1 does not exist for party1 in the shared config")

	// remove  batcher11 from shared config, expect for error
	fullConfig.SharedConfig.PartiesConfig = fullConfig.SharedConfig.PartiesConfig[1:3]
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "partyID 1 is not present in the shared configuration's party list")
}

func TestConfigurationCheckIfConsenterNodeExistsInSharedConfig(t *testing.T) {
	dir := t.TempDir()
	numOfParties := 4
	numOfShards := 2
	configPath := filepath.Join(dir, "config.yaml")
	_ = testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "ReadConfigConsenter", zap.DebugLevel)

	// choose local config for party1
	localConfigPathConsenter := filepath.Join(dir, "config", "party1", "local_config_consenter.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathConsenter, filepath.Join(dir, "storage"))

	fullConfig, genesisBlock, err := config.ReadConfig(localConfigPathConsenter, testLogger)
	require.NoError(t, err)
	require.NotNil(t, genesisBlock)

	// consenter party1 exists in shared config, should succeed
	err = fullConfig.CheckIfConsenterNodeExistsInSharedConfig()
	require.NoError(t, err)

	// change consenter1 cert
	fullConfig.SharedConfig.PartiesConfig[0].ConsenterConfig.TlsCert = []byte("FakeCert")
	err = fullConfig.CheckIfConsenterNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "TLS certificate mismatch")

	// change consenter1 sign cert
	fullConfig.SharedConfig.PartiesConfig[0].ConsenterConfig.SignCert = []byte("FakeCert")
	err = fullConfig.CheckIfConsenterNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "sign certificate mismatch")

	// remove consenter config from party1
	fullConfig.SharedConfig.PartiesConfig[0].ConsenterConfig = nil
	err = fullConfig.CheckIfConsenterNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "consenter with partyID 1 is not present in the shared configuration's party1 list")

	// remove consenter1 from shared config, expect for error
	fullConfig.SharedConfig.PartiesConfig = fullConfig.SharedConfig.PartiesConfig[1:3]
	err = fullConfig.CheckIfConsenterNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "partyID 1 is not present in the shared configuration's party list")
}

func TestConfigurationCheckIfAssemblerNodeExistsInSharedConfig(t *testing.T) {
	dir := t.TempDir()
	numOfParties := 4
	numOfShards := 2
	configPath := filepath.Join(dir, "config.yaml")
	_ = testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "ReadConfigAssembler", zap.DebugLevel)

	// choose local config for party1
	localConfigPathAssembler := filepath.Join(dir, "config", "party1", "local_config_assembler.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathAssembler, filepath.Join(dir, "storage"))

	fullConfig, genesisBlock, err := config.ReadConfig(localConfigPathAssembler, testLogger)
	require.NoError(t, err)
	require.NotNil(t, genesisBlock)

	// assembler party1 exists in shared config, should succeed
	err = fullConfig.CheckIfAssemblerNodeExistsInSharedConfig()
	require.NoError(t, err)

	// change assembler1 cert
	fullConfig.SharedConfig.PartiesConfig[0].AssemblerConfig.TlsCert = []byte("FakeCert")
	err = fullConfig.CheckIfAssemblerNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "TLS certificate mismatch")

	// remove assembler config from party1
	fullConfig.SharedConfig.PartiesConfig[0].AssemblerConfig = nil
	err = fullConfig.CheckIfAssemblerNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "assembler with partyID 1 is not present in the shared configuration's party1 list")

	// remove assembler1 from shared config, expect for error
	fullConfig.SharedConfig.PartiesConfig = fullConfig.SharedConfig.PartiesConfig[1:3]
	err = fullConfig.CheckIfAssemblerNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "partyID 1 is not present in the shared configuration's party list")
}
