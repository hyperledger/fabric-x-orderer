/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	protosorderer "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"google.golang.org/protobuf/proto"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	"github.com/hyperledger/fabric-x-orderer/common/msputils/mock"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
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
		netInfo := testutil.CreateNetwork(t, configPath, 4, 2, "mTLS", "mTLS")
		defer netInfo.CleanUp()
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
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel)

	// choose local config for party1
	localConfigPathRouter := filepath.Join(dir, "config", "party1", "local_config_router.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathRouter, filepath.Join(dir, "storage"), "", 0)

	fullConfig, genesisBlock, err := config.ReadConfig(localConfigPathRouter, testLogger)
	require.NoError(t, err)
	require.NotNil(t, genesisBlock)

	// router party1 exists in shared config, should succeed
	err = fullConfig.CheckIfRouterNodeExistsInSharedConfig()
	require.NoError(t, err)

	// change router1 cert
	caCert, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "ca", "ca-cert.pem"))
	require.NoError(t, err)
	caPrivateKey, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "ca", "priv_sk"))
	require.NoError(t, err)
	fakeTLSCert, err := ChangeExpirationTimeOfCert(t, fullConfig.SharedConfig.PartiesConfig[0].RouterConfig.TlsCert, caCert, caPrivateKey)
	require.NoError(t, err)
	fullConfig.SharedConfig.PartiesConfig[0].RouterConfig.TlsCert = fakeTLSCert
	err = fullConfig.CheckIfRouterNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "certificate mismatch")

	// remove router config from party1
	fullConfig.SharedConfig.PartiesConfig[0].RouterConfig = nil
	err = fullConfig.CheckIfRouterNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "router configuration of partyID 1 is missing from the shared configuration")

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
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "ReadConfigBatcher", zap.DebugLevel)

	// choose local config for party1 shard1
	localConfigPathBacther := filepath.Join(dir, "config", "party1", "local_config_batcher1.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathBacther, filepath.Join(dir, "storage"), "", 0)

	fullConfig, genesisBlock, err := config.ReadConfig(localConfigPathBacther, testLogger)
	require.NoError(t, err)
	require.NotNil(t, genesisBlock)

	localSignCert, err := os.ReadFile(filepath.Join(fullConfig.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "signcerts", "sign-cert.pem"))
	require.NoError(t, err)
	require.NotNil(t, localSignCert)

	// batcher11 exists in shared config, should succeed
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig(localSignCert)
	require.NoError(t, err)

	// change batcher11 sign cert
	caCert, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "ca", "ca-cert.pem"))
	require.NoError(t, err)
	caPrivateKey, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "ca", "priv_sk"))
	require.NoError(t, err)
	fakeSignCert, err := ChangeExpirationTimeOfCert(t, fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig[0].SignCert, caCert, caPrivateKey)
	require.NoError(t, err)
	fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig[0].SignCert = fakeSignCert
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig(localSignCert)
	require.Error(t, err)
	require.ErrorContains(t, err, "sign certificate mismatch")

	// change batcher11 TLS cert
	fakeTLSCert, err := ChangeExpirationTimeOfCert(t, fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig[0].TlsCert, caCert, caPrivateKey)
	require.NoError(t, err)
	fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig[0].TlsCert = fakeTLSCert
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig(localSignCert)
	require.Error(t, err)
	require.ErrorContains(t, err, "certificate mismatch")

	// remove shard1 from shared config, expect for error
	fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig = fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig[1:]
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig(localSignCert)
	require.Error(t, err)
	require.ErrorContains(t, err, "batcher in shard1 does not exist for party1 in the shared config")

	// remove batchers config from party1
	fullConfig.SharedConfig.PartiesConfig[0].BatchersConfig = nil
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig(localSignCert)
	require.Error(t, err)
	require.ErrorContains(t, err, "batcher in shard1 does not exist for party1 in the shared config")

	// remove  batcher11 from shared config, expect for error
	fullConfig.SharedConfig.PartiesConfig = fullConfig.SharedConfig.PartiesConfig[1:3]
	err = fullConfig.CheckIfBatcherNodeExistsInSharedConfig(localSignCert)
	require.Error(t, err)
	require.ErrorContains(t, err, "partyID 1 is not present in the shared configuration's party list")
}

func TestConfigurationCheckIfConsenterNodeExistsInSharedConfig(t *testing.T) {
	dir := t.TempDir()
	numOfParties := 4
	numOfShards := 2
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "ReadConfigConsenter", zap.DebugLevel)

	// choose local config for party1
	localConfigPathConsenter := filepath.Join(dir, "config", "party1", "local_config_consenter.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathConsenter, filepath.Join(dir, "storage"), "", 0)

	fullConfig, genesisBlock, err := config.ReadConfig(localConfigPathConsenter, testLogger)
	require.NoError(t, err)
	require.NotNil(t, genesisBlock)

	localSignCert, err := os.ReadFile(filepath.Join(fullConfig.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "signcerts", "sign-cert.pem"))
	require.NoError(t, err)
	require.NotNil(t, localSignCert)

	// consenter party1 exists in shared config, should succeed
	err = fullConfig.CheckIfConsenterNodeExistsInSharedConfig(localSignCert)
	require.NoError(t, err)

	// change consenter1 tls cert
	caCert, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "ca", "ca-cert.pem"))
	require.NoError(t, err)
	caPrivateKey, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "ca", "priv_sk"))
	require.NoError(t, err)
	fakeTLSCert, err := ChangeExpirationTimeOfCert(t, fullConfig.SharedConfig.PartiesConfig[0].ConsenterConfig.TlsCert, caCert, caPrivateKey)
	require.NoError(t, err)
	fullConfig.SharedConfig.PartiesConfig[0].ConsenterConfig.TlsCert = fakeTLSCert
	err = fullConfig.CheckIfConsenterNodeExistsInSharedConfig(localSignCert)
	require.Error(t, err)
	require.ErrorContains(t, err, "certificate mismatch")

	// change consenter1 sign cert
	fakeSignCert, err := ChangeExpirationTimeOfCert(t, fullConfig.SharedConfig.PartiesConfig[0].ConsenterConfig.SignCert, caCert, caPrivateKey)
	require.NoError(t, err)
	fullConfig.SharedConfig.PartiesConfig[0].ConsenterConfig.SignCert = fakeSignCert
	err = fullConfig.CheckIfConsenterNodeExistsInSharedConfig(localSignCert)
	require.Error(t, err)
	require.ErrorContains(t, err, "sign certificate mismatch")

	// remove consenter config from party1
	fullConfig.SharedConfig.PartiesConfig[0].ConsenterConfig = nil
	err = fullConfig.CheckIfConsenterNodeExistsInSharedConfig(localSignCert)
	require.Error(t, err)
	require.ErrorContains(t, err, "consenter configuration of partyID 1 is missing from the shared configuration")

	// remove consenter1 from shared config, expect for error
	fullConfig.SharedConfig.PartiesConfig = fullConfig.SharedConfig.PartiesConfig[1:3]
	err = fullConfig.CheckIfConsenterNodeExistsInSharedConfig(localSignCert)
	require.Error(t, err)
	require.ErrorContains(t, err, "partyID 1 is not present in the shared configuration's party list")
}

func TestConfigurationCheckIfAssemblerNodeExistsInSharedConfig(t *testing.T) {
	dir := t.TempDir()
	numOfParties := 4
	numOfShards := 2
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "ReadConfigAssembler", zap.DebugLevel)

	// choose local config for party1
	localConfigPathAssembler := filepath.Join(dir, "config", "party1", "local_config_assembler.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathAssembler, filepath.Join(dir, "storage"), "", 0)

	fullConfig, genesisBlock, err := config.ReadConfig(localConfigPathAssembler, testLogger)
	require.NoError(t, err)
	require.NotNil(t, genesisBlock)

	// assembler party1 exists in shared config, should succeed
	err = fullConfig.CheckIfAssemblerNodeExistsInSharedConfig()
	require.NoError(t, err)

	// change assembler1 cert
	caCert, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "ca", "ca-cert.pem"))
	require.NoError(t, err)
	caPrivateKey, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "ca", "priv_sk"))
	require.NoError(t, err)
	fakeTLSCert, err := ChangeExpirationTimeOfCert(t, fullConfig.SharedConfig.PartiesConfig[0].AssemblerConfig.TlsCert, caCert, caPrivateKey)
	require.NoError(t, err)
	fullConfig.SharedConfig.PartiesConfig[0].AssemblerConfig.TlsCert = fakeTLSCert
	err = fullConfig.CheckIfAssemblerNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "certificate mismatch")

	// remove assembler config from party1
	fullConfig.SharedConfig.PartiesConfig[0].AssemblerConfig = nil
	err = fullConfig.CheckIfAssemblerNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "assembler configuration of partyID 1 is missing from the shared configuration")

	// remove assembler1 from shared config, expect for error
	fullConfig.SharedConfig.PartiesConfig = fullConfig.SharedConfig.PartiesConfig[1:3]
	err = fullConfig.CheckIfAssemblerNodeExistsInSharedConfig()
	require.Error(t, err)
	require.ErrorContains(t, err, "partyID 1 is not present in the shared configuration's party list")
}

func TestConfigurationNewUpdatedConfigurationFromBlock(t *testing.T) {
	dir := t.TempDir()
	numOfParties := 4
	numOfShards := 2
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "UpdateConfigAssembler", zap.DebugLevel)

	// read config of assembler node from party1
	localConfigPathAssembler := filepath.Join(dir, "config", "party1", "local_config_assembler.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathAssembler, filepath.Join(dir, "storage"), "", 0)

	fullConfig, genesisBlock, err := config.ReadConfig(localConfigPathAssembler, testLogger)
	require.NoError(t, err)
	require.NotNil(t, genesisBlock)

	// change the genesis block to have a new port to the assembler of party1
	newPort := fullConfig.SharedConfig.PartiesConfig[0].AssemblerConfig.Port + 1
	envelope, err := protoutil.GetEnvelopeFromBlock(genesisBlock.Data.Data[0])
	require.NoError(t, err)
	require.NotNil(t, envelope)
	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
	require.NoError(t, err)
	require.NotNil(t, payload)
	configEnv := &common.ConfigEnvelope{}
	err = proto.Unmarshal(payload.Data, configEnv)
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	consensusTypeConfigValue := configEnv.Config.ChannelGroup.Groups["Orderer"].Values["ConsensusType"]
	consensusTypeValue := &protosorderer.ConsensusType{}
	err = proto.Unmarshal(consensusTypeConfigValue.Value, consensusTypeValue)
	require.NoError(t, err)
	require.NotNil(t, consensusTypeValue)
	sharedConfig := &ordererpb.SharedConfig{}
	err = proto.Unmarshal(consensusTypeValue.Metadata, sharedConfig)
	require.NoError(t, err)
	sharedConfig.PartiesConfig[0].AssemblerConfig.Port = newPort
	consensusTypeValue.Metadata, err = proto.Marshal(sharedConfig)
	require.NoError(t, err)
	configEnv.Config.ChannelGroup.Groups["Orderer"].Values["ConsensusType"] = &common.ConfigValue{
		Value: protoutil.MarshalOrPanic(consensusTypeValue),
	}

	genesisBlock.Data.Data[0] = protoutil.MarshalOrPanic(&common.Envelope{
		Payload: protoutil.MarshalOrPanic(&common.Payload{
			Data:   protoutil.MarshalOrPanic(configEnv),
			Header: payload.Header,
		}),
	})

	newConfig, err := fullConfig.NewUpdatedConfigurationFromBlock(genesisBlock)
	require.NoError(t, err)
	require.NotNil(t, newConfig)

	// verify that local config is kept and shared config is changed
	require.Equal(t, newPort, newConfig.SharedConfig.PartiesConfig[0].AssemblerConfig.Port)
	require.Equal(t, newConfig.LocalConfig, fullConfig.LocalConfig)
}

func TestExtractAssemblers(t *testing.T) {
	dir := t.TempDir()
	numOfParties := 4
	numOfShards := 2
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "TestExtractAssemblers", zap.DebugLevel)

	// choose local config for party1
	localConfigPathAssembler := filepath.Join(dir, "config", "party1", "local_config_assembler.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathAssembler, filepath.Join(dir, "storage"), "", 0)

	fullConfig, genesisBlock, err := config.ReadConfig(localConfigPathAssembler, testLogger)
	require.NoError(t, err)
	require.NotNil(t, genesisBlock)

	// Extract assemblers from the config
	assemblers := fullConfig.ExtractAssemblers()

	// Verify the number of assemblers matches the number of parties
	require.Equal(t, len(assemblers), numOfParties)

	// Verify each assembler matches the shared config
	for idx, assembler := range assemblers {
		sharedPartyConfig := fullConfig.SharedConfig.PartiesConfig[idx]

		// Check PartyID matches
		require.Equal(t, assembler.PartyID, types.PartyID(sharedPartyConfig.PartyID))

		// Check Endpoint matches (Host:Port)
		expectedEndpoint := net.JoinHostPort(sharedPartyConfig.AssemblerConfig.Host, strconv.Itoa(int(sharedPartyConfig.AssemblerConfig.Port)))
		require.Equal(t, assembler.Endpoint, expectedEndpoint)

		// Check TLSCACerts count matches
		require.Equal(t, len(assembler.TLSCACerts), len(sharedPartyConfig.TLSCACerts))

		// Check TLSCert is not empty
		require.NotEmpty(t, assembler.TLSCert)
		require.Equal(t, len(assembler.TLSCert), len(sharedPartyConfig.AssemblerConfig.TlsCert))
	}
}

func TestExtractAssemblerAddresses(t *testing.T) {
	dir := t.TempDir()
	numOfParties := 4
	numOfShards := 2
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	// Load the genesis block and extract the bundle
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	data, err := os.ReadFile(genesisBlockPath)
	require.NoError(t, err)
	genesisBlock, err := protoutil.UnmarshalBlock(data)
	require.NoError(t, err)

	env, err := protoutil.ExtractEnvelope(genesisBlock, 0)
	require.NoError(t, err)
	bundle, err := channelconfig.NewBundleFromEnvelope(env, factory.GetDefault())
	require.NoError(t, err)

	// Get the orderer config from the bundle
	ordererConfig, ok := bundle.OrdererConfig()
	require.True(t, ok, "orderer config should exist in bundle")

	// Load the full config to compare with
	testLogger := testutil.CreateLoggerForModule(t, "TestExtractAssemblerAddresses", zap.DebugLevel)
	localConfigPathAssembler := filepath.Join(dir, "config", "party1", "local_config_assembler.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathAssembler, filepath.Join(dir, "storage"), "", 0)
	fullConfig, _, err := config.ReadConfig(localConfigPathAssembler, testLogger)
	require.NoError(t, err)

	// Call ExtractAssemblerAddresses
	party2Endpoint, err := config.ExtractAssemblerAddresses(ordererConfig)
	require.NoError(t, err)

	// Verify the returned map has the correct number of assemblers
	require.Equal(t, len(party2Endpoint), numOfParties)

	// Verify each assembler address entry matches the shared config
	for _, sharedPartyConfig := range fullConfig.SharedConfig.PartiesConfig {
		partyID := types.PartyID(sharedPartyConfig.PartyID)
		endpoint, ok := party2Endpoint[partyID]
		require.True(t, ok, "party %d should exist in party2Endpoint map", partyID)
		require.NotNil(t, endpoint)

		// Verify endpoint address matches (Host:Port)
		expectedEndpoint := net.JoinHostPort(sharedPartyConfig.AssemblerConfig.Host, strconv.Itoa(int(sharedPartyConfig.AssemblerConfig.Port)))
		require.Equal(t, endpoint.Address, expectedEndpoint)

		// Verify TLS root certs count matches
		require.Equal(t, len(endpoint.RootCerts), len(sharedPartyConfig.TLSCACerts))

		// Verify each root cert is present
		for i, rootCert := range endpoint.RootCerts {
			require.NotEmpty(t, rootCert)
			require.Equal(t, len(rootCert), len(sharedPartyConfig.TLSCACerts[i]))
		}
	}
}

// rejoinTestEnv holds the shared setup for the node-rejoin block-selection test. Each node role
// (router, batcher, assembler, consensus) discovers its last stored config block from a different kind of
// local storage, but they all read it through config.ReadConfig and must boot from the more
// advanced of the stored block and the bootstrap block. This env factors out the common network
// generation and the config-block/bootstrap/read helpers shared across node roles and scenarios.
type rejoinTestEnv struct {
	dir          string
	genesisBlock *common.Block
	logger       *flogging.FabricLogger
}

// newRejoinTestEnv generates a fresh network once and returns an env that all node roles share.
func newRejoinTestEnv(t *testing.T) *rejoinTestEnv {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 4, 2, "mTLS", "mTLS")
	t.Cleanup(netInfo.CleanUp)
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	// genesisBlock is a valid config block, used as the template for both stored and bootstrap blocks.
	genesisData, err := os.ReadFile(filepath.Join(dir, "bootstrap", "bootstrap.block"))
	require.NoError(t, err)
	genesisBlock, err := protoutil.UnmarshalBlock(genesisData)
	require.NoError(t, err)

	return &rejoinTestEnv{
		dir:          dir,
		genesisBlock: genesisBlock,
		logger:       testutil.CreateLoggerForModule(t, "ReadConfigRejoin", zap.DebugLevel),
	}
}

// configBlockOfNumber returns a clone of the genesis config block with the given sequence number.
func (e *rejoinTestEnv) configBlockOfNumber(number uint64) *common.Block {
	block := proto.Clone(e.genesisBlock).(*common.Block)
	block.Header.Number = number
	return block
}

// writeBootstrapBlock writes a config block with the given number to a file and returns its path.
func (e *rejoinTestEnv) writeBootstrapBlock(t *testing.T, name string, number uint64) string {
	path := filepath.Join(e.dir, "bootstrap", name)
	require.NoError(t, os.WriteFile(path, protoutil.MarshalOrPanic(e.configBlockOfNumber(number)), 0o644))
	return path
}

// readSelectedConfigBlock points the given node's local config at the storage path and bootstrap
// file, reads the configuration, and returns the config block that was selected.
func (e *rejoinTestEnv) readSelectedConfigBlock(t *testing.T, localConfigFile, storagePath, bootstrapPath string) *common.Block {
	localConfigPath := filepath.Join(e.dir, "config", "party1", localConfigFile)
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPath, storagePath, bootstrapPath, 0)
	_, block, err := config.ReadConfig(localConfigPath, e.logger)
	require.NoError(t, err)
	require.NotNil(t, block)
	return block
}

// rejoinScenario describes one stored-vs-bootstrap block-selection case, shared by all node roles.
type rejoinScenario struct {
	name                string
	subdir              string
	seed                bool     // whether the node's storage is pre-seeded with a stored config block
	storedNumber        uint64   // sequence number of the pre-seeded stored config block
	bootstrap           uint64   // sequence number of the bootstrap config block
	expected            uint64   // sequence number of the block that must be selected
	expectedStoreBlocks []uint64 // exact set of block numbers expected in a config-store node's (router/batcher) store afterwards
}

// rejoinNode describes one node role: which local config it reads and how to seed/verify its storage.
type rejoinNode struct {
	name            string
	localConfigFile string
	seed            func(t *testing.T, storagePath string, storedNumber uint64)
	verify          func(t *testing.T, storagePath string, s rejoinScenario) // optional, nil if none
}

// TestReadConfigRejoinBlock verifies how each node role selects between its last stored config block
// and the bootstrap block: it must always boot from the more advanced block (by sequence number).
// The network is created once and every scenario (empty / ahead / behind / equal) is exercised
// against all node roles — router, batcher, assembler, and consensus — which each store their last
// config block differently. Router and batcher additionally persist the selected block to their
// config store (when the bootstrap is a rejoin-block ahead of the stored one), so they also assert
// on store state.
func TestReadConfigRejoinBlock(t *testing.T) {
	e := newRejoinTestEnv(t)

	// --- router / batcher: config store ---
	// Router and batcher nodes both persist config blocks in a configstore.Store (see ReadConfig),
	// so they share the same seed and verify logic.
	seedConfigStore := func(t *testing.T, storagePath string, storedNumber uint64) {
		store, err := configstore.NewStore(storagePath)
		require.NoError(t, err)
		require.NoError(t, store.Add(e.configBlockOfNumber(storedNumber)))
	}
	verifyConfigStore := func(t *testing.T, storagePath string, s rejoinScenario) {
		store, err := configstore.NewStore(storagePath)
		require.NoError(t, err)
		last, err := store.Last()
		require.NoError(t, err)
		// The selected (more advanced) block must be the last block in the store.
		require.Equal(t, s.expected, last.Header.Number)
		// The store must hold exactly the expected set: a behind/equal bootstrap is never persisted.
		nums, err := store.ListBlockNumbers()
		require.NoError(t, err)
		require.ElementsMatch(t, s.expectedStoreBlocks, nums)
	}

	// --- assembler: block ledger ---
	// ledgerConfigBlock returns a clone of the genesis config block with the given sequence number,
	// a LAST_CONFIG metadata entry pointing at itself, and a previous-hash chained to prevBlock (nil
	// for a genesis block). This keeps the block valid for the assembler ledger, which validates the
	// block number and previous-hash chain on append.
	ledgerConfigBlock := func(number uint64, prevBlock *common.Block) *common.Block {
		block := e.configBlockOfNumber(number)
		// Reset the metadata to empty slots so the last-config index is read from the LAST_CONFIG entry
		// we set below. GetLastConfigIndexFromBlock reads the SIGNATURES metadata first and only falls
		// back to LAST_CONFIG when SIGNATURES is empty, so the cloned genesis SIGNATURES must be cleared.
		block.Metadata = &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {}, {}}}
		block.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
			Value: protoutil.MarshalOrPanic(&common.LastConfig{Index: number}),
		})
		if prevBlock != nil {
			block.Header.PreviousHash = protoutil.BlockHeaderHash(prevBlock.Header)
		} else {
			block.Header.PreviousHash = nil
		}
		return block
	}
	// seedAssemblerLedger appends config blocks 0..storedNumber to a fresh assembler ledger at
	// storagePath, so that GetLastConfigBlockFromAssemblerLedger returns the block numbered storedNumber.
	seedAssemblerLedger := func(t *testing.T, storagePath string, storedNumber uint64) {
		al, err := node_ledger.NewAssemblerLedger(e.logger, storagePath)
		require.NoError(t, err)
		defer al.Close()
		al.Metrics().NewAssemblerLedgerMetrics(monitoring.NewProvider(generate.DefaultMetricsProviderType, e.logger), "party1", e.logger)
		var prev *common.Block
		for n := uint64(0); n <= storedNumber; n++ {
			block := ledgerConfigBlock(n, prev)
			al.AppendBlock(block)
			prev = block
		}
	}

	// --- consensus: decision ledger ---
	// seedConsensusLedger appends a single decision block (number 0) to a fresh consensus ledger at
	// storagePath. The decision is its own last config decision and carries a config block numbered
	// storedNumber as its last available common block, so GetLastConfigBlockFromConsensusLedger
	// returns exactly that config block.
	seedConsensusLedger := func(t *testing.T, storagePath string, storedNumber uint64) {
		cl, err := node_ledger.NewConsensusLedger(storagePath)
		require.NoError(t, err)
		defer cl.Close()

		header := &state.Header{
			Num:                          0,
			DecisionNumOfLastConfigBlock: 0,
			AvailableCommonBlocks:        []*common.Block{e.configBlockOfNumber(storedNumber)},
		}
		proposal := smartbft_types.Proposal{Header: header.Serialize()}
		decisionBlock := state.CreateBlockToAppendFromDecision(0, proposal, nil, nil, 0)
		cl.Append(decisionBlock)
	}

	nodes := []rejoinNode{
		{name: "router", localConfigFile: "local_config_router.yaml", seed: seedConfigStore, verify: verifyConfigStore},
		{name: "batcher", localConfigFile: "local_config_batcher1.yaml", seed: seedConfigStore, verify: verifyConfigStore},
		{name: "assembler", localConfigFile: "local_config_assembler.yaml", seed: seedAssemblerLedger},
		{name: "consensus", localConfigFile: "local_config_consenter.yaml", seed: seedConsensusLedger},
	}

	scenarios := []rejoinScenario{
		{name: "empty storage, bootstrap taken", subdir: "empty", seed: false, storedNumber: 0, bootstrap: 0, expected: 0, expectedStoreBlocks: []uint64{0}},
		{name: "bootstrap ahead of stored, bootstrap taken", subdir: "ahead", seed: true, storedNumber: 3, bootstrap: 5, expected: 5, expectedStoreBlocks: []uint64{3, 5}},
		{name: "bootstrap behind stored, stored kept", subdir: "behind", seed: true, storedNumber: 5, bootstrap: 3, expected: 5, expectedStoreBlocks: []uint64{5}},
		{name: "bootstrap equal to stored, stored kept", subdir: "equal", seed: true, storedNumber: 5, bootstrap: 5, expected: 5, expectedStoreBlocks: []uint64{5}},
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			for _, n := range nodes {
				t.Run(n.name, func(t *testing.T) {
					storagePath := filepath.Join(e.dir, "storage", n.name, s.subdir)
					if s.seed {
						n.seed(t, storagePath, s.storedNumber)
					}
					bootstrapPath := e.writeBootstrapBlock(t, n.name+"_"+s.subdir+"_bootstrap.block", s.bootstrap)

					block := e.readSelectedConfigBlock(t, n.localConfigFile, storagePath, bootstrapPath)
					require.Equal(t, s.expected, block.Header.Number)

					if n.verify != nil {
						n.verify(t, storagePath, s)
					}
				})
			}
		})
	}
}

func ChangeExpirationTimeOfCert(t *testing.T, cert []byte, caCert []byte, caPrivateKey []byte) ([]byte, error) {
	// Parse the cert to be updated
	x509Cert, err := utils.Parsex509Cert(cert)
	require.NoError(t, err)

	// Parse ca cert and key
	x509CACert, err := utils.Parsex509Cert(caCert)
	require.NoError(t, err)
	caPrivKey, err := tx.CreateECDSAPrivateKey(caPrivateKey)
	require.NoError(t, err)

	// Modify expiration
	newCertTemplate := *x509Cert
	newCertTemplate.NotAfter = time.Now().Add(1 * time.Hour)

	// Re-sign the certificate with CA
	newCert, err := x509.CreateCertificate(rand.Reader, &newCertTemplate, x509CACert, x509Cert.PublicKey, caPrivKey)
	if err != nil {
		return nil, err
	}

	return pem.EncodeToMemory(&pem.Block{Bytes: newCert, Type: "CERTIFICATE"}), nil
}
