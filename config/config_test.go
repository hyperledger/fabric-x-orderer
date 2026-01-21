/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"

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
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathRouter, filepath.Join(dir, "storage"), "")

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
	_ = testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "ReadConfigBatcher", zap.DebugLevel)

	// choose local config for party1 shard1
	localConfigPathBacther := filepath.Join(dir, "config", "party1", "local_config_batcher1.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathBacther, filepath.Join(dir, "storage"), "")

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
	_ = testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "ReadConfigConsenter", zap.DebugLevel)

	// choose local config for party1
	localConfigPathConsenter := filepath.Join(dir, "config", "party1", "local_config_consenter.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathConsenter, filepath.Join(dir, "storage"), "")

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
	_ = testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	testLogger := testutil.CreateLoggerForModule(t, "ReadConfigAssembler", zap.DebugLevel)

	// choose local config for party1
	localConfigPathAssembler := filepath.Join(dir, "config", "party1", "local_config_assembler.yaml")
	testutil.EditDirectoryInNodeConfigYAML(t, localConfigPathAssembler, filepath.Join(dir, "storage"), "")

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
