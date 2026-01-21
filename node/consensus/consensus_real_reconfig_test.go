/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/msp"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	arma_node "github.com/hyperledger/fabric-x-orderer/node"
	batcher_node "github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestConsensusWithRealConfigUpdate(t *testing.T) {
	// Create a temporary directory for the test.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	numOfParties := 1
	numOfShards := 1

	// Create a config YAML file in the temporary directory.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)

	// Generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// Update the file store path
	fileStoreDir := t.TempDir()
	defer os.RemoveAll(fileStoreDir)
	nodeConfigPath := filepath.Join(dir, "config", "party1", "local_config_consenter.yaml")
	localConfig, _, err := config.LoadLocalConfig(nodeConfigPath)
	require.NoError(t, err)
	localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
	utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)

	// Get all for create consensus
	configContent, lastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigConsensus", zap.DebugLevel))
	require.NoError(t, err)
	consenterConfig := configContent.ExtractConsenterConfig(lastConfigBlock)
	require.NotNil(t, consenterConfig)
	localmsp := msp.BuildLocalMSP(configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID, configContent.LocalConfig.NodeLocalConfig.GeneralConfig.BCCSP)
	signer, err := localmsp.GetDefaultSigningIdentity()
	require.NoError(t, err)
	consenterLogger := testutil.CreateLogger(t, int(numOfParties))
	server := arma_node.CreateGRPCConsensus(consenterConfig)
	consensus := consensus.CreateConsensus(consenterConfig, server, lastConfigBlock, consenterLogger, signer, &policy.DefaultConfigUpdateProposer{})

	// Register and start grpc server
	protos.RegisterConsensusServer(server.Server(), consensus)
	orderer.RegisterAtomicBroadcastServer(server.Server(), consensus.DeliverService)
	orderer.RegisterClusterNodeServiceServer(server.Server(), consensus)
	go func() {
		server.Start()
	}()

	// Start consensus
	consensus.Start()
	ledgerListener := &storageListener{c: make(chan *common.Block, 100)}
	consensus.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(ledgerListener)

	// Submit to consensus a simple request (baf) from batcher
	digest := make([]byte, 32-3)
	digest123 := append([]byte{1, 2, 3}, digest...)
	keyBytes, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org1/orderers/party1/batcher1/msp/keystore/priv_sk"))
	require.NoError(t, err)
	privateKey, err := tx.CreateECDSAPrivateKey(keyBytes)
	require.NoError(t, err, "failed to create private key")
	baf, err := batcher_node.CreateBAF((*crypto.ECDSASigner)(privateKey), 1, 1, digest123, 1, 0, 0)
	require.NoError(t, err)
	controlEvent := &state.ControlEvent{BAF: baf}
	consensus.SubmitRequest(controlEvent.Bytes())
	require.Eventually(t, func() bool {
		b := <-ledgerListener.c
		return b.Header.Number == uint64(1)
	}, 30*time.Second, 100*time.Millisecond)

	// Submit to consensus a config request from router
	configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()
	configUpdatePbData := configUpdateBuilder.UpdateSmartBFTConfig(t, configutil.NewSmartBFTConfig(configutil.SmartBFTConfigName.SyncOnStart, true))
	env := configutil.CreateConfigTX(t, dir, numOfParties, 1, configUpdatePbData)
	configReq := &protos.Request{
		Payload:   env.Payload,
		Signature: env.Signature,
	}
	routerCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org1/orderers/party1/router/tls/tls-cert.pem"))
	require.NoError(t, err)
	block, _ := pem.Decode(routerCertBytes)
	require.NotNil(t, block)
	require.Equal(t, "CERTIFICATE", block.Type)
	routerCert, err := x509.ParseCertificate(block.Bytes)
	ctx, err := createContextForSubmitConfig(routerCert)
	require.NoError(t, err)
	_, err = consensus.SubmitConfig(ctx, configReq)
	require.NoError(t, err)
	var lastDecision *common.Block
	require.Eventually(t, func() bool {
		lastDecision = <-ledgerListener.c
		return lastDecision.Header.Number == uint64(2)
	}, 30*time.Second, 100*time.Millisecond)
	proposal, _, err := state.BytesToDecision(lastDecision.Data.Data[0])
	require.NotNil(t, proposal)
	require.NoError(t, err)

	header := &state.Header{}
	err = header.Deserialize(proposal.Header)
	require.NoError(t, err)

	lastBlock := header.AvailableCommonBlocks[len(header.AvailableCommonBlocks)-1]
	require.True(t, protoutil.IsConfigBlock(lastBlock))
	require.True(t, header.Num == header.DecisionNumOfLastConfigBlock)
}
