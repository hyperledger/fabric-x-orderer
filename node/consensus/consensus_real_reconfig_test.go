/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	arma_node "github.com/hyperledger/fabric-x-orderer/node"
	batcher_node "github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	consensus_node "github.com/hyperledger/fabric-x-orderer/node/consensus"
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

// TestConsensusWithRealConfigUpdate tests the consensus behavior when a real config update happens. It covers the following scenarios:
// 1. submit a simple request to consensus and make sure it's committed
// 2. submit a config update request with wrong context and make sure it's rejected
// 3. submit a config update request not signed by majority and make sure it's rejected
// 4. submit a valid config update request and make sure it's committed with a smart bft parameter update
// 5. restart consensus nodes and make sure they can pick the new config and process requests
// 6. submit a config update request that changes a consenter's TLS certificate and make sure it's committed and the new certificate is effective after restart
// 7. submit a config update request that removes a non-leader consenter and make sure it's committed and the removed consenter cannot participate after restart
// 8. submit a config update request that removes the leader consenter and make sure it's committed and the removed leader cannot participate after restart
// TODO: add more scenarios such as adding a new consenter, removing multiple consenters, etc.
func TestConsensusWithRealConfigUpdate(t *testing.T) {
	parties := []types.PartyID{1, 2, 3, 4, 5, 6}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, len(parties), numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)

	nodesIPs := testutil.GetNodesIPsFromNetInfo(netInfo)
	require.NotNil(t, nodesIPs)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStorePath(t, dir, parties)

	consensusNodes, servers := createConsensusNodesAndGRPCServers(t, dir, parties)
	ledgerListeners := startConsensusNodesAndRegisterGRPCServers(parties, consensusNodes, servers)

	// submit to consensus a simple request (baf) from batcher
	keyBytes, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org1/orderers/party1/batcher1/msp/keystore/priv_sk"))
	require.NoError(t, err)
	privateKey, err := tx.CreateECDSAPrivateKey(keyBytes)
	require.NoError(t, err, "failed to create private key")
	lastBlockNumber := uint64(1)
	configSeq := types.ConfigSequence(0)
	sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, configSeq, lastBlockNumber)

	// submit to consensus a config request from router, with parameter update
	configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()
	configUpdatePbData := configUpdateBuilder.UpdateSmartBFTConfig(t, configutil.NewSmartBFTConfig(configutil.SmartBFTConfigName.SyncOnStart, true))
	env := configutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
	configReq := &protos.Request{
		Payload:   env.Payload,
		Signature: env.Signature,
	}
	// try to submit config with bad ctx and it should be rejected
	_, err = consensusNodes[0].SubmitConfig(t.Context(), configReq)
	require.Error(t, err)
	// create context with the router's certificate
	routerCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org1/orderers/party1/router/tls/tls-cert.pem"))
	require.NoError(t, err)
	block, _ := pem.Decode(routerCertBytes)
	require.NotNil(t, block)
	require.Equal(t, "CERTIFICATE", block.Type)
	routerCert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	ctx, err := createContextForSubmitConfig(routerCert)
	require.NoError(t, err)
	// submit config update not signed by majority should be rejected
	badEnv := configutil.CreateConfigTX(t, dir, parties[1:3], 1, configUpdatePbData)
	badConfigReq := &protos.Request{
		Payload:   badEnv.Payload,
		Signature: badEnv.Signature,
	}
	_, err = consensusNodes[0].SubmitConfig(ctx, badConfigReq)
	require.Error(t, err)
	// submit a good config update
	_, err = consensusNodes[0].SubmitConfig(ctx, configReq)
	require.NoError(t, err)

	// make sure the config block is committed and stop the consensus node
	lastBlockNumber++
	lastConfigBlock := makeSureConfigBlockCommittedAndStopConsensusNodes(t, consensusNodes, ledgerListeners, lastBlockNumber)

	// restart consensus nodes
	consensusNodes, servers = createConsensusNodesAndGRPCServers(t, dir, parties)
	ledgerListeners = startConsensusNodesAndRegisterGRPCServers(parties, consensusNodes, servers)

	// send another simple request
	lastBlockNumber++
	configSeq++
	sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, configSeq, lastBlockNumber)

	// submit a config request that changes a consenter's certificate
	newConfigBlockStoreDir := t.TempDir()
	defer os.RemoveAll(newConfigBlockStoreDir)
	err = configtxgen.WriteOutputBlock(lastConfigBlock, filepath.Join(newConfigBlockStoreDir, "config.block"))
	require.NoError(t, err)
	configUpdateBuilder, cleanUp = configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(newConfigBlockStoreDir, "config.block"))
	defer cleanUp()
	consenterToUpdate := types.PartyID(2)
	caCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", consenterToUpdate), "tlsca", "tlsca-cert.pem")
	caPrivKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", consenterToUpdate), "tlsca", "priv_sk")
	newCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", consenterToUpdate), "orderers", fmt.Sprintf("party%d", consenterToUpdate), "consenter", "tls")
	newKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", consenterToUpdate), "orderers", fmt.Sprintf("party%d", consenterToUpdate), "consenter", "tls", "key.pem")
	newCert, err := armageddon.CreateNewCertificateFromCA(caCertPath, caPrivKeyPath, newCertPath, newKeyPath, nodesIPs)
	require.NoError(t, err)
	configUpdatePbData = configUpdateBuilder.UpdateConsensusTLSCert(t, consenterToUpdate, newCert)
	env = configutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
	configReq = &protos.Request{
		Payload:   env.Payload,
		Signature: env.Signature,
	}
	_, err = consensusNodes[0].SubmitConfig(ctx, configReq)
	require.NoError(t, err)

	// make sure the config block is committed and stop the consensus node
	lastBlockNumber++
	lastConfigBlock = makeSureConfigBlockCommittedAndStopConsensusNodes(t, consensusNodes, ledgerListeners, lastBlockNumber)

	// restart consensus nodes
	consensusNodes, servers = createConsensusNodesAndGRPCServers(t, dir, parties)
	ledgerListeners = startConsensusNodesAndRegisterGRPCServers(parties, consensusNodes, servers)

	// send another simple request
	lastBlockNumber++
	configSeq++
	sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, configSeq, lastBlockNumber)

	removedParty := types.PartyID(6)

	// submit a config request that removes the last party
	configBlockStoreDir := t.TempDir()
	defer os.RemoveAll(configBlockStoreDir)
	err = configtxgen.WriteOutputBlock(lastConfigBlock, filepath.Join(configBlockStoreDir, "config.block"))
	require.NoError(t, err)
	configUpdateBuilder, cleanUp = configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(configBlockStoreDir, "config.block"))
	defer cleanUp()
	configUpdatePbData = configUpdateBuilder.RemoveParty(t, removedParty)
	env = configutil.CreateConfigTX(t, dir, parties[0:5], 1, configUpdatePbData)
	configReq = &protos.Request{
		Payload:   env.Payload,
		Signature: env.Signature,
	}
	_, err = consensusNodes[0].SubmitConfig(ctx, configReq)
	require.NoError(t, err)

	// make sure the config block is committed and stop the consensus node
	lastBlockNumber++
	lastConfigBlock = makeSureConfigBlockCommittedAndStopConsensusNodes(t, consensusNodes, ledgerListeners, lastBlockNumber)

	parties = []types.PartyID{1, 2, 3, 4, 5}

	// try to get the removed party
	removedNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", removedParty), "local_config_consenter.yaml")
	removedNodeConfigContent, removedNodeLastConfigBlock, err := config.ReadConfig(removedNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigConsensus", zap.DebugLevel))
	require.NoError(t, err)
	require.Panics(t, func() { removedNodeConfigContent.ExtractConsenterConfig(removedNodeLastConfigBlock) })

	// restart consensus nodes
	consensusNodes, servers = createConsensusNodesAndGRPCServers(t, dir, parties)
	ledgerListeners = startConsensusNodesAndRegisterGRPCServers(parties, consensusNodes, servers)

	// send another simple request
	lastBlockNumber++
	configSeq++
	sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, configSeq, lastBlockNumber)

	removedPartyLeader := types.PartyID(1)

	// submit a config request that removes the first party (leader)
	anotherConfigBlockStoreDir := t.TempDir()
	defer os.RemoveAll(anotherConfigBlockStoreDir)
	err = configtxgen.WriteOutputBlock(lastConfigBlock, filepath.Join(anotherConfigBlockStoreDir, "config.block"))
	require.NoError(t, err)
	configUpdateBuilder, cleanUp = configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(anotherConfigBlockStoreDir, "config.block"))
	defer cleanUp()
	configUpdatePbData = configUpdateBuilder.RemoveParty(t, removedPartyLeader)
	env = configutil.CreateConfigTX(t, dir, parties[1:5], 1, configUpdatePbData)
	configReq = &protos.Request{
		Payload:   env.Payload,
		Signature: env.Signature,
	}
	_, err = consensusNodes[0].SubmitConfig(ctx, configReq)
	require.NoError(t, err)

	// make sure the config block is committed and stop the consensus node
	lastBlockNumber++
	lastConfigBlock = makeSureConfigBlockCommittedAndStopConsensusNodes(t, consensusNodes, ledgerListeners, lastBlockNumber)
	require.NotNil(t, lastConfigBlock)

	parties = []types.PartyID{2, 3, 4, 5}

	// try to get the removed party
	removedLeaderNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", removedParty), "local_config_consenter.yaml")
	removedLeaderNodeConfigContent, removedLeaderNodeLastConfigBlock, err := config.ReadConfig(removedLeaderNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigConsensus", zap.DebugLevel))
	require.NoError(t, err)
	require.Panics(t, func() { removedLeaderNodeConfigContent.ExtractConsenterConfig(removedLeaderNodeLastConfigBlock) })

	// restart consensus nodes
	consensusNodes, servers = createConsensusNodesAndGRPCServers(t, dir, parties)
	startConsensusNodesAndRegisterGRPCServers(parties, consensusNodes, servers)

	for _, consensusNode := range consensusNodes {
		consensusNode.Stop()
	}
}

func updateFileStorePath(t *testing.T, dir string, parties []types.PartyID) {
	for _, i := range parties {
		fileStoreDir := t.TempDir()
		nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_consenter.yaml")
		localConfig, _, err := config.LoadLocalConfig(nodeConfigPath)
		require.NoError(t, err)
		localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
		utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)
	}
}

func createConsensusNodesAndGRPCServers(t *testing.T, dir string, parties []types.PartyID) ([]*consensus_node.Consensus, []*comm.GRPCServer) {
	consensusNodes := make([]*consensus_node.Consensus, 0, len(parties))
	servers := make([]*comm.GRPCServer, 0, len(parties))
	for _, i := range parties {
		nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_consenter.yaml")
		configContent, lastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigConsensus", zap.DebugLevel))
		require.NoError(t, err)
		consenterConfig := configContent.ExtractConsenterConfig(lastConfigBlock)
		require.NotNil(t, consenterConfig)
		_, signer, _ := testutil.BuildTestLocalMSP(configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID)
		require.NoError(t, err)
		require.NotNil(t, signer)
		consenterLogger := testutil.CreateLogger(t, int(i))
		server := arma_node.CreateGRPCConsensus(consenterConfig)
		servers = append(servers, server)
		consensus := consensus_node.CreateConsensus(consenterConfig, server, lastConfigBlock, consenterLogger, signer, &policy.DefaultConfigUpdateProposer{})
		consensusNodes = append(consensusNodes, consensus)
	}
	return consensusNodes, servers
}

func startConsensusNodesAndRegisterGRPCServers(parties []types.PartyID, consensusNodes []*consensus_node.Consensus, servers []*comm.GRPCServer) []*storageListener {
	for i, consensusNode := range consensusNodes {
		protos.RegisterConsensusServer(servers[i].Server(), consensusNode)
		orderer.RegisterAtomicBroadcastServer(servers[i].Server(), consensusNode.DeliverService)
		orderer.RegisterClusterNodeServiceServer(servers[i].Server(), consensusNode)
		go func() {
			servers[i].Start()
		}()
	}

	ledgerListeners := make([]*storageListener, 0, len(parties))
	for _, consensusNode := range consensusNodes {
		consensusNode.Start()
		ledgerListener := &storageListener{c: make(chan *common.Block, 100)}
		consensusNode.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(ledgerListener)
		ledgerListeners = append(ledgerListeners, ledgerListener)
	}

	time.Sleep(5 * time.Second)

	return ledgerListeners
}

func sendSimpleRequest(t *testing.T, consensusNodes []*consensus_node.Consensus, ledgerListeners []*storageListener, privateKey *ecdsa.PrivateKey, configSeq types.ConfigSequence, expectedHeaderNumber uint64) {
	for _, consensusNode := range consensusNodes {
		baf, err := batcher_node.CreateBAF((*crypto.ECDSASigner)(privateKey), 1, 1, digest123, 2, 0, configSeq+1)
		require.NoError(t, err)
		controlEvent := &state.ControlEvent{BAF: baf}
		err = consensusNode.SubmitRequest(controlEvent.Bytes())
		require.ErrorContains(t, err, "mismatch config sequence")
	}
	baf, err := batcher_node.CreateBAF((*crypto.ECDSASigner)(privateKey), 1, 1, digest123, 2, 0, configSeq)
	require.NoError(t, err)
	controlEvent := &state.ControlEvent{BAF: baf}
	err = consensusNodes[0].SubmitRequest(controlEvent.Bytes())
	require.NoError(t, err)
	for _, ledgerListener := range ledgerListeners {
		require.Eventually(t, func() bool {
			b := <-ledgerListener.c
			return b.Header.Number == expectedHeaderNumber
		}, 30*time.Second, 100*time.Millisecond)
	}
}

func makeSureConfigBlockCommittedAndStopConsensusNodes(t *testing.T, consensusNodes []*consensus_node.Consensus, ledgerListeners []*storageListener, expectedBlockNumber uint64) (lastConfigBlock *common.Block) {
	for i, consensusNode := range consensusNodes {
		var lastDecision *common.Block
		require.Eventually(t, func() bool {
			lastDecision = <-ledgerListeners[i].c
			return lastDecision.Header.Number == expectedBlockNumber
		}, 30*time.Second, 100*time.Millisecond)

		proposal, _, err := state.BytesToDecision(lastDecision.Data.Data[0])
		require.NotNil(t, proposal)
		require.NoError(t, err)
		header := &state.Header{}
		err = header.Deserialize(proposal.Header)
		require.NoError(t, err)
		lastConfigBlock = header.AvailableCommonBlocks[len(header.AvailableCommonBlocks)-1]
		require.True(t, protoutil.IsConfigBlock(lastConfigBlock))
		require.True(t, header.Num == header.DecisionNumOfLastConfigBlock)

		consensusNode.Stop()
	}
	return lastConfigBlock
}
