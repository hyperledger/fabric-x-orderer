/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	batcher_node "github.com/hyperledger/fabric-x-orderer/node/batcher"
	consensus_node "github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
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
// 9. submit a config update request that changes a consenter's endpoint and make sure it's committed and the new endpoint is effective after restart
// 10. submit a config update request that adds a new consenter and make sure it's committed and the new consenter can participate in consensus
func TestConsensusWithRealConfigUpdate(t *testing.T) {
	parties := []types.PartyID{1, 2, 3, 4, 5, 6}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetworkWithPortAllocator(t, configPath, len(parties), numOfShards, "TLS", "none", testutil.SharedTestPortAllocator())
	require.NotNil(t, netInfo)

	nodesIPs := testutil.GetNodesIPsFromNetInfo(netInfo)
	require.NotNil(t, nodesIPs)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStoreAndMonitoringPort(t, dir, netInfo)

	netInfo.CleanUp()
	consensusNodes, servers, _ := createConsensusNodesAndGRPCServers(t, dir, parties)
	ledgerListeners := startConsensusNodesAndRegisterGRPCServers(t, parties, consensusNodes, servers)

	// submit to consensus a simple request (baf) from batcher
	keyBytes, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org1/orderers/party1/batcher1/msp/keystore/priv_sk"))
	require.NoError(t, err)
	privateKey, err := tx.CreateECDSAPrivateKey(keyBytes)
	require.NoError(t, err, "failed to create private key")
	keyBytes2, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org2/orderers/party2/batcher1/msp/keystore/priv_sk"))
	require.NoError(t, err)
	privateKey2, err := tx.CreateECDSAPrivateKey(keyBytes2)
	require.NoError(t, err, "failed to create private key")
	lastBlockNumber := uint64(1)
	configSeq := types.ConfigSequence(0)
	sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, 1, configSeq+1, lastBlockNumber, "mismatch config sequence")
	sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, 1, configSeq, lastBlockNumber, "")

	var routerCtx context.Context
	t.Run("reject config update", func(t *testing.T) {
		// submit to consensus a config request from router, with parameter update
		configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
		defer cleanUp()
		configUpdatePbData := configUpdateBuilder.UpdateSmartBFTConfig(t, configutil.NewSmartBFTConfig(configutil.SmartBFTConfigName.SyncOnStart, true))
		env := configutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
		}
		// try to submit config with bad ctx (just any context without the router's certificate) and it should be rejected
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
		routerCtx, err = createContextForSubmitConfig(routerCert)
		require.NoError(t, err)
		// submit config update not signed by majority should be rejected
		badEnv := configutil.CreateConfigTX(t, dir, parties[1:3], 1, configUpdatePbData)
		badConfigReq := &protos.Request{
			Payload:   badEnv.Payload,
			Signature: badEnv.Signature,
		}
		_, err = consensusNodes[0].SubmitConfig(routerCtx, badConfigReq)
		require.Error(t, err)
	})

	var lastConfigBlock *common.Block
	t.Run("config update with parameter change", func(t *testing.T) {
		// submit to consensus a config request from router, with parameter update
		configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
		defer cleanUp()
		configUpdatePbData := configUpdateBuilder.UpdateSmartBFTConfig(t, configutil.NewSmartBFTConfig(configutil.SmartBFTConfigName.SyncOnStart, true))
		env := configutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
		}
		// submit a good config update
		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		// make sure the config block is committed
		lastBlockNumber++
		configSeq++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, ledgerListeners, lastBlockNumber)

		// wait for consensus nodes to apply new config and run again
		for _, consenter := range consensusNodes {
			waitForRunningState(t, consenter, uint64(configSeq))
		}

		// re-register ledger listeners
		ledgerListeners = make([]*storageListener, 0, len(parties))
		for _, consensusNode := range consensusNodes {
			ledgerListener := &storageListener{c: make(chan *common.Block, 100)}
			consensusNode.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(ledgerListener)
			ledgerListeners = append(ledgerListeners, ledgerListener)
		}

		// wait for consenters to start before submitting a request
		time.Sleep(10 * time.Second)

		// send another simple request
		lastBlockNumber++
		sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, 1, configSeq, lastBlockNumber, "")
		sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, 1, configSeq+1, lastBlockNumber, "mismatch config sequence")
		sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, 1, configSeq-1, lastBlockNumber, "mismatch config sequence")
	})

	t.Run("config update with consenter's certificate change", func(t *testing.T) {
		// submit a config request that changes a consenter's certificate
		newConfigBlockStoreDir := t.TempDir()
		err = configtxgen.WriteOutputBlock(lastConfigBlock, filepath.Join(newConfigBlockStoreDir, "config.block"))
		require.NoError(t, err)
		configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(newConfigBlockStoreDir, "config.block"))
		defer cleanUp()
		consenterToUpdate := types.PartyID(2)
		caCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", consenterToUpdate), "tlsca", "tlsca-cert.pem")
		caPrivKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", consenterToUpdate), "tlsca", "priv_sk")
		newCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", consenterToUpdate), "orderers", fmt.Sprintf("party%d", consenterToUpdate), "consenter", "tls")
		newKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", consenterToUpdate), "orderers", fmt.Sprintf("party%d", consenterToUpdate), "consenter", "tls", "key.pem")
		newCert, err := armageddon.CreateNewCertificateFromCA(caCertPath, caPrivKeyPath, "tls", newCertPath, newKeyPath, nodesIPs)
		require.NoError(t, err)
		configUpdatePbData := configUpdateBuilder.UpdateConsensusTLSCert(t, consenterToUpdate, newCert)
		env := configutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
		}
		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		// make sure the config block is committed and stop the consensus node
		lastBlockNumber++
		configSeq++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, ledgerListeners, lastBlockNumber)

		// wait for the updated consensus node to enter pending admin state and then stop the node
		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() == consenterToUpdate {
				waitForPendingAdminState(t, consenter, uint64(configSeq-1))
				consenter.Stop()
				break
			}
		}

		// wait for the rest of consensus nodes to apply new config and run again
		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() != consenterToUpdate {
				waitForRunningState(t, consenter, uint64(configSeq))
			}
		}

		// restart the updated consensus node
		updatedConsensusNode, updatedConsensusNodeServer, _ := createConsensusNodesAndGRPCServers(t, dir, []types.PartyID{consenterToUpdate})
		updatedConsensusNodeLedgerListener := startConsensusNodesAndRegisterGRPCServers(t, []types.PartyID{consenterToUpdate}, updatedConsensusNode, updatedConsensusNodeServer)
		waitForRunningState(t, updatedConsensusNode[0], uint64(configSeq))

		// re-register ledger listeners
		oldConsensusNodes := consensusNodes
		ledgerListeners = make([]*storageListener, 0, len(parties))
		consensusNodes = make([]*consensus_node.Consensus, 0, len(parties))
		for _, consensusNode := range oldConsensusNodes {
			if consensusNode.Config.PartyId != consenterToUpdate {
				ledgerListener := &storageListener{c: make(chan *common.Block, 100)}
				consensusNode.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(ledgerListener)
				ledgerListeners = append(ledgerListeners, ledgerListener)
				consensusNodes = append(consensusNodes, consensusNode)
			}
		}

		ledgerListeners = append(ledgerListeners, updatedConsensusNodeLedgerListener[0])
		consensusNodes = append(consensusNodes, updatedConsensusNode[0])

		// wait for consenters to start before submitting a request
		time.Sleep(10 * time.Second)

		// send another simple request
		lastBlockNumber++
		sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, 1, configSeq, lastBlockNumber, "")
	})

	t.Run("config update with consenter removal", func(t *testing.T) {
		removedParty := types.PartyID(6)

		// submit a config request that removes the last party
		configBlockStoreDir := t.TempDir()
		err = configtxgen.WriteOutputBlock(lastConfigBlock, filepath.Join(configBlockStoreDir, "config.block"))
		require.NoError(t, err)
		configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(configBlockStoreDir, "config.block"))
		defer cleanUp()
		configUpdatePbData := configUpdateBuilder.RemoveParty(t, removedParty)
		env := configutil.CreateConfigTX(t, dir, parties[0:5], 1, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
		}
		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		// make sure the config block is committed
		lastBlockNumber++
		configSeq++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, ledgerListeners, lastBlockNumber)

		// wait for the removed consensus node to enter pending admin state and then stop the node
		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() == removedParty {
				waitForPendingAdminState(t, consenter, uint64(configSeq-1))
				consenter.Stop()
				break
			}
		}

		// wait for the rest of consensus nodes to apply new config and run again
		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() != removedParty {
				waitForRunningState(t, consenter, uint64(configSeq))
			}
		}

		parties = []types.PartyID{1, 2, 3, 4, 5}

		// try to get the removed party
		removedNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", removedParty), "local_config_consenter.yaml")
		removedNodeConfigContent, removedNodeLastConfigBlock, err := config.ReadConfig(removedNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigConsensus", zap.DebugLevel))
		require.NoError(t, err)
		require.Panics(t, func() { removedNodeConfigContent.ExtractConsenterConfig(removedNodeLastConfigBlock) })

		// re-register ledger listeners
		oldConsensusNodes := consensusNodes
		ledgerListeners = make([]*storageListener, 0, len(parties))
		consensusNodes = make([]*consensus_node.Consensus, 0, len(parties))
		for _, consensusNode := range oldConsensusNodes {
			if consensusNode.Config.PartyId != removedParty {
				ledgerListener := &storageListener{c: make(chan *common.Block, 100)}
				consensusNode.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(ledgerListener)
				ledgerListeners = append(ledgerListeners, ledgerListener)
				consensusNodes = append(consensusNodes, consensusNode)
			}
		}

		// send another simple request
		lastBlockNumber++
		sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, 1, configSeq, lastBlockNumber, "")
	})

	t.Run("config update with leader consenter removal", func(t *testing.T) {
		removedPartyLeader := types.PartyID(1)

		// submit a config request that removes the first party (leader)
		anotherConfigBlockStoreDir := t.TempDir()
		err = configtxgen.WriteOutputBlock(lastConfigBlock, filepath.Join(anotherConfigBlockStoreDir, "config.block"))
		require.NoError(t, err)
		configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(anotherConfigBlockStoreDir, "config.block"))
		defer cleanUp()
		configUpdatePbData := configUpdateBuilder.RemoveParty(t, removedPartyLeader)
		env := configutil.CreateConfigTX(t, dir, parties[1:5], 1, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
		}
		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		// make sure the config block is committed and stop the consensus node
		lastBlockNumber++
		configSeq++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, ledgerListeners, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		// wait for the removed consensus node to enter pending admin state and then stop the node
		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() == removedPartyLeader {
				waitForPendingAdminState(t, consenter, uint64(configSeq-1))
				consenter.Stop()
				break
			}
		}

		// wait for the rest of consensus nodes to apply new config and run again
		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() != removedPartyLeader {
				waitForRunningState(t, consenter, uint64(configSeq))
			}
		}

		parties = []types.PartyID{2, 3, 4, 5}

		// try to get the removed party
		removedLeaderNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", removedPartyLeader), "local_config_consenter.yaml")
		removedLeaderNodeConfigContent, removedLeaderNodeLastConfigBlock, err := config.ReadConfig(removedLeaderNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigConsensus", zap.DebugLevel))
		require.NoError(t, err)
		require.Panics(t, func() { removedLeaderNodeConfigContent.ExtractConsenterConfig(removedLeaderNodeLastConfigBlock) })

		// re-register ledger listeners
		oldConsensusNodes := consensusNodes
		ledgerListeners = make([]*storageListener, 0, len(parties))
		consensusNodes = make([]*consensus_node.Consensus, 0, len(parties))
		for _, consensusNode := range oldConsensusNodes {
			if consensusNode.Config.PartyId != removedPartyLeader {
				ledgerListener := &storageListener{c: make(chan *common.Block, 100)}
				consensusNode.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(ledgerListener)
				ledgerListeners = append(ledgerListeners, ledgerListener)
				consensusNodes = append(consensusNodes, consensusNode)
			}
		}

		// send another simple request
		lastBlockNumber++
		sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey, 1, configSeq, lastBlockNumber, "key does not exist")
		sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey2, 2, configSeq, lastBlockNumber, "")
	})

	t.Run("config update with consenter's endpoint change", func(t *testing.T) {
		consenterPartyToUpdate := types.PartyID(2)
		consenterPartyToUpdateIndex := 0
		for i, consenter := range consensusNodes {
			if consenter.GetPartyID() == consenterPartyToUpdate {
				consenterPartyToUpdateIndex = i
				break
			}
		}

		// stop nodes before reading the current config (ReadConfig func needs to read the consenter ledger and it is unavailable when a consenter is running)
		for _, consensusNode := range consensusNodes {
			consensusNode.Stop()
		}

		// get the current endpoint
		consenterNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", consenterPartyToUpdate), "local_config_consenter.yaml")
		cfg, lcb, err := config.ReadConfig(consenterNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigConsenter", zap.DebugLevel))
		require.NoError(t, err)
		consenterConfig := cfg.ExtractConsenterConfig(lcb)
		nodeIP := strings.Split(consenterConfig.Consenters[consenterPartyToUpdateIndex].Endpoint, ":")[0]
		availablePort, ll := testutil.SharedTestPortAllocator().Allocate(t)
		ll.Close()
		newPort, err := strconv.Atoi(availablePort)
		require.NoError(t, err)

		// create the config update
		oneMoreConfigBlockStoreDir := t.TempDir()
		err = configtxgen.WriteOutputBlock(lastConfigBlock, filepath.Join(oneMoreConfigBlockStoreDir, "config.block"))
		require.NoError(t, err)
		configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(oneMoreConfigBlockStoreDir, "config.block"))
		defer cleanUp()
		configUpdatePbData := configUpdateBuilder.UpdateOrderingEndpoint(t, consenterPartyToUpdate, nodeIP, newPort)
		env := configutil.CreateConfigTX(t, dir, parties[0:4], 1, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
		}

		// restart consensus nodes
		consensusNodes, servers, _ = createConsensusNodesAndGRPCServers(t, dir, parties)
		ledgerListeners = startConsensusNodesAndRegisterGRPCServers(t, parties, consensusNodes, servers)

		// use a different router's certificate to submit config update (as the old router's party was removed in the previous test)
		routerCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org2/orderers/party2/router/tls/tls-cert.pem"))
		require.NoError(t, err)
		block, _ := pem.Decode(routerCertBytes)
		require.NotNil(t, block)
		require.Equal(t, "CERTIFICATE", block.Type)
		routerCert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)
		routerCtx, err = createContextForSubmitConfig(routerCert)
		require.NoError(t, err)

		// submit the config update
		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		// make sure the config block is committed
		lastBlockNumber++
		configSeq++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, ledgerListeners, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		// update the node local config
		localConfig, _, err := config.LoadLocalConfig(consenterNodeConfigPath)
		require.NoError(t, err)
		localConfig.NodeLocalConfig.GeneralConfig.ListenAddress = nodeIP
		localConfig.NodeLocalConfig.GeneralConfig.ListenPort = uint32(newPort)
		err = utils.WriteToYAML(localConfig.NodeLocalConfig, consenterNodeConfigPath)
		require.NoError(t, err)

		// wait for the updated consensus node to enter pending admin state and then stop the node
		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() == consenterPartyToUpdate {
				waitForPendingAdminState(t, consenter, uint64(configSeq-1))
				consenter.Stop()
				break
			}
		}

		// wait for the rest of consensus nodes to apply new config and run again
		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() != consenterPartyToUpdate {
				waitForRunningState(t, consenter, uint64(configSeq))
			}
		}

		// restart the updated consensus node
		updatedConsensusNode, updatedConsensusNodeServer, _ := createConsensusNodesAndGRPCServers(t, dir, []types.PartyID{consenterPartyToUpdate})
		updatedConsensusNodeLedgerListener := startConsensusNodesAndRegisterGRPCServers(t, []types.PartyID{consenterPartyToUpdate}, updatedConsensusNode, updatedConsensusNodeServer)
		waitForRunningState(t, updatedConsensusNode[0], uint64(configSeq))

		// re-register ledger listeners
		oldConsensusNodes := consensusNodes
		ledgerListeners = make([]*storageListener, 0, len(parties))
		consensusNodes = make([]*consensus_node.Consensus, 0, len(parties))
		for _, consensusNode := range oldConsensusNodes {
			if consensusNode.Config.PartyId != consenterPartyToUpdate {
				ledgerListener := &storageListener{c: make(chan *common.Block, 100)}
				consensusNode.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(ledgerListener)
				ledgerListeners = append(ledgerListeners, ledgerListener)
				consensusNodes = append(consensusNodes, consensusNode)
			} else {
				ledgerListeners = append(ledgerListeners, updatedConsensusNodeLedgerListener[0])
				consensusNodes = append(consensusNodes, updatedConsensusNode[0])
			}
		}

		// wait for consenters to start before submitting a request
		time.Sleep(10 * time.Second)

		// send another simple request
		lastBlockNumber++
		sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey2, 2, configSeq, lastBlockNumber, "")
	})

	t.Run("config update with consenter addition", func(t *testing.T) {
		// create config update to add the new party
		addConfigBlockStoreDir := t.TempDir()
		err = configtxgen.WriteOutputBlock(lastConfigBlock, filepath.Join(addConfigBlockStoreDir, "config.block"))
		require.NoError(t, err)
		configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(addConfigBlockStoreDir, "config.block"))
		defer cleanUp()

		// add the new party to the configuration
		addedPartyID, addedNetInfo := configUpdateBuilder.PrepareAndAddNewParty(t, dir)
		require.NotNil(t, addedNetInfo)

		// create and sign the config transaction (parties are now [2,3,4,5] after party 1 was removed)
		configUpdatePbData := configUpdateBuilder.ConfigUpdatePBData(t)
		env := configutil.CreateConfigTX(t, dir, parties[0:3], int(parties[0]), configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
		}

		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		// make sure the config block is committed
		lastBlockNumber++
		configSeq++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, ledgerListeners, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		// wait for existing consensus nodes to apply new config and run again
		for _, consenter := range consensusNodes {
			waitForRunningState(t, consenter, uint64(configSeq))
		}

		// add the new party to the parties list
		parties = append(parties, addedPartyID)

		// write the last config block to a file so the new party can read it
		newConfigBlockPath := filepath.Join(t.TempDir(), "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, newConfigBlockPath)
		require.NoError(t, err)

		// update the config block path in the net info of the added party
		for _, netNode := range addedNetInfo {
			netNode.ConfigBlockPath = newConfigBlockPath
		}

		// update file store and monitoring port for the new party
		updateFileStoreAndMonitoringPort(t, dir, addedNetInfo)

		// close the listeners allocated by ExtendNetwork so the ports can be reused
		for _, nodeInfo := range addedNetInfo {
			nodeInfo.Close()
		}

		// start the new consensus node
		newConsensusNode, newConsensusNodeServer, _ := createConsensusNodesAndGRPCServers(t, dir, []types.PartyID{addedPartyID})
		newConsensusNodeLedgerListener := startConsensusNodesAndRegisterGRPCServers(t, []types.PartyID{addedPartyID}, newConsensusNode, newConsensusNodeServer)
		waitForRunningState(t, newConsensusNode[0], uint64(configSeq))

		// re-register ledger listeners for all nodes including the new one
		ledgerListeners = make([]*storageListener, 0, len(parties))
		for _, consensusNode := range consensusNodes {
			ledgerListener := &storageListener{c: make(chan *common.Block, 100)}
			consensusNode.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(ledgerListener)
			ledgerListeners = append(ledgerListeners, ledgerListener)
		}
		ledgerListeners = append(ledgerListeners, newConsensusNodeLedgerListener[0])
		consensusNodes = append(consensusNodes, newConsensusNode[0])

		// wait for consenters to start before submitting a request
		time.Sleep(10 * time.Second)

		// send another simple request to verify the new consenter is participating
		lastBlockNumber++
		sendSimpleRequest(t, consensusNodes, ledgerListeners, privateKey2, 2, configSeq, lastBlockNumber, "")
	})

	for _, consensusNode := range consensusNodes {
		consensusNode.Stop()
	}
}

func sendSimpleRequest(t *testing.T, consensusNodes []*consensus_node.Consensus, ledgerListeners []*storageListener, privateKey *ecdsa.PrivateKey, batcherID types.PartyID, configSeqToSend types.ConfigSequence, expectedHeaderNumber uint64, expectedError string) {
	baf, err := batcher_node.CreateBAF((*crypto.ECDSASigner)(privateKey), batcherID, 1, digest123, 2, 0, configSeqToSend, 1)
	require.NoError(t, err)
	controlEvent := &state.ControlEvent{BAF: baf}
	err = consensusNodes[0].SubmitRequest(controlEvent.Bytes())
	if expectedError != "" {
		require.ErrorContains(t, err, expectedError)
		return
	}
	require.NoError(t, err)

	for _, ledgerListener := range ledgerListeners {
		require.Eventually(t, func() bool {
			select {
			case b := <-ledgerListener.c:
				return b.Header.Number == expectedHeaderNumber
			default:
				return false
			}
		}, 60*time.Second, 100*time.Millisecond)
	}
}

func makeSureConfigBlockCommitted(t *testing.T, consensusNodes []*consensus_node.Consensus, ledgerListeners []*storageListener, expectedBlockNumber uint64) (lastConfigBlock *common.Block) {
	for i := range consensusNodes {
		var lastDecision *common.Block
		require.Eventually(t, func() bool {
			select {
			case lastDecision = <-ledgerListeners[i].c:
				return lastDecision.Header.Number == expectedBlockNumber
			default:
				return false
			}
		}, 30*time.Second, 100*time.Millisecond)

		proposal, err := state.BytesToProposal(lastDecision.Data.Data[0])
		require.NotNil(t, proposal)
		require.NoError(t, err)
		header := &state.Header{}
		err = header.Deserialize(proposal.Header)
		require.NoError(t, err)
		lastConfigBlock = header.AvailableCommonBlocks[len(header.AvailableCommonBlocks)-1]
		require.True(t, protoutil.IsConfigBlock(lastConfigBlock))
		require.True(t, header.Num == header.DecisionNumOfLastConfigBlock)
	}
	return lastConfigBlock
}

func waitForRunningState(t *testing.T, consenter *consensus_node.Consensus, configSeq uint64) {
	require.Eventually(t, func() bool {
		status := consenter.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == configSeq
	}, 120*time.Second, 100*time.Millisecond)
}

func waitForPendingAdminState(t *testing.T, consenter *consensus_node.Consensus, configSeq uint64) {
	require.Eventually(t, func() bool {
		status := consenter.GetStatus()
		return status.State == node_utils.StatePendingAdmin && status.ConfigSequenceNumber == configSeq
	}, 120*time.Second, 100*time.Millisecond)
}
