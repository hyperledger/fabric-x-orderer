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
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	consensus_node "github.com/hyperledger/fabric-x-orderer/node/consensus"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

// TestConsensusFullReplacement tests the consensus behavior when parties are replaced.
// Starting with parties 1,2,3,4, the test performs the following sequence:
// 1. Send a simple request to verify initial state
// 2. Add party 5, then remove party 1
// 3. Add party 6, then remove party 2
// 4. Add party 7, then remove party 3
// 5. Add party 8, then remove party 4
// 6. Add party 9
// Final state: parties 5,6,7,8,9 (complete replacement of all original parties plus one additional)
func TestConsensusFullReplacement(t *testing.T) {
	// Start with parties 1,2,3,4 only
	initialParties := []types.PartyID{1, 2, 3, 4}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")

	// Create network with only 4 parties initially
	netInfo := testutil.CreateNetworkWithPortAllocator(t, configPath, len(initialParties), numOfShards, "TLS", "none", testutil.SharedTestPortAllocator())
	require.NotNil(t, netInfo)

	nodesIPs := testutil.GetNodesIPsFromNetInfo(netInfo)
	require.NotNil(t, nodesIPs)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStoreAndMonitoringPort(t, dir, netInfo)

	netInfo.CleanUp()

	// Start only with the initial 4 parties
	consensusNodes, servers, _ := createConsensusNodesAndGRPCServers(t, dir, initialParties)
	startConsensusNodesAndRegisterGRPCServers(t, initialParties, consensusNodes, servers)

	// Load private keys for batchers of initial parties
	privateKey1, err := loadBatcherPrivateKey(t, dir, 1)
	require.NoError(t, err)
	privateKey2, err := loadBatcherPrivateKey(t, dir, 2)
	require.NoError(t, err)
	privateKey3, err := loadBatcherPrivateKey(t, dir, 3)
	require.NoError(t, err)
	privateKey4, err := loadBatcherPrivateKey(t, dir, 4)
	require.NoError(t, err)
	var privateKey5 *ecdsa.PrivateKey

	t.Logf(">>> Initial setup complete with parties: %v", initialParties)

	lastBlockNumber := uint64(0) // Start with genesis block
	configSeq := types.ConfigSequence(0)
	activeParties := []types.PartyID{1, 2, 3, 4}

	// Initial simple request to verify the network is working
	lastBlockNumber++ // Increment to 1 for the first request
	t.Logf(">>> Sending initial request with parties: %v at block %d", activeParties, lastBlockNumber)
	sendSimpleRequest(t, consensusNodes, privateKey1, privateKey2, 1, 2, configSeq, lastBlockNumber, "")
	t.Logf(">>> Initial request committed at block %d", lastBlockNumber)

	// Create router context for config submissions
	routerCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org1/orderers/party1/router/tls/tls-cert.pem"))
	require.NoError(t, err)
	block, _ := pem.Decode(routerCertBytes)
	require.NotNil(t, block)
	require.Equal(t, "CERTIFICATE", block.Type)
	routerCert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	routerCtx, err := createContextForSubmitConfig(routerCert)
	require.NoError(t, err)

	var lastConfigBlock *common.Block

	// Step 1: Add party 5
	t.Run("Add party 5", func(t *testing.T) {
		t.Logf(">>> Adding party 5 to parties: %v", activeParties)

		// Create config update to add party 5
		configBlockPath := filepath.Join(dir, "bootstrap", "bootstrap.block")
		configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)

		// Add the new party to the configuration
		addedPartyID, addedNetInfo := configUpdateBuilder.PrepareAndAddNewParty(t, dir)
		require.NotNil(t, addedNetInfo)
		require.Equal(t, types.PartyID(5), addedPartyID, "Expected to add party 5")
		t.Logf(">>> Added party ID: %d", addedPartyID)

		// Create and sign the config transaction
		configUpdatePbData := configUpdateBuilder.ConfigUpdatePBData(t)
		// Use parties 1,2,3 for signing (majority of 4)
		env := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2, 3}, 1, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
			ConfigSeq: uint32(configSeq),
		}

		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		// Wait for existing consensus nodes to apply new config
		configSeq++
		waitForRunningStateMultiNodes(t, consensusNodes, uint64(configSeq))

		// Config block will be at the next block number
		lastBlockNumber++
		t.Logf(">>> Checking for config block at block number %d", lastBlockNumber)
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		// Add the added party to active parties
		activeParties = append(activeParties, addedPartyID)

		// Write the last config block to a file so the new party can read it
		newConfigBlockPath := filepath.Join(t.TempDir(), "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, newConfigBlockPath)
		require.NoError(t, err)

		// Update the config block path in the net info of the added party
		for _, netNode := range addedNetInfo {
			netNode.ConfigBlockPath = newConfigBlockPath
		}

		// Update file store and monitoring port for the new party
		updateFileStoreAndMonitoringPort(t, dir, addedNetInfo)

		// Close the listeners allocated by ExtendNetwork
		for _, nodeInfo := range addedNetInfo {
			nodeInfo.Close()
		}

		// Start the new consensus node
		newConsensusNode, newConsensusNodeServer, _ := createConsensusNodesAndGRPCServers(t, dir, []types.PartyID{addedPartyID})
		startConsensusNodesAndRegisterGRPCServers(t, []types.PartyID{addedPartyID}, newConsensusNode, newConsensusNodeServer)
		waitForRunningState(t, newConsensusNode[0], uint64(configSeq))

		// Add the new consensus node to the list
		consensusNodes = append(consensusNodes, newConsensusNode[0])

		// Wait for connections to be reestablished
		time.Sleep(30 * time.Second)

		t.Logf(">>> Successfully added party %d. Active parties: %v", addedPartyID, activeParties)

		// Send a simple request to verify all parties are in sync
		lastBlockNumber++ // Increment before the request
		t.Logf(">>> Sending sync request after adding party %d at block %d", addedPartyID, lastBlockNumber)
		sendSimpleRequest(t, consensusNodes, privateKey1, privateKey2, 1, 2, configSeq, lastBlockNumber, "")
	})

	// Step 2: Remove party 1
	t.Run("Remove party 1", func(t *testing.T) {
		t.Logf(">>> Removing party 1 from parties: %v", activeParties)
		require.NotNil(t, lastConfigBlock, "lastConfigBlock should not be nil")

		// Create config update to remove party 1
		configBlockStoreDir := t.TempDir()
		configBlockPath := filepath.Join(configBlockStoreDir, "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath)
		require.NoError(t, err)

		configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		configUpdatePbData := configUpdateBuilder.RemoveParty(t, 1)

		// Sign with parties 2,3,4 (majority of remaining parties)
		env := configutil.CreateConfigTX(t, dir, []types.PartyID{2, 3, 4}, 2, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
			ConfigSeq: uint32(configSeq),
		}

		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		// Wait for party 1 to enter pending admin state and stop it
		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() == 1 {
				waitForPendingAdminState(t, consenter, uint64(configSeq))
				consenter.Stop()
				break
			}
		}

		// Update consensus nodes list (remove party 1)
		oldConsensusNodes := consensusNodes
		consensusNodes = make([]*consensus_node.Consensus, 0)
		for _, consensusNode := range oldConsensusNodes {
			if consensusNode.PartyID != 1 {
				consensusNodes = append(consensusNodes, consensusNode)
			}
		}

		// Update active parties list
		activeParties = []types.PartyID{2, 3, 4, 5}

		configSeq++
		// Wait for the rest of consensus nodes to apply new config
		waitForRunningStateMultiNodes(t, consensusNodes, uint64(configSeq))

		// Config block will be at the next block number
		lastBlockNumber++
		t.Logf(">>> Checking for config block at block number %d", lastBlockNumber)
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		t.Logf(">>> Successfully removed party 1. Active parties: %v", activeParties)

		// Send a simple request to verify remaining parties are in sync
		// Use party 2's private key since party 1 is removed
		lastBlockNumber++ // Increment before the request
		t.Logf(">>> Sending sync request after removing party 1 at block %d", lastBlockNumber)
		sendSimpleRequest(t, consensusNodes, privateKey2, privateKey3, 2, 3, configSeq, lastBlockNumber, "")

		// Create new router context using party 2's certificate since party 1 is removed
		routerCertBytes2, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org2/orderers/party2/router/tls/tls-cert.pem"))
		require.NoError(t, err)
		block2, _ := pem.Decode(routerCertBytes2)
		require.NotNil(t, block2)
		require.Equal(t, "CERTIFICATE", block2.Type)
		routerCert2, err := x509.ParseCertificate(block2.Bytes)
		require.NoError(t, err)
		routerCtx, err = createContextForSubmitConfig(routerCert2)
		require.NoError(t, err)
	})

	// Step 3: Add party 6
	t.Run("Add party 6", func(t *testing.T) {
		t.Logf(">>> Adding party 6 to parties: %v", activeParties)

		configBlockStoreDir := t.TempDir()
		configBlockPath := filepath.Join(configBlockStoreDir, "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath)
		require.NoError(t, err)

		configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		addedPartyID, addedNetInfo := configUpdateBuilder.PrepareAndAddNewParty(t, dir)
		require.NotNil(t, addedNetInfo)
		require.Equal(t, types.PartyID(6), addedPartyID, "Expected to add party 6")

		configUpdatePbData := configUpdateBuilder.ConfigUpdatePBData(t)
		env := configutil.CreateConfigTX(t, dir, []types.PartyID{2, 3, 4}, 2, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
			ConfigSeq: uint32(configSeq),
		}

		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		configSeq++
		waitForRunningStateMultiNodes(t, consensusNodes, uint64(configSeq))

		lastBlockNumber++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		activeParties = append(activeParties, addedPartyID)

		newConfigBlockPath := filepath.Join(t.TempDir(), "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, newConfigBlockPath)
		require.NoError(t, err)

		for _, netNode := range addedNetInfo {
			netNode.ConfigBlockPath = newConfigBlockPath
		}

		updateFileStoreAndMonitoringPort(t, dir, addedNetInfo)

		for _, nodeInfo := range addedNetInfo {
			nodeInfo.Close()
		}

		newConsensusNode, newConsensusNodeServer, _ := createConsensusNodesAndGRPCServers(t, dir, []types.PartyID{addedPartyID})
		startConsensusNodesAndRegisterGRPCServers(t, []types.PartyID{addedPartyID}, newConsensusNode, newConsensusNodeServer)
		waitForRunningState(t, newConsensusNode[0], uint64(configSeq))

		consensusNodes = append(consensusNodes, newConsensusNode[0])
		time.Sleep(30 * time.Second)

		t.Logf(">>> Successfully added party %d. Active parties: %v", addedPartyID, activeParties)

		lastBlockNumber++
		sendSimpleRequest(t, consensusNodes, privateKey2, privateKey3, 2, 3, configSeq, lastBlockNumber, "")
	})

	// Step 4: Remove party 2
	t.Run("Remove party 2", func(t *testing.T) {
		t.Logf(">>> Removing party 2 from parties: %v", activeParties)

		configBlockStoreDir := t.TempDir()
		configBlockPath := filepath.Join(configBlockStoreDir, "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath)
		require.NoError(t, err)

		configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		configUpdatePbData := configUpdateBuilder.RemoveParty(t, 2)

		env := configutil.CreateConfigTX(t, dir, []types.PartyID{3, 4, 5}, 3, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
			ConfigSeq: uint32(configSeq),
		}

		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() == 2 {
				waitForPendingAdminState(t, consenter, uint64(configSeq))
				consenter.Stop()
				break
			}
		}

		oldConsensusNodes := consensusNodes
		consensusNodes = make([]*consensus_node.Consensus, 0)
		for _, consensusNode := range oldConsensusNodes {
			if consensusNode.PartyID != 2 {
				consensusNodes = append(consensusNodes, consensusNode)
			}
		}

		activeParties = []types.PartyID{3, 4, 5, 6}

		configSeq++
		waitForRunningStateMultiNodes(t, consensusNodes, uint64(configSeq))

		lastBlockNumber++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		t.Logf(">>> Successfully removed party 2. Active parties: %v", activeParties)

		lastBlockNumber++
		sendSimpleRequest(t, consensusNodes, privateKey3, privateKey4, 3, 4, configSeq, lastBlockNumber, "")

		// Create new router context using party 3's certificate for next steps
		routerCertBytes3, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org3/orderers/party3/router/tls/tls-cert.pem"))
		require.NoError(t, err)
		block3, _ := pem.Decode(routerCertBytes3)
		require.NotNil(t, block3)
		require.Equal(t, "CERTIFICATE", block3.Type)
		routerCert3, err := x509.ParseCertificate(block3.Bytes)
		require.NoError(t, err)
		routerCtx, err = createContextForSubmitConfig(routerCert3)
		require.NoError(t, err)
	})

	// Step 5: Add party 7
	t.Run("Add party 7", func(t *testing.T) {
		t.Logf(">>> Adding party 7 to parties: %v", activeParties)

		configBlockStoreDir := t.TempDir()
		configBlockPath := filepath.Join(configBlockStoreDir, "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath)
		require.NoError(t, err)

		configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		addedPartyID, addedNetInfo := configUpdateBuilder.PrepareAndAddNewParty(t, dir)
		require.NotNil(t, addedNetInfo)
		require.Equal(t, types.PartyID(7), addedPartyID, "Expected to add party 7")

		configUpdatePbData := configUpdateBuilder.ConfigUpdatePBData(t)
		env := configutil.CreateConfigTX(t, dir, []types.PartyID{3, 4, 5}, 3, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
			ConfigSeq: uint32(configSeq),
		}

		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		configSeq++
		waitForRunningStateMultiNodes(t, consensusNodes, uint64(configSeq))

		lastBlockNumber++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		activeParties = append(activeParties, addedPartyID)

		newConfigBlockPath := filepath.Join(t.TempDir(), "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, newConfigBlockPath)
		require.NoError(t, err)

		for _, netNode := range addedNetInfo {
			netNode.ConfigBlockPath = newConfigBlockPath
		}

		updateFileStoreAndMonitoringPort(t, dir, addedNetInfo)

		for _, nodeInfo := range addedNetInfo {
			nodeInfo.Close()
		}

		newConsensusNode, newConsensusNodeServer, _ := createConsensusNodesAndGRPCServers(t, dir, []types.PartyID{addedPartyID})
		startConsensusNodesAndRegisterGRPCServers(t, []types.PartyID{addedPartyID}, newConsensusNode, newConsensusNodeServer)
		waitForRunningState(t, newConsensusNode[0], uint64(configSeq))

		consensusNodes = append(consensusNodes, newConsensusNode[0])
		time.Sleep(30 * time.Second)

		t.Logf(">>> Successfully added party %d. Active parties: %v", addedPartyID, activeParties)

		lastBlockNumber++
		sendSimpleRequest(t, consensusNodes, privateKey3, privateKey4, 3, 4, configSeq, lastBlockNumber, "")
	})

	// Step 6: Remove party 3
	t.Run("Remove party 3", func(t *testing.T) {
		t.Logf(">>> Removing party 3 from parties: %v", activeParties)

		configBlockStoreDir := t.TempDir()
		configBlockPath := filepath.Join(configBlockStoreDir, "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath)
		require.NoError(t, err)

		configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		configUpdatePbData := configUpdateBuilder.RemoveParty(t, 3)

		env := configutil.CreateConfigTX(t, dir, []types.PartyID{4, 5, 6}, 4, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
			ConfigSeq: uint32(configSeq),
		}

		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() == 3 {
				waitForPendingAdminState(t, consenter, uint64(configSeq))
				consenter.Stop()
				break
			}
		}

		oldConsensusNodes := consensusNodes
		consensusNodes = make([]*consensus_node.Consensus, 0)
		for _, consensusNode := range oldConsensusNodes {
			if consensusNode.PartyID != 3 {
				consensusNodes = append(consensusNodes, consensusNode)
			}
		}

		activeParties = []types.PartyID{4, 5, 6, 7}

		configSeq++
		waitForRunningStateMultiNodes(t, consensusNodes, uint64(configSeq))

		lastBlockNumber++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		t.Logf(">>> Successfully removed party 3. Active parties: %v", activeParties)

		lastBlockNumber++
		privateKey5, err = loadBatcherPrivateKey(t, dir, 5)
		require.NoError(t, err)
		sendSimpleRequest(t, consensusNodes, privateKey4, privateKey5, 4, 5, configSeq, lastBlockNumber, "")
	})

	// Step 7: Add party 8
	t.Run("Add party 8", func(t *testing.T) {
		t.Logf(">>> Adding party 8 to parties: %v", activeParties)

		configBlockStoreDir := t.TempDir()
		configBlockPath := filepath.Join(configBlockStoreDir, "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath)
		require.NoError(t, err)

		configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		addedPartyID, addedNetInfo := configUpdateBuilder.PrepareAndAddNewParty(t, dir)
		require.NotNil(t, addedNetInfo)
		require.Equal(t, types.PartyID(8), addedPartyID, "Expected to add party 8")

		configUpdatePbData := configUpdateBuilder.ConfigUpdatePBData(t)
		env := configutil.CreateConfigTX(t, dir, []types.PartyID{4, 5, 6}, 4, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
			ConfigSeq: uint32(configSeq),
		}

		// Create new router context using party 4's certificate
		routerCertBytes4, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org4/orderers/party4/router/tls/tls-cert.pem"))
		require.NoError(t, err)
		block4, _ := pem.Decode(routerCertBytes4)
		require.NotNil(t, block4)
		require.Equal(t, "CERTIFICATE", block4.Type)
		routerCert4, err := x509.ParseCertificate(block4.Bytes)
		require.NoError(t, err)
		routerCtx, err = createContextForSubmitConfig(routerCert4)
		require.NoError(t, err)

		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		configSeq++
		waitForRunningStateMultiNodes(t, consensusNodes, uint64(configSeq))

		lastBlockNumber++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		activeParties = append(activeParties, addedPartyID)

		newConfigBlockPath := filepath.Join(t.TempDir(), "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, newConfigBlockPath)
		require.NoError(t, err)

		for _, netNode := range addedNetInfo {
			netNode.ConfigBlockPath = newConfigBlockPath
		}

		updateFileStoreAndMonitoringPort(t, dir, addedNetInfo)

		for _, nodeInfo := range addedNetInfo {
			nodeInfo.Close()
		}

		newConsensusNode, newConsensusNodeServer, _ := createConsensusNodesAndGRPCServers(t, dir, []types.PartyID{addedPartyID})
		startConsensusNodesAndRegisterGRPCServers(t, []types.PartyID{addedPartyID}, newConsensusNode, newConsensusNodeServer)
		waitForRunningState(t, newConsensusNode[0], uint64(configSeq))

		consensusNodes = append(consensusNodes, newConsensusNode[0])
		time.Sleep(30 * time.Second)

		t.Logf(">>> Successfully added party %d. Active parties: %v", addedPartyID, activeParties)

		lastBlockNumber++
		sendSimpleRequest(t, consensusNodes, privateKey4, privateKey5, 4, 5, configSeq, lastBlockNumber, "")
	})

	// Step 8: Remove party 4
	t.Run("Remove party 4", func(t *testing.T) {
		t.Logf(">>> Removing party 4 from parties: %v", activeParties)

		configBlockStoreDir := t.TempDir()
		configBlockPath := filepath.Join(configBlockStoreDir, "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath)
		require.NoError(t, err)

		configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		configUpdatePbData := configUpdateBuilder.RemoveParty(t, 4)

		// Sign with parties 5,6,7 (majority of remaining parties)
		env := configutil.CreateConfigTX(t, dir, []types.PartyID{5, 6, 7}, 5, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
			ConfigSeq: uint32(configSeq),
		}

		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		// Wait for party 4 to enter pending admin state and stop it
		for _, consenter := range consensusNodes {
			if consenter.GetPartyID() == 4 {
				waitForPendingAdminState(t, consenter, uint64(configSeq))
				consenter.Stop()
				break
			}
		}

		// Update consensus nodes list (remove party 4)
		oldConsensusNodes := consensusNodes
		consensusNodes = make([]*consensus_node.Consensus, 0)
		for _, consensusNode := range oldConsensusNodes {
			if consensusNode.PartyID != 4 {
				consensusNodes = append(consensusNodes, consensusNode)
			}
		}

		// Update active parties list
		activeParties = []types.PartyID{5, 6, 7, 8}

		configSeq++
		// Wait for the rest of consensus nodes to apply new config
		waitForRunningStateMultiNodes(t, consensusNodes, uint64(configSeq))

		// Config block will be at the next block number
		lastBlockNumber++
		t.Logf(">>> Checking for config block at block number %d", lastBlockNumber)
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		t.Logf(">>> Successfully removed party 4. Active parties: %v", activeParties)

		// Send a simple request to verify remaining parties are in sync
		// Use party 5's private key since party 4 is removed
		lastBlockNumber++ // Increment before the request
		t.Logf(">>> Sending sync request after removing party 4 at block %d", lastBlockNumber)
		sendSimpleRequest(t, consensusNodes, privateKey5, privateKey5, 5, 5, configSeq, lastBlockNumber, "")
	})

	// Step 9: Add party 9
	t.Run("Add party 9", func(t *testing.T) {
		t.Logf(">>> Adding party 9 to parties: %v", activeParties)

		configBlockStoreDir := t.TempDir()
		configBlockPath := filepath.Join(configBlockStoreDir, "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath)
		require.NoError(t, err)

		configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		addedPartyID, addedNetInfo := configUpdateBuilder.PrepareAndAddNewParty(t, dir)
		require.NotNil(t, addedNetInfo)
		require.Equal(t, types.PartyID(9), addedPartyID, "Expected to add party 9")

		configUpdatePbData := configUpdateBuilder.ConfigUpdatePBData(t)
		// Sign with parties 5,6,7 (majority of current parties)
		env := configutil.CreateConfigTX(t, dir, []types.PartyID{5, 6, 7}, 5, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
			ConfigSeq: uint32(configSeq),
		}

		// Create new router context using party 5's certificate
		routerCertBytes5, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org5/orderers/party5/router/tls/tls-cert.pem"))
		require.NoError(t, err)
		block5, _ := pem.Decode(routerCertBytes5)
		require.NotNil(t, block5)
		require.Equal(t, "CERTIFICATE", block5.Type)
		routerCert5, err := x509.ParseCertificate(block5.Bytes)
		require.NoError(t, err)
		routerCtx, err = createContextForSubmitConfig(routerCert5)
		require.NoError(t, err)

		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		configSeq++
		waitForRunningStateMultiNodes(t, consensusNodes, uint64(configSeq))

		lastBlockNumber++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
		require.NotNil(t, lastConfigBlock)

		activeParties = append(activeParties, addedPartyID)

		newConfigBlockPath := filepath.Join(t.TempDir(), "config.block")
		err = configtxgen.WriteOutputBlock(lastConfigBlock, newConfigBlockPath)
		require.NoError(t, err)

		for _, netNode := range addedNetInfo {
			netNode.ConfigBlockPath = newConfigBlockPath
		}

		updateFileStoreAndMonitoringPort(t, dir, addedNetInfo)

		for _, nodeInfo := range addedNetInfo {
			nodeInfo.Close()
		}

		newConsensusNode, newConsensusNodeServer, _ := createConsensusNodesAndGRPCServers(t, dir, []types.PartyID{addedPartyID})
		startConsensusNodesAndRegisterGRPCServers(t, []types.PartyID{addedPartyID}, newConsensusNode, newConsensusNodeServer)
		waitForRunningState(t, newConsensusNode[0], uint64(configSeq))

		consensusNodes = append(consensusNodes, newConsensusNode[0])
		time.Sleep(30 * time.Second)

		t.Logf(">>> Successfully added party %d. Active parties: %v", addedPartyID, activeParties)

		lastBlockNumber++
		privateKey6, err := loadBatcherPrivateKey(t, dir, 6)
		require.NoError(t, err)
		sendSimpleRequest(t, consensusNodes, privateKey5, privateKey6, 5, 6, configSeq, lastBlockNumber, "")
	})

	// Final verification - we should have 5 parties: 5,6,7,8,9 (complete replacement of original parties 1,2,3,4 plus one additional)
	// Verify against the actual consensusNodes slice, not the manually tracked activeParties
	require.Equal(t, 5, len(consensusNodes), "Final consensus node count mismatch")

	// Extract actual party IDs from consensusNodes
	actualPartyIDs := make([]types.PartyID, 0, len(consensusNodes))
	for _, node := range consensusNodes {
		actualPartyIDs = append(actualPartyIDs, node.GetPartyID())
	}

	require.Contains(t, actualPartyIDs, types.PartyID(5), "Party 5 should be present")
	require.Contains(t, actualPartyIDs, types.PartyID(6), "Party 6 should be present")
	require.Contains(t, actualPartyIDs, types.PartyID(7), "Party 7 should be present")
	require.Contains(t, actualPartyIDs, types.PartyID(8), "Party 8 should be present")
	require.Contains(t, actualPartyIDs, types.PartyID(9), "Party 9 should be present")
	require.NotContains(t, actualPartyIDs, types.PartyID(1), "Party 1 should be removed")
	require.NotContains(t, actualPartyIDs, types.PartyID(2), "Party 2 should be removed")
	require.NotContains(t, actualPartyIDs, types.PartyID(3), "Party 3 should be removed")
	require.NotContains(t, actualPartyIDs, types.PartyID(4), "Party 4 should be removed")

	t.Logf(">>> Complete replacement and expansion! Started with parties 1,2,3,4 and ended with parties %v (all original parties replaced, plus one additional)", actualPartyIDs)

	// Cleanup
	for _, consensusNode := range consensusNodes {
		consensusNode.Stop()
	}
}

// loadBatcherPrivateKey loads the private key for a batcher node
func loadBatcherPrivateKey(t *testing.T, dir string, partyID types.PartyID) (*ecdsa.PrivateKey, error) {
	keyBytes, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("crypto/ordererOrganizations/org%d/orderers/party%d/batcher1/msp/keystore/priv_sk", partyID, partyID)))
	if err != nil {
		return nil, err
	}
	privateKey, err := tx.CreateECDSAPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create private key for party %d: %w", partyID, err)
	}
	return privateKey, nil
}
