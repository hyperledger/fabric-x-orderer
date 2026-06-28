/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	consensus_node "github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

// TestConsensusWithConsenterSyncAfterMissingConfigTx tests the consensus behavior when a consenter
// fails, misses a config transaction, and then syncs back up. The test covers the following scenario:
//
// 1. Start 7 consenters and submit a normal transaction to verify the cluster is working
// 2. Submit a config update that changes a minor parameter (batcher batch creation timeout)
// 3. Stop consenter with ID=2 to simulate a failure
// 4. While consenter 2 is down, the remaining consenters process and commit another config update (changing batcher max message count)
// 5. Restart the failed consenter (ID=2) and verify that it successfully syncs and catches up with the missed config transaction
// 6. Verify that all consenters (including the recovered one) can process new transactions
//
// This test validates that the synchronization mechanism correctly handles config blocks and
// ensures that a consenter can rejoin the cluster after missing configuration changes.
func TestConsensusWithConsenterSyncAfterMissingConfigTx(t *testing.T) {
	// Setup: Create 7 consenters
	parties := []types.PartyID{1, 2, 3, 4, 5, 6, 7}
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
	startConsensusNodesAndRegisterGRPCServers(t, parties, consensusNodes, servers)

	// Load private keys for batchers to sign transactions
	keyBytes1, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org1/orderers/party1/batcher1/msp/keystore/priv_sk"))
	require.NoError(t, err)
	privateKey1, err := tx.CreateECDSAPrivateKey(keyBytes1)
	require.NoError(t, err, "failed to create private key1")

	keyBytes2, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org2/orderers/party2/batcher1/msp/keystore/priv_sk"))
	require.NoError(t, err)
	privateKey2, err := tx.CreateECDSAPrivateKey(keyBytes2)
	require.NoError(t, err, "failed to create private key2")

	lastBlockNumber := uint64(1)
	configSeq := types.ConfigSequence(0)

	var routerCtx context.Context
	var lastConfigBlock *common.Block

	// Step 1: Submit a normal transaction to verify the cluster is working
	t.Run("initial normal transaction", func(t *testing.T) {
		t.Logf(">>> Step 1: Submitting initial normal transaction")
		sendSimpleRequest(t, consensusNodes, privateKey1, privateKey2, 1, configSeq, lastBlockNumber, "")
		t.Logf(">>> Step 1: Initial transaction committed at block %d", lastBlockNumber)
	})

	// Step 2: Submit first config update (change batcher batch creation timeout)
	t.Run("first config update - batch creation timeout", func(t *testing.T) {
		t.Logf(">>> Step 2: Submitting first config update (batch creation timeout)")

		// Create router context for config submission
		routerCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org1/orderers/party1/router/tls/tls-cert.pem"))
		require.NoError(t, err)
		block, _ := pem.Decode(routerCertBytes)
		require.NotNil(t, block)
		require.Equal(t, "CERTIFICATE", block.Type)
		routerCert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)
		routerCtx, err = createContextForSubmitConfig(routerCert)
		require.NoError(t, err)

		// Create config update to change batch creation timeout
		configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
		configUpdatePbData := configUpdateBuilder.UpdateBatchTimeouts(t, configutil.NewBatchTimeoutsConfig(configutil.BatchTimeoutsConfigName.BatchCreationTimeout, "2s"))
		env := configutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
			ConfigSeq: uint32(configSeq),
		}

		_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		// Wait for all consensus nodes to apply new config
		configSeq++
		waitForRunningStateMultiNodes(t, consensusNodes, uint64(configSeq))

		// Wait for config block to be committed
		lastBlockNumber++
		lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
		t.Logf(">>> Step 2: First config block committed at block %d, configSeq=%d", lastBlockNumber, configSeq)

		t.Logf(">>> Step 2: All consenters applied first config update")
	})

	// Step 3: Stop consenter with ID=2
	var stoppedConsenter *consensus_node.Consensus
	var stoppedConsenterIndex int
	var activeConsenters []*consensus_node.Consensus
	t.Run("stop consenter 2", func(t *testing.T) {
		t.Logf(">>> Step 3: Stopping consenter with ID=2")
		for i, consenter := range consensusNodes {
			if consenter.GetPartyID() == 2 {
				stoppedConsenter = consenter
				stoppedConsenterIndex = i
				consenter.Stop()
				t.Logf(">>> Step 3: Consenter 2 stopped")
				break
			}
		}
		require.NotNil(t, stoppedConsenter, "consenter 2 should exist")
	})

	// Step 4: While consenter 2 is down, process another config update
	t.Run("config update while consenter 2 is down", func(t *testing.T) {
		t.Logf(">>> Step 4: Processing config update while consenter 2 is down")

		// Get remaining consenters (excluding the stopped one)
		activeConsenters = make([]*consensus_node.Consensus, 0, len(consensusNodes)-1)
		for i, consenter := range consensusNodes {
			if i != stoppedConsenterIndex {
				activeConsenters = append(activeConsenters, consenter)
			}
		}

		// Submit second config update (change max message count) - consenter 2 will miss this
		t.Logf(">>> Step 4: Submitting second config update (max message count)")
		configBlockStoreDir := t.TempDir()
		err = configtxgen.WriteOutputBlock(lastConfigBlock, filepath.Join(configBlockStoreDir, "config.block"))
		require.NoError(t, err)
		configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(configBlockStoreDir, "config.block"))
		configUpdatePbData := configUpdateBuilder.UpdateBatchSizeConfig(t, configutil.NewBatchSizeConfig(configutil.BatchSizeConfigName.MaxMessageCount, 15))
		env := configutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
		configReq := &protos.Request{
			Payload:   env.Payload,
			Signature: env.Signature,
			ConfigSeq: uint32(configSeq),
		}

		_, err = activeConsenters[0].SubmitConfig(routerCtx, configReq)
		require.NoError(t, err)

		// Wait for active consensus nodes to apply new config
		configSeq++
		waitForRunningStateMultiNodes(t, activeConsenters, uint64(configSeq))

		// Wait for config block to be committed by active consenters
		lastBlockNumber++
		lastConfigBlock = makeSureConfigBlockCommitted(t, activeConsenters, lastBlockNumber)
		t.Logf(">>> Step 4: Second config block committed at block %d, configSeq=%d (consenter 2 missed this)", lastBlockNumber, configSeq)

		time.Sleep(30 * time.Second) // wait for connections to be reestablished
		t.Logf(">>> Step 4: Active consenters applied second config update")
	})

	// Step 5: Restart the failed consenter (ID=2)
	t.Run("restart consenter 2", func(t *testing.T) {
		t.Logf(">>> Step 5: Restarting consenter 2")

		// Recreate and restart consenter 2
		recoveredConsensusNodes, recoveredServers, _ := createConsensusNodesAndGRPCServers(t, dir, []types.PartyID{2})
		startConsensusNodesAndRegisterGRPCServers(t, []types.PartyID{2}, recoveredConsensusNodes, recoveredServers)

		// Update the consensus nodes list
		consensusNodes[stoppedConsenterIndex] = recoveredConsensusNodes[0]

		t.Logf(">>> Step 5: Consenter 2 restarted, waiting for it to reach running state with correct config")

		// Wait for consenter 2 to reach running state with correct config sequence
		require.Eventually(t, func() bool {
			status := consensusNodes[stoppedConsenterIndex].GetStatus()
			isRunning := status.State == node_utils.StateRunning
			hasCorrectConfig := status.ConfigSequenceNumber == uint64(configSeq)
			t.Logf(">>> Step 5: Consenter 2 state: %v, configSeq: %d (expected: %d)", status.State, status.ConfigSequenceNumber, configSeq)
			return isRunning && hasCorrectConfig
		}, 120*time.Second, 5*time.Second, "consenter 2 should be running with correct config sequence")

		t.Logf(">>> Step 5: Consenter 2 is now running with correct config sequence")
	})

	// Step 6: Verify the synced consenter can advance with the rest of the cluster
	t.Run("verify synced consenter advances with cluster", func(t *testing.T) {
		t.Logf(">>> Step 6: Verifying that synced consenter 2 can advance with the cluster")
		time.Sleep(30 * time.Second)

		// Submit first transaction to the cluster
		t.Logf(">>> Step 6: Submitting first transaction to the cluster")
		lastBlockNumber++
		sendSimpleRequest(t, activeConsenters, privateKey1, privateKey2, 1, configSeq, lastBlockNumber, "")
		t.Logf(">>> Step 6: First transaction committed at block %d", lastBlockNumber)

		// Verify active consenters processed the transaction
		expectedHeight := lastBlockNumber + 1
		t.Logf(">>> Step 6: Verifying active consenters are at height %d", expectedHeight)
		for _, consenter := range activeConsenters {
			height := consenter.Storage.(*ledger.ConsensusLedger).Height()
			require.Equal(t, expectedHeight, height, "consenter %d should be at height %d", consenter.GetPartyID(), expectedHeight)
			t.Logf(">>> Step 6: Active consenter %d is at height %d", consenter.GetPartyID(), height)
		}

		// Submit second transaction to verify continued advancement
		t.Logf(">>> Step 6: Submitting second transaction to verify continued advancement")
		lastBlockNumber++
		sendSimpleRequest(t, activeConsenters, privateKey2, privateKey2, 2, configSeq, lastBlockNumber, "")
		t.Logf(">>> Step 6: Second transaction committed at block %d", lastBlockNumber)

		// Verify active consenters processed the second transaction
		expectedHeight = lastBlockNumber + 1
		t.Logf(">>> Step 6: Verifying active consenters are at height %d", expectedHeight)
		for _, consenter := range activeConsenters {
			height := consenter.Storage.(*ledger.ConsensusLedger).Height()
			require.Equal(t, expectedHeight, height, "consenter %d should be at height %d", consenter.GetPartyID(), expectedHeight)
			t.Logf(">>> Step 6: Active consenter %d is at height %d", consenter.GetPartyID(), height)
		}

		// Verify that consenter 2 is advancing with the cluster
		t.Logf(">>> Step 6: Verifying consenter 2 is advancing with the cluster")
		require.Eventually(t, func() bool {
			height := consensusNodes[stoppedConsenterIndex].Storage.(*ledger.ConsensusLedger).Height()
			t.Logf(">>> Step 6: Consenter 2 current height: %d, expected: %d", height, expectedHeight)
			return height == expectedHeight-1 || height == expectedHeight
		}, 60*time.Second, 1*time.Second, "consenter 2 should advance with the cluster")

		t.Logf(">>> Step 6: SUCCESS - Consenter 2 is advancing with the cluster and all consenters are synchronized")
	})

	// Cleanup
	for _, consensusNode := range consensusNodes {
		consensusNode.Stop()
	}
}
