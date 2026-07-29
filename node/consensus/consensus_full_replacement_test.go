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
// Starting with parties 1,2,3,4, the test loops 6 times.
// Each iteration i (1..6) adds party i+4 and then removes party i.
// Final state: parties 7,8,9,10
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

	t.Logf(">>> Initial setup complete with parties: %v", initialParties)

	lastBlockNumber := uint64(0) // Start with genesis block
	configSeq := types.ConfigSequence(0)

	// Initial router context uses party 1's certificate
	routerCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org1/orderers/party1/router/tls/server.crt"))
	require.NoError(t, err)
	block, _ := pem.Decode(routerCertBytes)
	require.NotNil(t, block)
	require.Equal(t, "CERTIFICATE", block.Type)
	routerCert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	routerCtx, err := createContextForSubmitConfig(routerCert)
	require.NoError(t, err)

	var lastConfigBlock *common.Block

	// Initial simple request to verify the network is working
	lastBlockNumber++
	t.Logf(">>> Sending initial request with parties: %v at block %d", initialParties, lastBlockNumber)
	pk1, err := loadBatcherPrivateKey(t, dir, 1)
	require.NoError(t, err)
	pk2, err := loadBatcherPrivateKey(t, dir, 2)
	require.NoError(t, err)
	sendSimpleRequest(t, consensusNodes, pk1, pk2, 1, 2, configSeq, lastBlockNumber, "")
	t.Logf(">>> Initial request committed at block %d", lastBlockNumber)

	// Each iteration i (1-based) adds party i+4 and removes party i.
	// After 6 iterations the active set goes from {1,2,3,4} to {7,8,9,10}.
	for i := 1; i <= 6; i++ {
		newParty := types.PartyID(i + 4)
		removeParty := types.PartyID(i)

		// Active parties at the start of this iteration: i, i+1, i+2, i+3.
		// signerA/B/C are the first three — a majority used to sign the add TX.
		signerA := types.PartyID(i)
		signerB := types.PartyID(i + 1)
		signerC := types.PartyID(i + 2)

		// ── Add new party ──────────────────────────────────────────────────────
		t.Run(fmt.Sprintf("Add party %d", newParty), func(t *testing.T) {
			t.Logf(">>> Adding party %d", newParty)

			var configBlockPath string
			if lastConfigBlock == nil {
				// First iteration: use the bootstrap block produced by armageddon
				configBlockPath = filepath.Join(dir, "bootstrap", "bootstrap.block")
			} else {
				configBlockStoreDir := t.TempDir()
				configBlockPath = filepath.Join(configBlockStoreDir, "config.block")
				require.NoError(t, configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath))
			}

			configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
			addedPartyID, addedNetInfo := configUpdateBuilder.PrepareAndAddNewParty(t, dir)
			require.NotNil(t, addedNetInfo)
			require.Equal(t, newParty, addedPartyID, "Unexpected added party ID")

			configUpdatePbData := configUpdateBuilder.ConfigUpdatePBData(t)
			env := configutil.CreateConfigTX(t, dir, []types.PartyID{signerA, signerB, signerC}, int(signerA), configUpdatePbData)
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
			t.Logf(">>> Checking for config block at block number %d", lastBlockNumber)
			lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
			require.NotNil(t, lastConfigBlock)

			// Write new config block so the joining node can bootstrap from it
			newConfigBlockPath := filepath.Join(t.TempDir(), "config.block")
			require.NoError(t, configtxgen.WriteOutputBlock(lastConfigBlock, newConfigBlockPath))

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

			t.Logf(">>> Successfully added party %d", addedPartyID)

			// Verify with a simple request using two of the current signers
			pkA, loadErr := loadBatcherPrivateKey(t, dir, signerA)
			require.NoError(t, loadErr)
			pkB, loadErr := loadBatcherPrivateKey(t, dir, signerB)
			require.NoError(t, loadErr)
			lastBlockNumber++
			t.Logf(">>> Sending sync request after adding party %d at block %d", addedPartyID, lastBlockNumber)
			sendSimpleRequest(t, consensusNodes, pkA, pkB, signerA, signerB, configSeq, lastBlockNumber, "")
		})

		// ── Remove old party ───────────────────────────────────────────────────
		t.Run(fmt.Sprintf("Remove party %d", removeParty), func(t *testing.T) {
			t.Logf(">>> Removing party %d", removeParty)
			require.NotNil(t, lastConfigBlock, "lastConfigBlock should not be nil before removal")

			configBlockStoreDir := t.TempDir()
			configBlockPath := filepath.Join(configBlockStoreDir, "config.block")
			require.NoError(t, configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath))

			configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
			configUpdatePbData := configUpdateBuilder.RemoveParty(t, removeParty)

			// Sign with signerB, signerC, and newParty (the three that remain after removal)
			env := configutil.CreateConfigTX(t, dir, []types.PartyID{signerB, signerC, newParty}, int(signerB), configUpdatePbData)
			configReq := &protos.Request{
				Payload:   env.Payload,
				Signature: env.Signature,
				ConfigSeq: uint32(configSeq),
			}

			_, err = consensusNodes[0].SubmitConfig(routerCtx, configReq)
			require.NoError(t, err)

			// Wait for the removed party to enter pending-admin state, then stop it
			for _, consenter := range consensusNodes {
				if consenter.GetPartyID() == removeParty {
					waitForPendingAdminState(t, consenter, uint64(configSeq))
					consenter.Stop()
					break
				}
			}

			// Remove the stopped node from the slice
			filtered := make([]*consensus_node.Consensus, 0, len(consensusNodes)-1)
			for _, cn := range consensusNodes {
				if cn.PartyID != removeParty {
					filtered = append(filtered, cn)
				}
			}
			consensusNodes = filtered

			configSeq++
			waitForRunningStateMultiNodes(t, consensusNodes, uint64(configSeq))

			lastBlockNumber++
			t.Logf(">>> Checking for config block at block number %d", lastBlockNumber)
			lastConfigBlock = makeSureConfigBlockCommitted(t, consensusNodes, lastBlockNumber)
			require.NotNil(t, lastConfigBlock)

			t.Logf(">>> Successfully removed party %d", removeParty)

			// Verify with two of the remaining parties
			pkB, loadErr := loadBatcherPrivateKey(t, dir, signerB)
			require.NoError(t, loadErr)
			pkC, loadErr := loadBatcherPrivateKey(t, dir, signerC)
			require.NoError(t, loadErr)
			lastBlockNumber++
			t.Logf(">>> Sending sync request after removing party %d at block %d", removeParty, lastBlockNumber)
			sendSimpleRequest(t, consensusNodes, pkB, pkC, signerB, signerC, configSeq, lastBlockNumber, "")

			// Update router context to signerB's certificate for the next iteration
			certPath := filepath.Join(dir, fmt.Sprintf(
				"crypto/ordererOrganizations/org%d/orderers/party%d/router/tls/server.crt",
				signerB, signerB,
			))
			certBytes, readErr := os.ReadFile(certPath)
			require.NoError(t, readErr)
			blk, _ := pem.Decode(certBytes)
			require.NotNil(t, blk)
			cert, parseErr := x509.ParseCertificate(blk.Bytes)
			require.NoError(t, parseErr)
			routerCtx, err = createContextForSubmitConfig(cert)
			require.NoError(t, err)
		})
	}

	// Final verification: active set should be {7,8,9,10}
	require.Equal(t, 4, len(consensusNodes), "Final consensus node count mismatch")

	actualPartyIDs := make([]types.PartyID, 0, len(consensusNodes))
	for _, node := range consensusNodes {
		actualPartyIDs = append(actualPartyIDs, node.GetPartyID())
	}

	for _, expected := range []types.PartyID{7, 8, 9, 10} {
		require.Contains(t, actualPartyIDs, expected, "Party %d should be present", expected)
	}
	for _, removed := range []types.PartyID{1, 2, 3, 4, 5, 6} {
		require.NotContains(t, actualPartyIDs, removed, "Party %d should have been removed", removed)
	}

	t.Logf(">>> Complete replacement! Started with parties 1,2,3,4 and ended with parties %v", actualPartyIDs)

	// Cleanup
	for _, cn := range consensusNodes {
		cn.Stop()
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
