/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package membership

import (
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	test_utils "github.com/hyperledger/fabric-x-orderer/test/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestRemovePartyRunAll verifies that removing a party via a config update
// propagates to the running Arma network. It boots a temporary network,
// submits a config update to remove a specific party, waits for nodes to stop,
// validates the updated router config no longer includes the removed party,
// then restarts the removed party's nodes expecting failures, while confirming
// the remaining parties restart successfully.
func TestRemovePartyRunAll(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 5
	numOfShards := 2
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// Build Arma binary
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)
	require.NotNil(t, uc)

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	// Create config update to remove a party
	configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()

	partyToRemove := types.PartyID(2)
	configUpdatePbData := configUpdateBuilder.RemoveParty(t, partyToRemove)

	parties := make([]types.PartyID, 0, numOfParties)
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	remainingParties := make([]types.PartyID, 0, numOfParties-1)
	for i := 1; i <= numOfParties; i++ {
		if types.PartyID(i) != partyToRemove {
			parties = append(parties, types.PartyID(i))
		}
	}

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	// Wait for Arma nodes to soft stop
	testutil.WaitSoftStopped(t, netInfo)

	// Check that shared config of Router does not include the removed party
	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", submittingParty), "local_config_router.yaml")
	routerNodeConfig, _, err := config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)
	require.Equal(t, numOfParties-1, len(routerNodeConfig.SharedConfig.GetPartiesConfig()), "Party was not removed from the config")

	for _, partyConfig := range routerNodeConfig.SharedConfig.GetPartiesConfig() {
		require.NotEqual(t, partyToRemove, partyConfig.PartyID, "Removed party still exists in the config")
	}

	t.Log("Wait for the removed party to enter pending admin state and then stop the party")
	testutil.WaitForPendingAdminByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, []types.PartyID{partyToRemove})
	armaNetwork.StopParties([]types.PartyID{partyToRemove})

	t.Log("Wait for arma nodes to restart dynamically")
	testutil.WaitForRelaunchByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, remainingParties, 1)

	numOfNodesPerParty := 3 + numOfShards
	readyChan = make(chan string, (numOfParties-1)*numOfNodesPerParty)

	t.Log("Try to restart the removed party nodes, expect them to fail to start")
	armaNetwork.RestartParties(t, []types.PartyID{partyToRemove}, readyChan)
	defer armaNetwork.Stop()
	// TODO: improve the detection of failed nodes by checking specific exit codes,
	// rather than relying on string matching in the output
	// every node should report a panic during startup
	testutil.WaitPanic(t, readyChan, numOfNodesPerParty-1, 10)

	armaNetwork.StopParties(remainingParties)
}

// TestRemoveStoppedPartyThenRestart verifies the removal of a stopped party.
// A network of 5 nodes is initialized, then party 5 is stopped.
// A config tx removing this party is submitted.
// all Arma nodes, except those of the removed party, restart dynamically.
// When the removed party's nodes restarted, they fail to establish connections with the rest of the network.
func TestRemoveStoppedPartyThenRestart(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 5
	numOfShards := 1

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// Build Arma binary
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, types.PartyID(1))
	require.NoError(t, err)
	require.NotNil(t, uc)

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	totalTxNumber := 10

	// Send transactions to all parties to ensure network is operational before config update
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	org := fmt.Sprintf("org%d", 1)

	for i := range totalTxNumber {
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	partyToRemove := types.PartyID(5)

	// Stop the party to be removed
	armaNetwork.StopParties([]types.PartyID{partyToRemove})

	// Update user config to remove the party
	uc.RouterEndpoints = append(uc.RouterEndpoints[:partyToRemove-1], uc.RouterEndpoints[partyToRemove:]...)
	uc.AssemblerEndpoints = append(uc.AssemblerEndpoints[:partyToRemove-1], uc.AssemblerEndpoints[partyToRemove:]...)

	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

	// Send more transactions to all remaining parties to ensure network is still operational before config update
	for i := range totalTxNumber {
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	// Remove the party from netInfo
	for n := range netInfo {
		if netInfo[n].PartyId == partyToRemove {
			delete(netInfo, n)
		}
	}

	remainingParties := []types.PartyID{}
	for i := 1; i <= numOfParties; i++ {
		if types.PartyID(i) == partyToRemove {
			continue
		}
		remainingParties = append(remainingParties, types.PartyID(i))
	}

	// Create config update to remove a party
	configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()

	configUpdatePbData := configUpdateBuilder.RemoveParty(t, partyToRemove)

	// Submit config update
	submittingParty := 1
	env := configutil.CreateConfigTX(t, dir, remainingParties, submittingParty, configUpdatePbData)
	require.NotNil(t, env)
	for _, partyId := range remainingParties {
		// Send the config tx
		err = broadcastClient.SendTxTo(env, partyId)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	testutil.WaitForRelaunchByType(t, netInfo, []testutil.NodeType{testutil.Assembler}, 1)

	statusUnknown := common.Status_UNKNOWN
	// Pull blocks to verify all transactions are included
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      remainingParties,
		Transactions: totalTxNumber*2 + 1, // including config update tx
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      60,
		Status:       &statusUnknown,
		Signer:       signutil.CreateTestSigner(t, "org1", dir),
	})

	// Verify that the party is removed by checking the router's shared config
	for _, partyId := range remainingParties {
		routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyId), "local_config_router.yaml")
		routerNodeConfig, _, err := config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
		require.NoError(t, err)
		require.Equal(t, numOfParties-1, len(routerNodeConfig.SharedConfig.GetPartiesConfig()), "Party was not removed from the config")

		for _, partyConfig := range routerNodeConfig.SharedConfig.GetPartiesConfig() {
			require.NotEqual(t, partyToRemove, partyConfig.PartyID, "Removed party still exists in the config")
		}
	}

	// Restart the removed party
	armaNetwork.RestartParties(t, []types.PartyID{partyToRemove}, readyChan)
	testutil.WaitReady(t, readyChan, 3+numOfShards, 10)

	observedPrimaryIdByRemovedParty := types.PartyID(uint64(1)%uint64(numOfParties) + 1)
	primaryBatcher := armaNetwork.GetBatcher(t, observedPrimaryIdByRemovedParty, types.ShardID(1))
	primaryBatcherEndpoint := fmt.Sprintf("%s:%d", primaryBatcher.Listener.Addr().(*net.TCPAddr).IP.String(), primaryBatcher.Listener.Addr().(*net.TCPAddr).Port)
	removedBatcher := armaNetwork.GetBatcher(t, partyToRemove, types.ShardID(1))

	// Verify that the removed party's batcher fails to connect to the primary batcher of its shard (as seen from its own stale view).
	detectCh := removedBatcher.RunInfo.Session.Err.Detect(
		`Failed creating Deliver stream to %s: .*error: tls: unknown certificate authority`,
		primaryBatcherEndpoint,
	)
	defer removedBatcher.RunInfo.Session.Err.CancelDetects()
	select {
	case <-detectCh:
	case <-time.After(15 * time.Second):
		require.Fail(t, "Removed party's batcher succeeded to connect to the primary batcher")
	}
}

// TestRemoveParty verifies that removing a party via a config update succeeds,
// that the updated shared config no longer includes the removed party, and that
// the remaining Arma nodes can be restarted and continue processing transactions.
func TestRemoveParty(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 5
	numOfShards := 2
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	parties := make([]types.PartyID, 0, numOfParties)
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	uc, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)
	require.NotNil(t, uc)

	totalTxNumber := 10

	// Send transactions to all parties to ensure network is operational before config update
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	org := fmt.Sprintf("org%d", submittingParty)

	for i := range totalTxNumber {
		txContent := tx.PrepareTxWithTimestamp(i+totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	pullRequestSigner := signutil.CreateTestSigner(t, "org1", dir)
	statusUnknown := common.Status_UNKNOWN

	// Pull blocks to verify all transactions are included
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      120,
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// Create config update to remove a party
	configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()

	partyToRemove := types.PartyID(2)
	configUpdatePbData := configUpdateBuilder.RemoveParty(t, partyToRemove)

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	// Wait for Arma nodes to soft stop
	testutil.WaitSoftStopped(t, netInfo)

	// Verify that the party is removed by checking the router's shared config
	var remainingParties []types.PartyID
	for i := 1; i <= numOfParties; i++ {
		if types.PartyID(i) == partyToRemove {
			continue
		}
		remainingParties = append(remainingParties, types.PartyID(i))
	}

	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", submittingParty), "local_config_router.yaml")
	routerNodeConfig, _, err := config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)
	require.Equal(t, len(remainingParties), len(routerNodeConfig.SharedConfig.GetPartiesConfig()), "Party was not removed from the config")

	for _, partyConfig := range routerNodeConfig.SharedConfig.GetPartiesConfig() {
		require.NotEqual(t, partyToRemove, partyConfig.PartyID, "Removed party still exists in the config")
	}

	t.Log("Wait for the removed party to enter pending admin state and then stop the party")
	testutil.WaitForPendingAdminByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, []types.PartyID{partyToRemove})
	armaNetwork.StopParties([]types.PartyID{partyToRemove})

	t.Log("Wait for arma nodes to restart dynamically")
	testutil.WaitForRelaunchByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, remainingParties, 1)

	// Send transactions to remaining parties to verify they are processed
	uc.RouterEndpoints = append(uc.RouterEndpoints[:partyToRemove-1], uc.RouterEndpoints[partyToRemove:]...)
	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

	for i := range totalTxNumber {
		txContent := tx.PrepareTxWithTimestamp(i+totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	statusUnknown = common.Status_UNKNOWN

	// Pull blocks to verify all transactions are included
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      remainingParties,
		Transactions: totalTxNumber*2 + 1, // including config update tx
		Timeout:      120,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       signutil.CreateTestSigner(t, "org1", dir),
	})

	armaNetwork.StopParties(remainingParties)
}

// TestAddNewParty verifies that adding a party via a config update succeeds,
// that the new party's config is included in the updated shared config,
// and that the new party can join (start) and process transactions after the config update.
func TestAddNewParty(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 1

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	submittingParty := types.PartyID(1)

	userConfig, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)

	broadcastClient := client.NewBroadcastTxClient(userConfig, 10*time.Second)

	totalTxNumber := 100

	// rate limiter parameters
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	rate := 500
	capacity := rate / fillFrequency
	rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
		os.Exit(3)
	}

	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, userConfig.MSPDir)
	require.NoError(t, err)
	org := fmt.Sprintf("org%d", submittingParty)

	for i := range totalTxNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	var parties []types.PartyID
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	pullRequestSigner := signutil.CreateTestSigner(t, "org1", dir)
	statusUnknown := common.Status_UNKNOWN
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      120,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// Create config update to add a party
	configUpdateBuilder, _ := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	addedPartyId, addedNetInfo := configUpdateBuilder.PrepareAndAddNewParty(t, dir)

	env := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2, 3}, int(submittingParty), configUpdateBuilder.ConfigUpdatePBData(t))
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	t.Log("Wait for network relaunch")
	testutil.WaitForNetworkRelaunch(t, netInfo, 1)

	t.Log("Get the config block from an assembler ledger and write it to a temp location")
	configBlockStoreDir := t.TempDir()
	newConfigBlockPath := filepath.Join(configBlockStoreDir, "config.block")

	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber + 1, // include the config block
		Timeout:      120,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
		BlockHandler: &exportConfigBlockToFile{configSeq: 1, path: newConfigBlockPath},
	})

	t.Log("Verify the config block file was created")
	require.FileExists(t, newConfigBlockPath, "Config block file should exist after pulling from assembler")
	t.Logf("Config block successfully written to: %s", newConfigBlockPath)

	// Update the config block path in the net info of the added party to point to the new config block path
	// This is needed for the added party to be able to join the network using the new config block
	for _, netNode := range addedNetInfo {
		netNode.ConfigBlockPath = newConfigBlockPath
	}

	addedPartyUserConfig, err := testutil.GetUserConfig(dir, addedPartyId)
	require.NoError(t, err)

	var routerEndpoints, assemblerEndpoints []string
	var tlsCACertsBytesPartiesCollection [][]byte

	routerEndpoints = append(routerEndpoints, userConfig.RouterEndpoints...)
	routerEndpoints = append(routerEndpoints, addedPartyUserConfig.RouterEndpoints...)
	assemblerEndpoints = append(assemblerEndpoints, userConfig.AssemblerEndpoints...)
	assemblerEndpoints = append(assemblerEndpoints, addedPartyUserConfig.AssemblerEndpoints...)
	// the added party TLS CA certs already has all the existing parties TLS CA certs
	tlsCACertsBytesPartiesCollection = append(tlsCACertsBytesPartiesCollection, addedPartyUserConfig.TLSCACerts...)

	numOfParties++

	for i := range numOfParties {
		uc, err := testutil.GetUserConfig(dir, types.PartyID(i+1))
		require.NoError(t, err)

		uc.RouterEndpoints = routerEndpoints
		uc.AssemblerEndpoints = assemblerEndpoints
		uc.TLSCACerts = tlsCACertsBytesPartiesCollection

		err = utils.WriteToYAML(uc, filepath.Join(dir, "config", fmt.Sprintf("party%d", i+1), "user_config.yaml"))
		require.NoError(t, err)
	}

	maps.Copy(netInfo, addedNetInfo)
	numOfNewArmaNodes := len(addedNetInfo)
	readyChan = make(chan string, numOfNewArmaNodes)

	t.Log("Start the new added party")
	armaNetwork.AddAndStartNodes(t, dir, armaBinaryPath, readyChan, addedNetInfo)

	t.Log("Wait for the new party to be ready")
	testutil.WaitReady(t, readyChan, len(addedNetInfo), 10)

	userConfig, err = testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)

	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)
	defer broadcastClient.Stop()

	t.Log("Send a single transaction to the network")
	txContent := tx.PrepareTxWithTimestamp(0, 64, []byte("sessionNumber"))
	env = tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
	err = broadcastClient.SendTxTo(env, addedPartyId)
	require.NoError(t, err)

	t.Log("Send more transactions to the network")
	totalTxNumber = 100

	for i := range totalTxNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	parties = []types.PartyID{}
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	pullRequestSigner = signutil.CreateTestSigner(t, "org1", dir)
	statusUnknown = common.Status_UNKNOWN
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber*2 + 2, // including the config tx and the single tx
		Timeout:      120,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})
}

// TestReplacePartiesPartially verifies that an arma network can dynamically replace parties
// by removing one party and adding multiple new parties in successive configuration updates.
// Scenario:
//  1. Run an arma network of 5 parties, single shard.
//  2. Send txs and pull blocks to ensure network is operational.
//  3. Send a config tx that removes party 5.
//  4. Wait for the removed party to enter pending admin state and then stop the party, wait for the rest of the network to restart dynamically.
//  5. Clean the removed party from the network information, the shared config yaml and the crypto dir.
//  6. Pull the last config block from assembler so that block is written to a temporary directory, this block will be the bootstrap block of the first added party.
//  7. Send successive config updates that add a party:
//     Iteratively (2 times):
//     - Send a config tx that adds a new party.
//     - Wait for dynamic restart of all nodes.
//     - Pull the last config block from assembler so that block is written to a temporal directory, this block will be the bootstrap block of the next added party.
//     - Add the new party to the network and start the party.
//     - Updates user configurations across all parties to include new endpoints and TLS certificates of the added parties.
//  8. Send more txs and pull blocks to verify the network is operational again, after all accumulated configuration changes.
func TestReplacePartiesPartially(t *testing.T) {
	// 1.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 5
	numOfShards := 1
	submittingPartyID := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	configFilePath := filepath.Join(dir, fmt.Sprintf("config/party%d/local_config_router.yaml", types.PartyID(submittingPartyID)))
	conf, _, err := config.LoadLocalConfig(configFilePath)
	require.NoError(t, err)

	// Modify the router configuration to require client signature verification.
	conf.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = true
	err = utils.WriteToYAML(conf.NodeLocalConfig, configFilePath)
	require.NoError(t, err)

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan string, numOfArmaNodes)
	defer netInfo.CleanUp()
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()
	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	parties := make([]types.PartyID, 0, numOfParties)
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	uc, err := testutil.GetUserConfig(dir, submittingPartyID)
	require.NoError(t, err)

	txNumber := 10
	totalTxNumber := 0

	// 2.
	// Send transactions to all parties to ensure network is operational before config update
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	submittingOrg := fmt.Sprintf("org%d", submittingPartyID)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}

	// Pull blocks to verify all transactions are included
	pullRequestSigner := signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown := common.Status_UNKNOWN
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      60,
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	broadcastClient.Stop()

	// 3.
	configBlockPath := filepath.Join(dir, "bootstrap", "bootstrap.block")
	builder, _ := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
	partyToRemove := types.PartyID(5)
	remainingParties := make([]types.PartyID, 0, numOfParties-1)
	for i := 1; i <= numOfParties; i++ {
		if types.PartyID(i) != partyToRemove {
			remainingParties = append(remainingParties, types.PartyID(i))
		}
	}
	builder.RemoveParty(t, partyToRemove)

	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

	// Send the config tx
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingPartyID), builder.ConfigUpdatePBData(t))
	require.NotNil(t, env)
	configSeq := uint64(1)

	err = broadcastClient.SendTxTo(env, submittingPartyID)
	require.NoError(t, err)
	totalTxNumber++ // for the config update transaction

	// 4.
	t.Logf("Wait for the removed party %d to enter pending admin state", partyToRemove)
	testutil.WaitForPendingAdminByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, []types.PartyID{partyToRemove})

	t.Logf("Wait for arma nodes to restart dynamically after removal of party %d", partyToRemove)
	testutil.WaitForRelaunchByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, remainingParties, configSeq)

	t.Logf("Stop the removed party %d", partyToRemove)
	armaNetwork.StopParties([]types.PartyID{partyToRemove})
	// 5.
	// Remove the removed party from the network info and parties list
	maps.DeleteFunc(netInfo, func(nodeName testutil.NodeName, _ *testutil.ArmaNodeInfo) bool {
		return nodeName.PartyID == partyToRemove
	})

	// Remove the removed party from the list of parties
	parties = slices.DeleteFunc(parties, func(partyID types.PartyID) bool {
		return partyID == partyToRemove
	})

	// Remove the removed party from the shared config
	sharedConfig := config.SharedConfigYaml{}
	err = utils.ReadFromYAML(&sharedConfig, filepath.Join(dir, "bootstrap", "shared_config.yaml"))
	require.NoError(t, err, "failed to load shared config")
	sharedConfig.PartiesConfig = slices.DeleteFunc(sharedConfig.PartiesConfig, func(partyConfig config.PartyConfig) bool {
		return partyConfig.PartyID == partyToRemove
	})
	err = utils.WriteToYAML(sharedConfig, filepath.Join(dir, "bootstrap", "shared_config.yaml"))
	require.NoError(t, err, "failed to write updated shared config")

	// Remove the crypto materials of the removed party
	err = os.RemoveAll(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyToRemove)))
	require.NoError(t, err)

	// 6.
	t.Log("Get the config block from an assembler ledger and write it to a temp location")
	submittingPartyID = parties[0]
	configBlockStoreDir := t.TempDir()
	configBlockPath = filepath.Join(configBlockStoreDir, fmt.Sprintf("config_%d.block", configSeq))

	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      120,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
		BlockHandler: &exportConfigBlockToFile{configSeq: configSeq, path: configBlockPath},
	})

	t.Logf("Verify the config block %d file was created", configSeq)
	require.FileExists(t, configBlockPath, "Config block file should exist after pulling from assembler")
	t.Logf("Config block successfully written to: %s", configBlockPath)

	broadcastClient.Stop()

	// 7.
	for range 2 {
		// Create config update to add a new party
		builder, _ = configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		addedPartyId, addedNetInfo := builder.PrepareAndAddNewParty(t, dir)

		uc, err = testutil.GetUserConfig(dir, submittingPartyID)
		require.NoError(t, err)

		broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

		// Send the config tx
		env = configutil.CreateConfigTX(t, dir, parties, int(submittingPartyID), builder.ConfigUpdatePBData(t))
		require.NotNil(t, env)
		configSeq++

		t.Logf("Send config tx that adds party %d", addedPartyId)
		err = broadcastClient.SendTxTo(env, submittingPartyID)
		require.NoError(t, err)
		totalTxNumber++ // for the config update transaction

		broadcastClient.Stop()

		t.Logf("Len of netInfo is: %v\n", len(netInfo))
		for info := range netInfo {
			t.Logf("party id in net info: %d", info.PartyID)
		}
		t.Logf("Wait for network relaunch with config seq: %d", configSeq)
		testutil.WaitForNetworkRelaunch(t, netInfo, configSeq)

		t.Logf("Get the config block %d from an assembler ledger and write it to a temp location", configSeq)
		configBlockPath = filepath.Join(configBlockStoreDir, fmt.Sprintf("config_%d.block", configSeq))

		test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
			UserConfig:   uc,
			Parties:      parties,
			Transactions: totalTxNumber,
			Timeout:      120,
			ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
			Status:       &statusUnknown,
			Signer:       pullRequestSigner,
			BlockHandler: &exportConfigBlockToFile{configSeq: configSeq, path: configBlockPath},
		})

		t.Logf("Verify the config block %d file was created", configSeq)
		require.FileExists(t, configBlockPath, "Config block file should exist after pulling from assembler")
		t.Logf("Config block %d successfully written to: %s", configSeq, configBlockPath)

		for _, netNode := range addedNetInfo {
			netNode.ConfigBlockPath = configBlockPath
		}
		// Update the network info with the new party's nodes
		maps.Copy(netInfo, addedNetInfo)
		numOfNewArmaNodes := len(addedNetInfo)
		readyChan = make(chan string, numOfNewArmaNodes)

		t.Logf("Start the new added party %d", addedPartyId)
		armaNetwork.AddAndStartNodes(t, dir, armaBinaryPath, readyChan, addedNetInfo)

		t.Logf("Wait for the new party %d to be ready", addedPartyId)
		testutil.WaitReady(t, readyChan, len(addedNetInfo), 10)

		addedPartyUserConfig, err := testutil.GetUserConfig(dir, addedPartyId)
		require.NoError(t, err)

		parties = append(parties, addedPartyId)

		// Update the user config of all parties to include the new party's endpoints and TLS CA certs
		for _, partyID := range parties {
			userConfig, err := testutil.GetUserConfig(dir, partyID)
			require.NoError(t, err)

			userConfig.RouterEndpoints = append(uc.RouterEndpoints, addedPartyUserConfig.RouterEndpoints...)
			userConfig.AssemblerEndpoints = append(uc.AssemblerEndpoints, addedPartyUserConfig.AssemblerEndpoints...)
			userConfig.TLSCACerts = addedPartyUserConfig.TLSCACerts

			err = utils.WriteToYAML(userConfig, filepath.Join(dir, "config", fmt.Sprintf("party%d", partyID), "user_config.yaml"))
			require.NoError(t, err)
		}

		// Reload uc to get the updated endpoints after writing all user configs
		uc, err = testutil.GetUserConfig(dir, submittingPartyID)
		require.NoError(t, err)

		// Pull blocks from the newly added party's assembler to verify it has synced
		t.Logf("Wait for party %d to complete syncing", addedPartyId)
		syncVerifier := &verifySyncComplete{expectedConfigSeq: configSeq, syncComplete: false}
		test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
			UserConfig:   uc,
			Parties:      []types.PartyID{addedPartyId}, // Only pull from the new party
			Transactions: totalTxNumber,
			Timeout:      120,
			ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
			Status:       &statusUnknown,
			Signer:       pullRequestSigner,
			BlockHandler: syncVerifier,
		})
		require.True(t, syncVerifier.syncComplete, "Party %d did not sync config block %d", addedPartyId, configSeq)
		t.Logf("Party %d has completed syncing with config block %d", addedPartyId, configSeq)
		time.Sleep(60 * time.Second) // give the new party some time to finish connecting to the consensus cluster
	}

	// After removing and adding parties, verify that the remaining parties can still process transactions with the updated config
	// Send transactions to all parties to ensure network is operational after all the config updates
	uc, err = testutil.GetUserConfig(dir, submittingPartyID)
	require.NoError(t, err)

	signer, certBytes, err = testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)
	submittingOrg = fmt.Sprintf("org%d", submittingPartyID)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}

	broadcastClient.Stop()

	pullRequestSigner = signutil.CreateTestSigner(t, submittingOrg, dir)
	// Pull blocks to verify all transactions are included
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      60,
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})
}

type verifySyncComplete struct {
	expectedConfigSeq uint64
	syncComplete      bool
}

func (v *verifySyncComplete) HandleBlock(t *testing.T, block *common.Block) error {
	if protoutil.IsConfigBlock(block) {
		env, err := protoutil.ExtractEnvelope(block, 0)
		require.NoError(t, err)
		payload, err := protoutil.UnmarshalPayload(env.Payload)
		require.NoError(t, err)
		configEnv, err := protoutil.UnmarshalConfigEnvelope(payload.Data)
		require.NoError(t, err)
		if configEnv.GetConfig().GetSequence() == v.expectedConfigSeq {
			t.Logf("Verified party has synced config block %d", block.Header.Number)
			v.syncComplete = true
		}
	}
	return nil
}

// TestRemoveMultipleParties verifies that the Arma orderer network can dynamically remove multiple parties
// through configuration updates and continue operating correctly with the remaining parties.
func TestRemoveMultipleParties(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 7
	numOfShards := 1
	submittingPartyID := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	require.NotNil(t, netInfo)
	defer netInfo.CleanUp()

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	configFilePath := filepath.Join(
		dir,
		"config",
		fmt.Sprintf("party%d", types.PartyID(submittingPartyID)),
		"local_config_router.yaml",
	)
	conf, _, err := config.LoadLocalConfig(configFilePath)
	require.NoError(t, err)

	// Modify the router configuration to require client signature verification.
	conf.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = true
	err = utils.WriteToYAML(conf.NodeLocalConfig, configFilePath)
	require.NoError(t, err)

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	t.Cleanup(func() {
		armaNetwork.Stop()
	})

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	parties := make([]types.PartyID, 0, numOfParties)
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	uc, err := testutil.GetUserConfig(dir, submittingPartyID)
	require.NoError(t, err)

	txNumber := 10
	totalTxNumber := 0
	// Send transactions to all parties to ensure network is operational before config update
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	submittingOrg := fmt.Sprintf("org%d", submittingPartyID)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}
	pullRequestSigner := signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown := common.Status_UNKNOWN
	// Pull blocks to verify all transactions are included
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      60,
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	broadcastClient.Stop()

	configBlockPath := filepath.Join(dir, "bootstrap", "bootstrap.block")

	for range 3 {
		// Create config update to remove a party
		builder, _ := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		builder.RemoveParty(t, types.PartyID(numOfParties))

		broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

		// Send the config tx
		env := configutil.CreateConfigTX(t, dir, parties, int(submittingPartyID), builder.ConfigUpdatePBData(t))
		require.NotNil(t, env)

		err = broadcastClient.SendTxTo(env, submittingPartyID)
		require.NoError(t, err)
		totalTxNumber++

		// Wait for Arma nodes to stop
		testutil.WaitSoftStopped(t, netInfo)

		broadcastClient.Stop()
		// Stop Arma nodes
		armaNetwork.Stop()

		// Read the last config block to get the updated config after removing a party
		_, lastConfigBlock, err := config.ReadConfig(filepath.Join(dir, "config", fmt.Sprintf("party%d", submittingPartyID), "local_config_assembler.yaml"), flogging.MustGetLogger("TestRemoveMultipleParties"))
		require.NoError(t, err)
		// Write the last config block to a separate location to be used for restarting the Arma nodes with the updated config
		configBlockPath = filepath.Join(dir, fmt.Sprintf("config.block.%d", numOfParties))
		err = configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath)
		require.NoError(t, err)

		uc.RouterEndpoints = slices.DeleteFunc(uc.RouterEndpoints, func(endpoint string) bool {
			return endpoint == netInfo[testutil.NodeName{PartyID: types.PartyID(numOfParties), NodeType: testutil.Router}].Listener.Addr().String()
		})
		uc.AssemblerEndpoints = slices.DeleteFunc(uc.AssemblerEndpoints, func(endpoint string) bool {
			return endpoint == netInfo[testutil.NodeName{PartyID: types.PartyID(numOfParties), NodeType: testutil.Assembler}].Listener.Addr().String()
		})
		maps.DeleteFunc(netInfo, func(nodeName testutil.NodeName, _ *testutil.ArmaNodeInfo) bool {
			return nodeName.PartyID == types.PartyID(numOfParties)
		})
		parties = parties[:numOfParties-1]
		numOfParties--

		numOfArmaNodes = len(netInfo)
		readyChan = make(chan string, numOfArmaNodes)
		armaNetwork = testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
		testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)
	}
	// After removing parties, verify that the remaining parties can still process transactions with the updated config
	// Send transactions to all parties to ensure network is operational after all the config updates

	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)
	submittingOrg = fmt.Sprintf("org%d", submittingPartyID)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}

	broadcastClient.Stop()

	pullRequestSigner = signutil.CreateTestSigner(t, submittingOrg, dir)
	// Pull blocks to verify all transactions are included
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      60,
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})
}

// TestJoinMultipleParties verifies that multiple parties can be added sequentially to a running Arma network.
// The test bootstraps an initial multi-party network, sends baseline transactions to confirm liveness,
// then iteratively adds three new parties via config updates. For each addition:
//  1. A config update is created and submitted to add a new party.
//  2. The network is stopped and restarted with the updated configuration.
//  3. All parties' user configs are updated to include the new party's endpoints and TLS CA certificates.
//
// After all additions, the test verifies that the expanded network remains operational by sending
// and pulling additional transactions across all parties
func TestJoinMultipleParties(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 1
	submittingPartyID := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	require.NotNil(t, netInfo)
	defer netInfo.CleanUp()

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	configFilePath := filepath.Join(dir, fmt.Sprintf("config/party%d/local_config_router.yaml", types.PartyID(submittingPartyID)))
	conf, _, err := config.LoadLocalConfig(configFilePath)
	require.NoError(t, err)

	// Modify the router configuration to require client signature verification.
	conf.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = true
	err = utils.WriteToYAML(conf.NodeLocalConfig, configFilePath)
	require.NoError(t, err)

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	t.Cleanup(func() {
		armaNetwork.Stop()
	})

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	parties := make([]types.PartyID, 0, numOfParties)
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	uc, err := testutil.GetUserConfig(dir, submittingPartyID)
	require.NoError(t, err)

	txNumber := 10
	totalTxNumber := 0
	// Send transactions to all parties to ensure network is operational before config update
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	submittingOrg := fmt.Sprintf("org%d", submittingPartyID)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}
	pullRequestSigner := signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown := common.Status_UNKNOWN
	// Pull blocks to verify all transactions are included
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      60,
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	broadcastClient.Stop()

	configBlockPath := filepath.Join(dir, "bootstrap", "bootstrap.block")

	for range 3 {
		// Create config update to add a new party
		builder, _ := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		addedPartyId, addedNetInfo := builder.PrepareAndAddNewParty(t, dir)
		uc, err = testutil.GetUserConfig(dir, submittingPartyID)
		require.NoError(t, err)

		broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

		// Send the config tx
		env := configutil.CreateConfigTX(t, dir, parties, int(submittingPartyID), builder.ConfigUpdatePBData(t))
		require.NotNil(t, env)

		err = broadcastClient.SendTxTo(env, submittingPartyID)
		require.NoError(t, err)
		totalTxNumber++

		// Wait for Arma nodes to stop
		testutil.WaitSoftStopped(t, netInfo)

		broadcastClient.Stop()
		// Stop Arma nodes
		armaNetwork.Stop()

		// Read the last config block to get the updated config with the new party
		_, lastConfigBlock, err := config.ReadConfig(filepath.Join(dir, "config", fmt.Sprintf("party%d", submittingPartyID), "local_config_assembler.yaml"), flogging.MustGetLogger("TestAddNewParty"))
		require.NoError(t, err)
		// Write the last config block to a separate location to be used for starting the Arma nodes with the updated config
		configBlockPath = filepath.Join(dir, fmt.Sprintf("config.block.%d", addedPartyId))
		err = configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath)
		require.NoError(t, err)

		for _, netNode := range addedNetInfo {
			netNode.ConfigBlockPath = configBlockPath
		}
		// Update the network info with the new party's nodes
		maps.Copy(netInfo, addedNetInfo)

		numOfArmaNodes = len(netInfo)
		readyChan = make(chan string, numOfArmaNodes)
		armaNetwork = testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
		testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

		addedPartyUserConfig, err := testutil.GetUserConfig(dir, addedPartyId)
		require.NoError(t, err)

		parties = append(parties, addedPartyId)
		// Update the user config of all parties to include the new party's endpoints and TLS CA certs
		for _, partyID := range parties {
			userConfig, err := testutil.GetUserConfig(dir, partyID)
			require.NoError(t, err)

			userConfig.RouterEndpoints = append(uc.RouterEndpoints, addedPartyUserConfig.RouterEndpoints...)
			userConfig.AssemblerEndpoints = append(uc.AssemblerEndpoints, addedPartyUserConfig.AssemblerEndpoints...)
			userConfig.TLSCACerts = addedPartyUserConfig.TLSCACerts

			err = utils.WriteToYAML(userConfig, filepath.Join(dir, "config", fmt.Sprintf("party%d", partyID), "user_config.yaml"))
			require.NoError(t, err)
		}

		testutil.StopAndRestartArmaNetwork(t, armaNetwork)
	}
	// After adding parties, verify that the remaining parties can still process transactions with the updated config
	// Send transactions to all parties to ensure network is operational after all the config updates
	uc, err = testutil.GetUserConfig(dir, submittingPartyID)
	require.NoError(t, err)

	signer, certBytes, err = testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)
	submittingOrg = fmt.Sprintf("org%d", submittingPartyID)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}

	broadcastClient.Stop()

	pullRequestSigner = signutil.CreateTestSigner(t, submittingOrg, dir)
	// Pull blocks to verify all transactions are included
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      60,
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})
}

type exportConfigBlockToFile struct {
	configSeq uint64
	path      string
}

func (ec *exportConfigBlockToFile) HandleBlock(t *testing.T, block *common.Block) error {
	if protoutil.IsConfigBlock(block) {
		env, err := protoutil.ExtractEnvelope(block, 0)
		require.NoError(t, err)
		payload, err := protoutil.UnmarshalPayload(env.Payload)
		require.NoError(t, err)
		configEnv, err := protoutil.UnmarshalConfigEnvelope(payload.Data)
		require.NoError(t, err)
		if configEnv.GetConfig().GetSequence() == ec.configSeq {
			configBlock := &common.Block{Header: block.GetHeader(), Data: block.GetData(), Metadata: block.GetMetadata()}
			err := configtxgen.WriteOutputBlock(configBlock, ec.path)
			require.NoError(t, err)
		}
	}
	return nil
}
