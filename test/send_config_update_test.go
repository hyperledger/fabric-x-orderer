/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
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
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestUpdatePartyRouterEndpoint(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 2
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 2, "none", "none")
	require.NotNil(t, netInfo)
	require.NoError(t, err)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

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

	userConfig, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

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

	broadcastClient := client.NewBroadcastTxClient(userConfig, 10*time.Second)
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

	statusUknown := common.Status_UNKNOWN
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUknown,
		Signer:       pullRequestSigner,
	})

	// Create config update
	configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()

	partyToUpdate := types.PartyID(submittingParty)
	routerIP := strings.Split(userConfig.RouterEndpoints[partyToUpdate-1], ":")[0] // extract IP from the user config router endpoint
	availablePort, newListener := testutil.GetAvailablePort(t)
	newPort, err := strconv.Atoi(availablePort)
	require.NoError(t, err)
	routerToMonitor := armaNetwork.GetRouter(t, submittingParty)
	routerToMonitor.Listener = newListener

	configUpdatePbData := configUpdateBuilder.UpdateRouterEndpoint(t, partyToUpdate, routerIP, newPort)

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2}, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	// Pull blocks to verify all transactions are included
	userBlockHandler := &verifyRouterEndpointUpdate{updatedParty: partyToUpdate, routerIP: routerIP, newPort: newPort}
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber + 1, // including config update tx
		Timeout:      60,
		BlockHandler: userBlockHandler,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUknown,
		Signer:       pullRequestSigner,
	})

	require.True(t, userBlockHandler.RouterEndpointUpdated.Load(), "Router endpoint was not updated in the config update")

	// Restart Arma nodes
	armaNetwork.Stop()

	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyToUpdate), "local_config_router.yaml")

	// Verify the router node config stored in the router ledger is updated
	cfg, _, err := config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)
	require.True(t, cfg.SharedConfig.GetPartiesConfig()[partyToUpdate-1].RouterConfig.Host == routerIP &&
		cfg.SharedConfig.GetPartiesConfig()[partyToUpdate-1].RouterConfig.Port == uint32(newPort), "Shared config was not updated with the new router endpoint")

	// Update the router node local config with the new endpoint to allow it to start
	localConfig, _, err := config.LoadLocalConfig(routerNodeConfigPath)
	require.NoError(t, err)
	localConfig.NodeLocalConfig.GeneralConfig.ListenAddress = routerIP
	localConfig.NodeLocalConfig.GeneralConfig.ListenPort = uint32(newPort)
	utils.WriteToYAML(localConfig.NodeLocalConfig, routerNodeConfigPath)

	armaNetwork.Restart(t, readyChan)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	// Send transactions again and verify they are processed

	// Update the user config with the new router endpoint
	userConfig.RouterEndpoints[0] = fmt.Sprintf("%s:%d", routerIP, newPort)
	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)

	for i := range totalTxNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i+totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	// Pull blocks to verify all transactions are included
	userBlockHandler = &verifyRouterEndpointUpdate{updatedParty: partyToUpdate, routerIP: routerIP, newPort: newPort}
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber*2 + 1, // including config update tx
		Timeout:      60,
		BlockHandler: userBlockHandler,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUknown,
		Signer:       pullRequestSigner,
	})

	require.True(t, userBlockHandler.RouterEndpointUpdated.Load(), "Router endpoint was not updated in the config update")
}

// Verify that the config update is applied by checking the router endpoint in the config update block
type verifyRouterEndpointUpdate struct {
	updatedParty          types.PartyID
	RouterEndpointUpdated atomic.Bool
	routerIP              string
	newPort               int
}

func (v *verifyRouterEndpointUpdate) HandleBlock(t *testing.T, block *common.Block) error {
	if protoutil.IsConfigBlock(block) {
		envelope, err := configutil.ReadConfigEnvelopeFromConfigBlock(block)
		if err != nil || envelope == nil {
			return fmt.Errorf("failed to read config envelope from config block: %w", err)
		}

		partyConfig := configutil.GetPartyConfig(t, envelope, v.updatedParty)
		if partyConfig == nil {
			return fmt.Errorf("party config for party %d not found in the config block", v.updatedParty)
		}

		v.RouterEndpointUpdated.Store(partyConfig.RouterConfig.Host == v.routerIP && partyConfig.RouterConfig.Port == uint32(v.newPort))
	}

	return nil
}

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
	numOfParties := 2
	numOfShards := 2
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	require.NotNil(t, netInfo)
	require.NoError(t, err)

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

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2}, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", submittingParty), "local_config_router.yaml")
	routerNodeConfig, _, err := config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)
	require.Equal(t, numOfParties-1, len(routerNodeConfig.SharedConfig.GetPartiesConfig()), "Party was not removed from the config")

	for _, partyConfig := range routerNodeConfig.SharedConfig.GetPartiesConfig() {
		require.NotEqual(t, partyToRemove, partyConfig.PartyID, "Removed party still exists in the config")
	}

	// Stop Arma nodes
	armaNetwork.Stop()

	numOfNodesPerParty := 3 + numOfShards
	readyChan = make(chan string, (numOfParties-1)*numOfNodesPerParty)

	// Try to restart the removed party nodes, expect them to fail to start
	armaNetwork.RestartParties(t, []types.PartyID{partyToRemove}, readyChan)
	defer armaNetwork.Stop()
	// Expect the removed party nodes to fail to start
	// TODO: improve the detection of failed nodes by checking specific exit codes,
	// rather than relying on string matching in the output
	// every node should report a panic during startup
	testutil.WaitPanic(t, readyChan, numOfNodesPerParty-1, 10)

	numOfArmaNodes = (numOfParties - 1) * numOfNodesPerParty
	readyChan = make(chan string, numOfArmaNodes)
	// Restart the remaining parties' nodes
	armaNetwork.RestartParties(t, []types.PartyID{1}, readyChan)
	// Expect the rest of the nodes to start successfully
	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)
}

// TestRemoveStoppedPartyThenRestart verifies that after removing a stopped party that via a config update
// and stopping all Arma nodes, restarting the entire network results in all nodes starting successfully,
// while the removed party's nodes fail to establish connections to the rest of the network.
func TestRemoveStoppedPartyThenRestart(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 1

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	require.NotNil(t, netInfo)
	require.NoError(t, err)

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

	partyToRemove := types.PartyID(4)
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

	statusUnknown := common.Status_UNKNOWN
	// Pull blocks to verify all transactions are included
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   uc,
		Parties:      remainingParties,
		Transactions: totalTxNumber*2 + 1, // including config update tx
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      60,
		Status:       &statusUnknown,
		Signer:       signutil.CreateTestSigner(t, "org1", dir),
	})

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	// Stop Arma nodes
	armaNetwork.Stop()

	for _, partyId := range remainingParties {
		// Verify that the party is removed by checking the assembler's shared config
		assemblerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyId), "local_config_assembler.yaml")
		assemblerNodeConfig, _, err := config.ReadConfig(assemblerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigAssembler", zap.DebugLevel))
		require.NoError(t, err)
		require.Equal(t, numOfParties-1, len(assemblerNodeConfig.SharedConfig.GetPartiesConfig()), "Party was not removed from the config")

		for _, partyConfig := range assemblerNodeConfig.SharedConfig.GetPartiesConfig() {
			require.NotEqual(t, partyToRemove, partyConfig.PartyID, "Removed party still exists in the config")
		}
	}

	// Restart nodes
	armaNetwork.Restart(t, readyChan)
	defer armaNetwork.Stop()

	// Expect all the nodes to start successfully
	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	primaryPartyID := types.PartyID((uint64(1))%uint64(numOfParties) + 1)

	primaryBatcher := armaNetwork.GetBatcher(t, primaryPartyID, types.ShardID(1))
	primaryBatcherEndPoint := fmt.Sprintf("%s:%d", primaryBatcher.Listener.Addr().(*net.TCPAddr).IP.String(), primaryBatcher.Listener.Addr().(*net.TCPAddr).Port)
	removedBatcher := armaNetwork.GetBatcher(t, partyToRemove, types.ShardID(1))

	// Verify that the removed party's batcher fails to connect to the primary batcher of its shard
	require.Eventually(t, func() bool {
		<-removedBatcher.RunInfo.Session.Err.Detect("%s", fmt.Sprintf("Failed creating Deliver stream to %s", primaryBatcherEndPoint))
		<-removedBatcher.RunInfo.Session.Err.Detect("error: tls: unknown certificate authority")
		return true
	}, 10*time.Second, 500*time.Millisecond, "Removed party's batcher succeded to connect to the primary batcher")
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
	numOfParties := 2
	numOfShards := 2
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	require.NotNil(t, netInfo)
	require.NoError(t, err)

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
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      60,
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

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	// Stop Arma nodes
	armaNetwork.Stop()

	// Verify that the party is removed by checking the router's shared config
	var remainingParties []types.PartyID
	for i := 1; i <= numOfParties; i++ {
		if types.PartyID(i) == partyToRemove {
			continue
		}
		remainingParties = append(remainingParties, types.PartyID(i))
	}

	numOfParties--
	numOfArmaNodes = numOfParties * (3 + numOfShards)

	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", submittingParty), "local_config_router.yaml")
	routerNodeConfig, _, err := config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)
	require.Equal(t, numOfParties, len(routerNodeConfig.SharedConfig.GetPartiesConfig()), "Party was not removed from the config")

	for _, partyConfig := range routerNodeConfig.SharedConfig.GetPartiesConfig() {
		require.NotEqual(t, partyToRemove, partyConfig.PartyID, "Removed party still exists in the config")
	}

	// Restart remaining Arma nodes
	readyChan = make(chan string, numOfArmaNodes)

	// Try to restart the remaining Arma nodes, removed party nodes will fail to start but the rest should start successfully
	armaNetwork.RestartParties(t, remainingParties, readyChan)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	// Send transactions to remaining parties to verify they are processed

	uc.RouterEndpoints = append(uc.RouterEndpoints[:partyToRemove-1], uc.RouterEndpoints[partyToRemove:]...)
	uc.AssemblerEndpoints = append(uc.AssemblerEndpoints[:partyToRemove-1], uc.AssemblerEndpoints[partyToRemove:]...)

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
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   uc,
		Parties:      remainingParties,
		Transactions: totalTxNumber*2 + 1, // including config update tx
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       signutil.CreateTestSigner(t, "org1", dir),
	})
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
	numOfParties := 3
	numOfShards := 1

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	require.NotNil(t, netInfo)
	require.NoError(t, err)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	submittingParty := types.PartyID(1)

	userConfig, err := testutil.GetUserConfig(dir, types.PartyID(submittingParty))
	require.NoError(t, err)

	broadcastClient := client.NewBroadcastTxClient(userConfig, 10*time.Second)
	totalTxNumber := 100
	// rate limiter parameters
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	rate := 500

	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, userConfig.MSPDir)
	require.NoError(t, err)
	org := fmt.Sprintf("org%d", submittingParty)

	capacity := rate / fillFrequency
	rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
		os.Exit(3)
	}

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
	statusUknown := common.Status_UNKNOWN
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      120,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUknown,
		Signer:       pullRequestSigner,
	})

	addedNetInfo, addedPartyConfig := testutil.ExtendNetwork(t, configPath)
	testutil.ExtendConfigAndCrypto(addedPartyConfig, dir, true)

	// Create config update
	configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()

	addedPartyId := types.PartyID(addedPartyConfig.Parties[0].ID)
	addedPartyDir := fmt.Sprintf("party%d", addedPartyId)
	addedOrg := fmt.Sprintf("org%d", addedPartyId)

	consenterConfig, _, err := config.LoadLocalConfig(filepath.Join(dir, "config", addedPartyDir, "local_config_consenter.yaml"))
	require.NoError(t, err)
	consenterTlsCert, err := os.ReadFile(consenterConfig.NodeLocalConfig.GeneralConfig.TLSConfig.Certificate)
	require.NoError(t, err)
	routerLocalConfig, _, err := config.LoadLocalConfig(filepath.Join(dir, "config", addedPartyDir, "local_config_router.yaml"))
	require.NoError(t, err)
	routerTlsCert, err := os.ReadFile(routerLocalConfig.NodeLocalConfig.GeneralConfig.TLSConfig.Certificate)
	require.NoError(t, err)
	assemblerConfig, _, err := config.LoadLocalConfig(filepath.Join(dir, "config", addedPartyDir, "local_config_assembler.yaml"))
	require.NoError(t, err)
	assemblerTlsCert, err := os.ReadFile(assemblerConfig.NodeLocalConfig.GeneralConfig.TLSConfig.Certificate)
	require.NoError(t, err)

	batchersConfig := make([]*protos.BatcherNodeConfig, len(addedPartyConfig.Parties[0].BatchersEndpoints))

	for i := range addedPartyConfig.Parties[0].BatchersEndpoints {
		batcherNodeConfig, _, err := config.LoadLocalConfig(filepath.Join(dir, "config", addedPartyDir, fmt.Sprintf("local_config_batcher%d.yaml", i+1)))
		require.NoError(t, err)
		batcherTlsCert, err := os.ReadFile(batcherNodeConfig.NodeLocalConfig.GeneralConfig.TLSConfig.Certificate)
		require.NoError(t, err)
		batcherSignCert, err := os.ReadFile(filepath.Join(batcherNodeConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "signcerts", "sign-cert.pem"))
		require.NoError(t, err)

		batchersConfig[i] = &protos.BatcherNodeConfig{
			ShardID:  uint32(i + 1),
			Host:     batcherNodeConfig.NodeLocalConfig.GeneralConfig.ListenAddress,
			Port:     batcherNodeConfig.NodeLocalConfig.GeneralConfig.ListenPort,
			TlsCert:  batcherTlsCert,
			SignCert: batcherSignCert,
		}
	}

	caCerts, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", addedOrg, "msp", "cacerts", "ca-cert.pem"))
	require.NoError(t, err)
	tlsCACerts, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", addedOrg, "msp", "tlscacerts", "tlsca-cert.pem"))
	require.NoError(t, err)
	consenterSignCert, err := os.ReadFile(filepath.Join(consenterConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "signcerts", "sign-cert.pem"))
	require.NoError(t, err)

	configUpdatePbData := configUpdateBuilder.AddNewParty(t, &protos.PartyConfig{
		CACerts:    [][]byte{caCerts},
		TLSCACerts: [][]byte{tlsCACerts},
		ConsenterConfig: &protos.ConsenterNodeConfig{
			Host:     consenterConfig.NodeLocalConfig.GeneralConfig.ListenAddress,
			Port:     consenterConfig.NodeLocalConfig.GeneralConfig.ListenPort,
			SignCert: consenterSignCert,
			TlsCert:  consenterTlsCert,
		},
		RouterConfig: &protos.RouterNodeConfig{
			Host:    routerLocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress,
			Port:    routerLocalConfig.NodeLocalConfig.GeneralConfig.ListenPort,
			TlsCert: routerTlsCert,
		},
		AssemblerConfig: &protos.AssemblerNodeConfig{
			Host:    assemblerConfig.NodeLocalConfig.GeneralConfig.ListenAddress,
			Port:    assemblerConfig.NodeLocalConfig.GeneralConfig.ListenPort,
			TlsCert: assemblerTlsCert,
		},
		BatchersConfig: batchersConfig,
	})

	env := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2, 3}, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	armaNetwork.Stop()

	// Get the config block from an assembler ledger and write it to a temp location
	configBlockStoreDir := t.TempDir()
	defer os.RemoveAll(configBlockStoreDir)

	newConfigBlockPath := filepath.Join(configBlockStoreDir, "config.block")
	_, lastConfigBlock, err := config.ReadConfig(filepath.Join(dir, "config", fmt.Sprintf("party%d", submittingParty), "local_config_assembler.yaml"), flogging.MustGetLogger("TestAddNewParty"))
	require.NoError(t, err)
	configBlock := &common.Block{Header: lastConfigBlock.GetHeader(), Data: lastConfigBlock.GetData(), Metadata: lastConfigBlock.GetMetadata()}
	err = configtxgen.WriteOutputBlock(configBlock, newConfigBlockPath)
	require.NoError(t, err)

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
	numOfArmaNodes = len(netInfo)
	readyChan = make(chan string, numOfArmaNodes)

	armaNetwork = testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	userConfig, err = testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)

	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)
	defer broadcastClient.Stop()

	txContent := tx.PrepareTxWithTimestamp(0, 64, []byte("sessionNumber"))
	env = tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
	err = broadcastClient.SendTxTo(env, addedPartyId)
	require.NoError(t, err)

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
	statusUknown = common.Status_UNKNOWN
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber * 2, // including the first tx sent to the new party
		Timeout:      120,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUknown,
		Signer:       pullRequestSigner,
	})
}

// TestChangePartyCertificates verifies that updating a party's certificates via a config update succeeds,
// and that the party can continue processing transactions after the config update with the new certificates.
func TestChangePartyCertificates(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 3
	numOfShards := 2
	submittingParty := types.PartyID(2)
	submittingOrg := fmt.Sprintf("org%d", submittingParty)
	partyToUpdate := types.PartyID(1)
	updateOrg := fmt.Sprintf("org%d", partyToUpdate)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	require.NotNil(t, netInfo)
	require.NoError(t, err)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	configFilePath := filepath.Join(dir, fmt.Sprintf("config/party%d/local_config_router.yaml", types.PartyID(submittingParty)))
	conf, _, err := config.LoadLocalConfig(configFilePath)
	require.NoError(t, err)

	// Modify the router configuration to require client signature verification.
	conf.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = true
	utils.WriteToYAML(conf.NodeLocalConfig, configFilePath)

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

	totalTxNumber := 10
	// Send transactions to all parties to ensure network is operational before config update
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)

	for i := range totalTxNumber {
		txContent := tx.PrepareTxWithTimestamp(i+totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}
	pullRequestSigner := signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown := common.Status_UNKNOWN
	// Pull blocks to verify all transactions are included
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      60,
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// Create config update to change a party's certificates
	configUpdateBuilder, _ := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))

	nodesIPs := testutil.GetNodesIPsFromNetInfo(netInfo)
	require.NotNil(t, nodesIPs)

	tlsCACertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "tlsca", "tlsca-cert.pem")
	tlsCAPrivKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "tlsca", "priv_sk")

	signCACertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "ca", "ca-cert.pem")
	signCAPrivKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "ca", "priv_sk")

	// Update the router TLS certs in the config
	newRouterTlsCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "router", "tls")
	newRouterTlsKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "router", "tls", "key.pem")
	newRouterTlsCertBytes, err := armageddon.CreateNewCertificateFromCA(tlsCACertPath, tlsCAPrivKeyPath, "tls", newRouterTlsCertPath, newRouterTlsKeyPath, nodesIPs)
	require.NoError(t, err)
	configUpdateBuilder.UpdateRouterTLSCert(t, partyToUpdate, newRouterTlsCertBytes)

	// Update the batchers TLS certs and signing certs in the config
	for shardToUpdate := types.ShardID(1); int(shardToUpdate) <= numOfShards; shardToUpdate++ {
		newBatcherTlsCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "tls")
		newBatcherTlsKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "tls", "key.pem")
		newBatcherTlsCertBytes, err := armageddon.CreateNewCertificateFromCA(tlsCACertPath, tlsCAPrivKeyPath, "tls", newBatcherTlsCertPath, newBatcherTlsKeyPath, nodesIPs)
		require.NoError(t, err)
		configUpdateBuilder.UpdateBatcherTLSCert(t, partyToUpdate, shardToUpdate, newBatcherTlsCertBytes)

		newBatcherSignCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "msp", "signcerts")
		newBatcherSignKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "msp", "keystore", "priv_sk")
		newBatcherSignCertBytes, err := armageddon.CreateNewCertificateFromCA(signCACertPath, signCAPrivKeyPath, "sign", newBatcherSignCertPath, newBatcherSignKeyPath, nodesIPs)
		require.NoError(t, err)
		configUpdateBuilder.UpdateBatcherSignCert(t, partyToUpdate, shardToUpdate, newBatcherSignCertBytes)
	}

	// Update the assembler TLS certs in the config
	newAssemblerTlsCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "assembler", "tls")
	newAssemblerTlsKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "assembler", "tls", "key.pem")
	newAssemblerTlsCertBytes, err := armageddon.CreateNewCertificateFromCA(tlsCACertPath, tlsCAPrivKeyPath, "tls", newAssemblerTlsCertPath, newAssemblerTlsKeyPath, nodesIPs)
	require.NoError(t, err)
	configUpdateBuilder.UpdateAssemblerTLSCert(t, partyToUpdate, newAssemblerTlsCertBytes)

	// Update the consenter TLS certs in the config
	newConsenterTlsCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "tls")
	newConsenterTlsKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "tls", "key.pem")
	newConsenterTlsCertBytes, err := armageddon.CreateNewCertificateFromCA(tlsCACertPath, tlsCAPrivKeyPath, "tls", newConsenterTlsCertPath, newConsenterTlsKeyPath, nodesIPs)
	require.NoError(t, err)
	configUpdateBuilder.UpdateConsensusTLSCert(t, partyToUpdate, newConsenterTlsCertBytes)

	// Update the consenter signing certs in the config
	newConsenterSignCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "msp", "signcerts")
	newConsenterSignKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "msp", "keystore", "priv_sk")
	newConsenterSignCertBytes, err := armageddon.CreateNewCertificateFromCA(signCACertPath, signCAPrivKeyPath, "sign", newConsenterSignCertPath, newConsenterSignKeyPath, nodesIPs)
	require.NoError(t, err)
	configUpdateBuilder.UpdateConsenterSignCert(t, partyToUpdate, newConsenterSignCertBytes)

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdateBuilder.ConfigUpdatePBData(t))
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	// Stop Arma nodes
	armaNetwork.Stop()

	// Verify that the party's certificates are updated by checking the router's shared config
	assemblerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyToUpdate), "local_config_assembler.yaml")
	assemblerConfig, _, err := config.ReadConfig(assemblerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigAssembler", zap.DebugLevel))
	require.NoError(t, err)

	var updatedPartyConfig *protos.PartyConfig
	for _, partyConfig := range assemblerConfig.SharedConfig.GetPartiesConfig() {
		if partyConfig.PartyID == uint32(partyToUpdate) {
			updatedPartyConfig = partyConfig
			break
		}
	}
	require.NotNil(t, updatedPartyConfig, "Updated party config not found in the config")

	newTlsCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "router", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	// Verify that the router TLS cert path is updated in the config
	require.Equal(t, newTlsCertBytes, updatedPartyConfig.RouterConfig.GetTlsCert(), "Certificate path was not updated in the config")

	// Verify that the batcher TLS certs path are updated in the config
	for _, shardConfig := range updatedPartyConfig.BatchersConfig {
		newBatcherTlsCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardConfig.ShardID), "tls")
		newTlsCertBytes, err = os.ReadFile(filepath.Join(newBatcherTlsCertPath, "tls-cert.pem"))
		require.NoError(t, err)
		newBatcherSignCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardConfig.ShardID), "msp", "signcerts")
		newSignCertBytes, err := os.ReadFile(filepath.Join(newBatcherSignCertPath, "sign-cert.pem"))
		require.NoError(t, err)
		require.Equal(t, newTlsCertBytes, shardConfig.GetTlsCert(), "Batcher certificate path was not updated in the config")
		require.Equal(t, newSignCertBytes, shardConfig.GetSignCert(), "Batcher signing certificate path was not updated in the config")
	}

	newTlsCertBytes, err = os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "assembler", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	// Verify that the assembler TLS cert path is updated in the config
	require.Equal(t, newTlsCertBytes, updatedPartyConfig.AssemblerConfig.GetTlsCert(), "Certificate path was not updated in the config")

	newTlsCertBytes, err = os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	newSignCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "msp", "signcerts", "sign-cert.pem"))
	require.NoError(t, err)
	// Verify that the consenter TLS and signing cert paths are updated in the config
	require.Equal(t, newTlsCertBytes, updatedPartyConfig.ConsenterConfig.GetTlsCert(), "Consenter certificate path was not updated in the config")
	require.Equal(t, newSignCertBytes, updatedPartyConfig.ConsenterConfig.GetSignCert(), "Consenter signing certificate path was not updated in the config")

	// Restart Arma nodes
	armaNetwork.Restart(t, readyChan)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	updatedRouterInfo := armaNetwork.GetRouter(t, partyToUpdate)
	// Verify that the updated router TLS connection to a consenter node is successful by checking for successful pulls,
	// if the TLS cert was not updated correctly, the router would fail to establish connections to the batchers and the pull would fail with TLS errors
	updatedRouterInfo.RunInfo.Session.Err.Detect("pullAndProcessDecisions -> Pulled config block number")

	for shardToUpdate := types.ShardID(1); int(shardToUpdate) <= numOfShards; shardToUpdate++ {
		updatedBatcherInfo := armaNetwork.GetBatcher(t, partyToUpdate, shardToUpdate)
		// Verify that the updated batcher TLS connection to a consenter is successful by checking for successful pulls,
		// if the TLS cert was not updated correctly, the batcher would fail to establish connections to the consenter and the pull would fail with TLS errors
		updatedBatcherInfo.RunInfo.Session.Err.Detect("replicateDecision -> Got config block number")

		assemblerInfo := armaNetwork.GetAssembler(t, partyToUpdate)
		// Verify that the assembler can pull blocks successfully with the updated certificates
		assemblerInfo.RunInfo.Session.Err.Detect("pullBlocks -> Started pulling blocks from: shard%dparty%d", shardToUpdate, partyToUpdate)
	}

	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)
	signer, certBytes, err = testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)

	for i := range totalTxNumber {
		txContent := tx.PrepareTxWithTimestamp(i+totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	pullRequestSigner = signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown = common.Status_UNKNOWN
	// Pull blocks to verify all transactions are included
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber*2 + 1, // including config update tx
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})
}
