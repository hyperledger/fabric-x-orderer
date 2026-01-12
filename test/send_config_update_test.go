/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
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

	statusUknown := common.Status_UNKNOWN
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUknown,
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
		BlockHandler: userBlockHandler,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUknown,
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
		BlockHandler: userBlockHandler,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUknown,
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

	statusUnknown := common.Status_UNKNOWN
	// Pull blocks to verify all transactions are included
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      60,
		Status:       &statusUnknown,
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
	})
}
