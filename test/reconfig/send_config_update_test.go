/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package reconfig

import (
	"bytes"
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
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
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	test_utils "github.com/hyperledger/fabric-x-orderer/test/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var caFolders = map[string]struct{}{
	"ca":         {},
	"tlsca":      {},
	"cacerts":    {},
	"tlscacerts": {},
}

func TestUpdatePartyRouterEndpoint(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 2, "none", "none")
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
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
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

	partyToUpdate := submittingParty
	nonUpdatedRouterParties := []types.PartyID{2, 3, 4}
	routerIP := strings.Split(userConfig.RouterEndpoints[partyToUpdate-1], ":")[0] // extract IP from the user config router endpoint
	availablePort, newListener := testutil.SharedTestPortAllocator().Allocate(t)
	newPort, err := strconv.Atoi(availablePort)
	require.NoError(t, err)
	routerToUpdate := armaNetwork.GetRouter(t, submittingParty)
	// Close the previous listener before replacing it
	if routerToUpdate.Listener != nil {
		routerToUpdate.Listener.Close()
	}
	routerToUpdate.Listener = newListener

	configUpdatePbData := configUpdateBuilder.UpdateRouterEndpoint(t, partyToUpdate, routerIP, newPort)

	// Create config tx
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	t.Log("Wait for the router to enter pending admin state and then stop it")
	testutil.WaitForPendingAdminByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Router}, []types.PartyID{partyToUpdate})
	armaNetwork.GetRouter(t, partyToUpdate).StopArmaNode()

	t.Log("Wait for arma nodes to restart dynamically")
	testutil.WaitForRelaunchByType(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher}, 1)
	testutil.WaitForRelaunchByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Router}, nonUpdatedRouterParties, 1)

	// Pull blocks to verify all transactions are included
	userBlockHandler := &verifyRouterEndpointUpdate{updatedParty: partyToUpdate, routerIP: routerIP, newPort: newPort}
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
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

	// Verify the config stored in the router's config store is updated
	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyToUpdate), "local_config_router.yaml")
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

	t.Log("Restart Router")
	armaNetwork.GetRouter(t, partyToUpdate).RestartArmaNode(t, readyChan)

	testutil.WaitReady(t, readyChan, 1, 10)

	// Update the user config with the new router endpoint
	userConfig.RouterEndpoints[partyToUpdate-1] = fmt.Sprintf("%s:%d", routerIP, newPort)
	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)

	t.Log("Send transactions again and verify they are processed")
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
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
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
	armaNetwork.Stop()
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

	observedPrimaryIdByRemovedParty := types.PartyID((uint64(1))%uint64(numOfParties) + 1)
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

// TestChangePartyCertificates verifies that updating a party's certificates via a config update succeeds,
// and that the party can continue processing transactions after the config update with the new certificates.
func TestChangePartyCertificates(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 2
	submittingParty := types.PartyID(2)
	submittingOrg := fmt.Sprintf("org%d", submittingParty)
	partyToUpdate := types.PartyID(1)
	updateOrg := fmt.Sprintf("org%d", partyToUpdate)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)

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
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	parties := make([]types.PartyID, 0, numOfParties)
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	nonUpdatedParties := make([]types.PartyID, 0, len(parties)-1)
	for _, party := range parties {
		if party != partyToUpdate {
			nonUpdatedParties = append(nonUpdatedParties, party)
		}
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
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      120,
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

	t.Logf("Wait for the party %d nodes to enter pending admin state and then stop it", partyToUpdate)
	testutil.WaitForPendingAdminByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, []types.PartyID{partyToUpdate})
	armaNetwork.StopParties([]types.PartyID{partyToUpdate})

	t.Log("Wait for arma nodes to restart dynamically")
	testutil.WaitForRelaunchByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, nonUpdatedParties, 1)

	t.Logf("Restart party %d", partyToUpdate)
	armaNetwork.RestartParties(t, []types.PartyID{partyToUpdate}, readyChan)
	testutil.WaitReady(t, readyChan, 3+numOfShards, 10)

	// Verify that the party's certificates are updated by checking the router's shared config
	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyToUpdate), "local_config_router.yaml")
	routerConfig, _, err := config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)

	var updatedPartyConfig *protos.PartyConfig
	for _, partyConfig := range routerConfig.SharedConfig.GetPartiesConfig() {
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
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber*2 + 1, // including config update tx
		Timeout:      120,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})
}

// TestChangePartyCACertificates tests a party's CA change.
// Scenario:
// 1. Run an arma network of 4 parties, single shard.
// 2. Send txs and pull blocks to ensure network is operational.
// 3. Create a new TLS and signing CA for party 1.
// 4. Send a config tx that appends each CA to the current CA list (TLS and signing) of party 1.
// 5. Wait for dynamic restart of all nodes.
// 6. Send more txs and pull blocks to verify the network is operational again.
// 7. Update party 1 crypto material (TLS and signing certificates) on disk with new certificates.
// 8. Send a config tx that updates all node-level TLS and signing certificates, issued by the new CA's, for party 1.
// 9. Wait for the nodes of party 1 to enter a pending admin state and stop party 1.
// 10. Restart party 1 and wait for dynamic restart of the non-updated parties
// 11. Extend client trust with the new TLS CA and send more txs and pull blocks to verify the network is operational again.
// 12. Update party 1 crypto material on disk with new CAs.
// 13. Send a config tx that updates the CA's list to include only the new CAs (i.e., remove old CAs).
// 14. Wait for dynamic restart of all nodes.
// 15. Send more txs and pull blocks to verify the network is operational again.
func TestChangePartyCACertificates(t *testing.T) {
	// 1.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 1
	submittingParty := types.PartyID(2)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)

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
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	parties := make([]types.PartyID, 0, numOfParties)
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	uc, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)

	// 2.
	txNumber := 10
	totalTxNumber := 0
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	submittingOrg := fmt.Sprintf("org%d", submittingParty)

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
		Timeout:      120,
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// 3.
	configUpdateBuilder, _ := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))

	partyToUpdate := types.PartyID(1)
	nonUpdatedParties := slices.DeleteFunc(slices.Clone(parties), func(partyID types.PartyID) bool {
		return partyID == partyToUpdate
	})

	nodesIPs := testutil.GetNodesIPsFromNetInfo(netInfo)
	require.NotNil(t, nodesIPs)

	var batchersEndpoints []string
	for shardID := types.ShardID(1); int(shardID) <= numOfShards; shardID++ {
		batchersEndpoints = append(batchersEndpoints, netInfo[testutil.NodeName{PartyID: partyToUpdate, NodeType: testutil.Batcher, ShardID: shardID}].Listener.Addr().String())
	}

	networkConfig := &generate.Network{
		Parties: []generate.Party{
			{
				ID:                partyToUpdate,
				RouterEndpoint:    netInfo[testutil.NodeName{PartyID: partyToUpdate, NodeType: testutil.Router}].Listener.Addr().String(),
				ConsenterEndpoint: netInfo[testutil.NodeName{PartyID: partyToUpdate, NodeType: testutil.Consensus}].Listener.Addr().String(),
				BatchersEndpoints: batchersEndpoints,
				AssemblerEndpoint: netInfo[testutil.NodeName{PartyID: partyToUpdate, NodeType: testutil.Assembler}].Listener.Addr().String(),
			},
		},
	}

	updateOrg := fmt.Sprintf("org%d", partyToUpdate)
	configUpdateDir := filepath.Join(dir, "config_update")
	err = os.MkdirAll(configUpdateDir, 0o755)
	require.NoError(t, err, "failed to create config update directory")
	defer os.RemoveAll(configUpdateDir)

	err = armageddon.GenerateCryptoConfig(networkConfig, configUpdateDir)
	require.NoError(t, err, "failed to regenerate crypto config with Armageddon")

	// merge the new crypto config for the updated party to the existing crypto config directory so that the config update builder can pick up the new certs
	copyDir(filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", updateOrg),
		filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg),
		copyCAFilesPredicate,
		true,
	)

	// Append the new TLS CA cert and Sign CA cert to the config update builder
	newTlsCACertPath := filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", updateOrg, "msp", "tlscacerts", "tlsca-cert.pem")
	newTlsCACertBytes, err := os.ReadFile(newTlsCACertPath)
	require.NoError(t, err)
	configUpdateBuilder.AppendPartyTLSCACerts(t, partyToUpdate, [][]byte{newTlsCACertBytes})

	newSignCACertPath := filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", updateOrg, "msp", "cacerts", "ca-cert.pem")
	newSignCACertBytes, err := os.ReadFile(newSignCACertPath)
	require.NoError(t, err)
	configUpdateBuilder.AppendPartyCACerts(t, partyToUpdate, [][]byte{newSignCACertBytes})

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdateBuilder.ConfigUpdatePBData(t))
	require.NotNil(t, env)

	// 4.
	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	totalTxNumber++

	broadcastClient.Stop()

	// 5.
	t.Log("Wait for arma nodes to restart dynamically")
	testutil.WaitForNetworkRelaunch(t, netInfo, 1)

	// Verify that the party's CA certificates are updated by checking the router's shared config
	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyToUpdate), "local_config_router.yaml")
	routerConfig, lastConfigBlock, err := config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)

	routerNodeConfig := routerConfig.ExtractRouterConfig(lastConfigBlock)
	ordererConfig, ok := routerNodeConfig.Bundle.OrdererConfig()
	require.True(t, ok, "failed to extract orderer config from the last config block")

	routerSharedConfig := protos.SharedConfig{}
	err = proto.Unmarshal(ordererConfig.ConsensusMetadata(), &routerSharedConfig)
	require.NoError(t, err)

	var updatedPartyConfig *protos.PartyConfig
	for _, partyConfig := range routerSharedConfig.GetPartiesConfig() {
		if partyConfig.PartyID == uint32(partyToUpdate) {
			updatedPartyConfig = partyConfig
			break
		}
	}
	require.NotNil(t, updatedPartyConfig, "Updated party config not found in the config")

	require.True(t, func() bool {
		for _, cert := range updatedPartyConfig.GetCACerts() {
			if bytes.Equal(cert, newSignCACertBytes) {
				return true
			}
		}
		return false
	}(), "Signing CA certs were not updated in the config")

	require.True(t, func() bool {
		for _, cert := range updatedPartyConfig.TLSCACerts {
			if bytes.Equal(cert, newTlsCACertBytes) {
				return true
			}
		}
		return false
	}(), "TLS CA certs were not updated in the config")

	// 6.
	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}

	// Pull blocks to verify all transactions are included
	pullRequestSigner = signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown = common.Status_UNKNOWN
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	copyDir(filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", updateOrg), filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg),
		copyNonCAFilesPredicate, false,
	)

	// 7.
	newConfigBlockPath := filepath.Join(dir, "config1.block")
	err = configtxgen.WriteOutputBlock(lastConfigBlock, newConfigBlockPath)
	require.NoError(t, err)

	configUpdateBuilder, _ = configutil.NewConfigUpdateBuilder(t, dir, newConfigBlockPath)

	// Update the router TLS certs in the config
	newRouterTlsCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "router", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdateRouterTLSCert(t, partyToUpdate, newRouterTlsCertBytes)

	// Update the batchers TLS certs and signing certs in the config
	for shardToUpdate := types.ShardID(1); int(shardToUpdate) <= numOfShards; shardToUpdate++ {
		newBatcherTlsCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "tls", "tls-cert.pem"))
		require.NoError(t, err)
		configUpdateBuilder.UpdateBatcherTLSCert(t, partyToUpdate, shardToUpdate, newBatcherTlsCertBytes)

		newBatcherSignCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "msp", "signcerts", "sign-cert.pem"))
		require.NoError(t, err)
		configUpdateBuilder.UpdateBatcherSignCert(t, partyToUpdate, shardToUpdate, newBatcherSignCertBytes)
	}

	// Update the assembler TLS certs in the config
	newAssemblerTlsCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "assembler", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdateAssemblerTLSCert(t, partyToUpdate, newAssemblerTlsCertBytes)

	// Update the consenter TLS certs in the config
	newConsenterTlsCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdateConsensusTLSCert(t, partyToUpdate, newConsenterTlsCertBytes)

	// Update the consenter signing certs in the config
	newConsenterSignCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "msp", "signcerts", "sign-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdateConsenterSignCert(t, partyToUpdate, newConsenterSignCertBytes)

	// Submit config update
	env = configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdateBuilder.ConfigUpdatePBData(t))
	require.NotNil(t, env)

	// 8.
	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	totalTxNumber++

	broadcastClient.Stop()

	// 9.
	t.Logf("Wait for party %d to enter pending admin state and then stop it", partyToUpdate)
	testutil.WaitForPendingAdminByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, []types.PartyID{partyToUpdate})
	armaNetwork.StopParties([]types.PartyID{partyToUpdate})

	// 10.
	// Restart the updated party
	armaNetwork.RestartParties(t, []types.PartyID{partyToUpdate}, readyChan)
	testutil.WaitReady(t, readyChan, 3+numOfShards, 10)

	t.Log("Wait for arma nodes to restart dynamically")
	testutil.WaitForRelaunchByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, nonUpdatedParties, 2)

	// Get the config block from router's config store and write it to a temp location
	_, lastConfigBlock, err = config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)
	newConfigBlockPath = filepath.Join(dir, "config2.block")
	err = configtxgen.WriteOutputBlock(lastConfigBlock, newConfigBlockPath)
	require.NoError(t, err)

	// 11.
	// Update the TLS CA certs
	uc.TLSCACerts = append(uc.TLSCACerts, newTlsCACertBytes)

	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)
	signer, certBytes, err = testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}

	// Pull blocks to verify all transactions are included
	pullRequestSigner = signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown = common.Status_UNKNOWN
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// 12.
	configUpdateBuilder, _ = configutil.NewConfigUpdateBuilder(t, dir, newConfigBlockPath)

	oldUC, err := testutil.GetUserConfig(dir, partyToUpdate)
	require.NoError(t, err)
	oldSigner, oldCertBytes, err := testutil.LoadCryptoMaterialsFromDir(t, oldUC.MSPDir)
	require.NoError(t, err)

	// Override the party's crypto materials with the new ones regenerated
	dstDir := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg)
	err = os.RemoveAll(dstDir)
	require.NoError(t, err, "failed to remove directory %s", dstDir)
	copyDir(filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", updateOrg), dstDir, copyAllPredicate, false)

	// Set the new TLS CA cert and Sign CA cert to the config update builder
	tlsCACertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "msp", "tlscacerts", "tlsca-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdatePartyTLSCACerts(t, partyToUpdate, [][]byte{tlsCACertBytes})

	signCACertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "msp", "cacerts", "ca-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdatePartyCACerts(t, partyToUpdate, [][]byte{signCACertBytes})

	// Submit a new config update
	env = configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdateBuilder.ConfigUpdatePBData(t))
	require.NotNil(t, env)

	// 13.
	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	totalTxNumber++

	broadcastClient.Stop()

	// 14.
	testutil.WaitForNetworkRelaunch(t, netInfo, 3)

	// 15.
	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)
	signer, certBytes, err = testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}

	// Pull blocks to verify all transactions are included
	pullRequestSigner = signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown = common.Status_UNKNOWN
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// Try sending a transaction with the old certificate which should fail as the old cert is no longer trusted by the network
	txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
	env = tx.CreateSignedStructuredEnvelope(txContent, oldSigner, oldCertBytes, fmt.Sprintf("org%d", partyToUpdate))
	err = broadcastClient.SendTx(env)
	require.ErrorContains(t, err, "signature did not satisfy policy", "expected error when sending transaction with old certificate after CA rotation, but got no error")

	broadcastClient.Stop()
}

type copyPredicate func(path string, d os.DirEntry) bool

func copyNonCAFilesPredicate(path string, d os.DirEntry) bool {
	if d.IsDir() {
		return false
	}

	dir := filepath.Dir(path)

	if _, ok := caFolders[filepath.Base(dir)]; ok {
		return false
	}
	return true
}

func copyCAFilesPredicate(path string, d os.DirEntry) bool {
	if d.IsDir() {
		return false
	}

	dir := filepath.Dir(path)

	if _, ok := caFolders[filepath.Base(dir)]; ok {
		return true
	}
	return false
}

func copyAllPredicate(_ string, d os.DirEntry) bool {
	return true
}

func copyDir(src, dst string, pred copyPredicate, backUpOriginal bool) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if pred != nil && !pred(path, d) {
			return nil
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		target := filepath.Join(dst, relPath)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if backUpOriginal {
			target = uniqueFileName(target)
		}

		err = armageddon.CopyFile(path, target)
		return err
	})
}

func uniqueFileName(path string) string {
	base := path
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]
	for i := 1; ; i++ {
		if _, err := os.Stat(base); os.IsNotExist(err) {
			return base
		}
		base = name + "-bak" + strconv.Itoa(i) + ext
	}
}

// TestUpdateTimeoutParameters verifies that updating a party's timeout parameters via a config update succeeds,
// and that the party can continue processing transactions after the config update with the new timeout parameters.
// NOTE: as the memory pool options are not updated in dynamic reconfig, this scenario is tested as an admin action requirement.
// TODO: move to dynamic reconfig approach
func TestUpdateTimeoutParameters(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 2, "none", "none")
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
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	userConfig, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

	txNumber := 100
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

	for i := range txNumber {
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

	totalTxNumber := txNumber

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
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// Create config update
	configUpdateBuilder, _ := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))

	autoRemoveTimeout := "100ms"
	configUpdatePbData := configUpdateBuilder.UpdateBatchTimeouts(t, configutil.NewBatchTimeoutsConfig(configutil.BatchTimeoutsConfigName.AutoRemoveTimeout, autoRemoveTimeout))

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	// Restart Arma nodes
	armaNetwork.Stop()

	armaNetwork.Restart(t, readyChan)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	// Send transactions again and verify they are processed
	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)

	for i := range txNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i+txNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	totalTxNumber += txNumber

	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber + 1, // including config update tx
		BlockHandler: &verifyTimeoutParam{
			AutoRemoveTimeout: autoRemoveTimeout,
		},
		Timeout:   60,
		ErrString: "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:    &statusUnknown,
		Signer:    pullRequestSigner,
	})
}

type verifyTimeoutParam struct {
	AutoRemoveTimeout string
}

func (vt *verifyTimeoutParam) HandleBlock(t *testing.T, block *common.Block) error {
	isGenesisBlock := block.Header.Number == 0 || block.Header.GetDataHash() == nil
	if isGenesisBlock {
		return nil
	}

	if protoutil.IsConfigBlock(block) {
		envelope, err := configutil.ReadConfigEnvelopeFromConfigBlock(block)
		if err != nil || envelope == nil {
			return fmt.Errorf("failed to read config envelope from config block: %w", err)
		}

		sharedConfig := configutil.GetSharedConfig(t, envelope)
		require.NotNil(t, sharedConfig)

		if vt.AutoRemoveTimeout != sharedConfig.BatchingConfig.BatchTimeouts.AutoRemoveTimeout {
			return fmt.Errorf("AutoRemoveTimeout in the config block does not match the expected value. Expected: %s, Got: %s", vt.AutoRemoveTimeout, sharedConfig.BatchingConfig.BatchTimeouts.AutoRemoveTimeout)
		}
		return nil
	}

	return nil
}

// TestUpdateSmartBFTParameters verifies that updating SmartBFT parameters via a config update succeeds,
// and that the network can continue processing transactions after the config update with the new parameters.
func TestUpdateSmartBFTParameters(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 2, "none", "none")
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
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	userConfig, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

	txNumber := 100

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

	for i := range txNumber {
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

	totalTxNumber := txNumber

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
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// Create config update
	configUpdateBuilder, _ := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))

	requestBatchMaxBytes := uint64(1048576)
	configUpdatePbData := configUpdateBuilder.UpdateSmartBFTConfig(t, configutil.NewSmartBFTConfig(configutil.SmartBFTConfigName.RequestBatchMaxBytes, strconv.FormatUint(requestBatchMaxBytes, 10)))

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	t.Log("Wait for arma nodes to restart dynamically")
	testutil.WaitForNetworkRelaunch(t, netInfo, 1)

	// Send transactions again and verify they are processed
	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)

	for i := range txNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i+txNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	totalTxNumber += txNumber

	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber + 1, // including config update tx
		BlockHandler: &verifySmartBFTParam{
			RequestBatchMaxBytes: requestBatchMaxBytes,
		},
		Timeout:   60,
		ErrString: "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:    &statusUnknown,
		Signer:    pullRequestSigner,
	})
}

type verifySmartBFTParam struct {
	RequestBatchMaxBytes uint64
}

func (vt *verifySmartBFTParam) HandleBlock(t *testing.T, block *common.Block) error {
	isGenesisBlock := block.Header.Number == 0 || block.Header.GetDataHash() == nil
	if isGenesisBlock {
		return nil
	}

	if protoutil.IsConfigBlock(block) {
		envelope, err := configutil.ReadConfigEnvelopeFromConfigBlock(block)
		if err != nil || envelope == nil {
			return fmt.Errorf("failed to read config envelope from config block: %w", err)
		}

		sharedConfig := configutil.GetSharedConfig(t, envelope)
		require.NotNil(t, sharedConfig)

		if vt.RequestBatchMaxBytes != sharedConfig.ConsensusConfig.SmartBFTConfig.RequestBatchMaxBytes {
			return fmt.Errorf("RequestBatchMaxBytes in the config block does not match the expected value. Expected: %d, Got: %d", vt.RequestBatchMaxBytes, sharedConfig.ConsensusConfig.SmartBFTConfig.RequestBatchMaxBytes)
		}
		return nil
	}

	return nil
}

// TestUpdateBatchingParameters verifies that updating a party's batching parameters via a config update succeeds,
// and that the party can continue processing transactions after the config update with the new batching parameters.
// NOTE: as the memory pool options are not updated in dynamic reconfig, this scenario is tested as an admin action requirement.
// TODO: move to dynamic reconfig approach
func TestUpdateBatchingParameters(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 2, "none", "none")
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
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	userConfig, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

	txNumber := 100
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

	// Create config update
	configUpdateBuilder, _ := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))

	maxMessageCount := 2
	configUpdatePbData := configUpdateBuilder.UpdateBatchSizeConfig(t, configutil.NewBatchSizeConfig(configutil.BatchSizeConfigName.MaxMessageCount, maxMessageCount))

	var parties []types.PartyID
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	// Restart Arma nodes
	armaNetwork.Stop()

	armaNetwork.Restart(t, readyChan)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	// Send transactions again and verify they are processed
	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)

	for i := range txNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, fmt.Sprintf("org%d", submittingParty))
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	totalTxNumber := txNumber

	pullRequestSigner := signutil.CreateTestSigner(t, "org1", dir)
	statusUnknown := common.Status_UNKNOWN

	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber + 1, // including config update tx
		BlockHandler: &verifyMaxMessageCount{MaxMessageCount: maxMessageCount},
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})
}

type verifyMaxMessageCount struct {
	MaxMessageCount int
}

func (vm *verifyMaxMessageCount) HandleBlock(t *testing.T, block *common.Block) error {
	isGenesisBlock := block.Header.Number == 0 || block.Header.GetDataHash() == nil
	if isGenesisBlock {
		return nil
	}

	if protoutil.IsConfigBlock(block) {
		envelope, err := configutil.ReadConfigEnvelopeFromConfigBlock(block)
		if err != nil || envelope == nil {
			return fmt.Errorf("failed to read config envelope from config block: %w", err)
		}

		sharedConfig := configutil.GetSharedConfig(t, envelope)
		require.NotNil(t, sharedConfig)

		require.Equal(t, vm.MaxMessageCount, int(sharedConfig.BatchingConfig.BatchSize.MaxMessageCount), "MaxMessageCount in the config block does not match the expected value")
		return nil
	}

	if len(block.GetData().GetData()) > vm.MaxMessageCount {
		return fmt.Errorf("block contains %d transactions, which exceeds the max message count per block of %d", len(block.GetData().GetData()), vm.MaxMessageCount)
	}

	return nil
}

// TestReplacePartiesPartially verifies that a blockchain network can dynamically replace parties
// by removing one party and adding multiple new parties in successive configuration updates.
// 1. Sets up a network of 5 parties with mTLS configuration
// 2. Generates Arma node configurations and starts all nodes
// 3. Sends initial transactions to verify network operability
// 4. Creates and applies a configuration update to remove party 1
// 5. Restarts the network with the reduced party set
// 6. Iteratively (4 times):
//   - Adds a new party to the network through a configuration update
//   - Restarts all nodes with the updated configuration
//   - Updates user configurations across all parties to include new endpoints and TLS certificates
//
// 7. Verifies the final network can process transactions with all accumulated configuration changes
func TestReplacePartiesPartially(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
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

	configBlockStoreDir := t.TempDir()
	defer os.RemoveAll(configBlockStoreDir)

	configBlockPath := filepath.Join(dir, "bootstrap", "bootstrap.block")
	builder, _ := configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
	partyToRemove := types.PartyID(1)
	builder.RemoveParty(t, partyToRemove)

	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

	// Send the config tx
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingPartyID), builder.ConfigUpdatePBData(t))
	require.NotNil(t, env)

	err = broadcastClient.SendTxTo(env, submittingPartyID)
	require.NoError(t, err)
	totalTxNumber++ // for the config update transaction

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	broadcastClient.Stop()
	// Stop Arma nodes
	armaNetwork.Stop()

	// Remove the removed party from the network info and parties list
	maps.DeleteFunc(netInfo, func(nodeName testutil.NodeName, _ *testutil.ArmaNodeInfo) bool {
		return nodeName.PartyID == partyToRemove
	})
	// Remove the removed party from the list of parties
	parties = slices.DeleteFunc(parties, func(partyID types.PartyID) bool {
		return partyID == partyToRemove
	})

	sharedConfig := config.SharedConfigYaml{}
	err = utils.ReadFromYAML(&sharedConfig, filepath.Join(dir, "bootstrap", "shared_config.yaml"))
	require.NoError(t, err, "failed to load shared config")
	// Remove the removed party from the shared config
	sharedConfig.PartiesConfig = slices.DeleteFunc(sharedConfig.PartiesConfig, func(partyConfig config.PartyConfig) bool {
		return partyConfig.PartyID == partyToRemove
	})
	// Write the updated shared config back to disk
	err = utils.WriteToYAML(sharedConfig, filepath.Join(dir, "bootstrap", "shared_config.yaml"))
	require.NoError(t, err, "failed to write updated shared config")
	// Remove the crypto materials of the removed party
	err = os.RemoveAll(filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyToRemove)))
	require.NoError(t, err)

	submittingPartyID = parties[0]
	configBlockPath = filepath.Join(configBlockStoreDir, fmt.Sprintf("config_%d.block", submittingPartyID))
	_, lastConfigBlock, err := config.ReadConfig(filepath.Join(dir, "config", fmt.Sprintf("party%d", submittingPartyID), "local_config_assembler.yaml"), flogging.MustGetLogger("TestAddNewParty"))
	require.NoError(t, err)
	err = configtxgen.WriteOutputBlock(lastConfigBlock, configBlockPath)
	require.NoError(t, err)

	numOfArmaNodes = len(netInfo)
	readyChan = make(chan string, numOfArmaNodes)
	armaNetwork = testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	for range 4 {
		// Create config update to add a new party
		builder, _ = configutil.NewConfigUpdateBuilder(t, dir, configBlockPath)
		addedPartyId, addedNetInfo := builder.PrepareAndAddNewParty(t, dir)

		uc, err = testutil.GetUserConfig(dir, submittingPartyID)
		require.NoError(t, err)

		broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

		// Send the config tx
		env = configutil.CreateConfigTX(t, dir, parties, int(submittingPartyID), builder.ConfigUpdatePBData(t))
		require.NotNil(t, env)

		err = broadcastClient.SendTxTo(env, submittingPartyID)
		require.NoError(t, err)
		totalTxNumber++ // for the config update transaction

		// Wait for Arma nodes to stop
		testutil.WaitSoftStopped(t, netInfo)

		broadcastClient.Stop()
		// Stop Arma nodes
		armaNetwork.Stop()

		configBlockPath = filepath.Join(configBlockStoreDir, fmt.Sprintf("config_%d.block", submittingPartyID))
		// Read the last config block to get the updated config with the new party
		_, lastConfigBlock, err = config.ReadConfig(filepath.Join(dir, "config", fmt.Sprintf("party%d", submittingPartyID), "local_config_assembler.yaml"), flogging.MustGetLogger("TestAddNewParty"))
		require.NoError(t, err)
		// Write the last config block to a separate location to be used for starting the Arma nodes with the updated config
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

		// Stop Arma nodes
		armaNetwork.Stop()
		// Restart nodes
		armaNetwork = testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
		testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)
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

// TestSubmitReceiveAndVerifySignaturesConfigBlock tests the end-to-end process of submitting a configuration
// transaction, receiving the resulting configuration block, and verifying its signatures.
func TestSubmitReceiveAndVerifySignaturesConfigBlock(t *testing.T) {
	// 1. Builds the arma binary and creates a temporary directory for test artifacts
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	numOfShards := 1
	numOfParties := 1

	// create temp dir
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 2. Creates a network configuration with the specified number of shards and parties
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)
	numOfArmaNodes := len(netInfo)

	// 3. Generates necessary cryptographic materials and network artifacts using the arma CLI
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// 4. Starts arma nodes and waits for them to be ready
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	require.NoError(t, err)
	require.NotNil(t, uc)

	logger := testutil.CreateLogger(t, 0)

	// 5. Builds a signature verifier from the consenter information
	verifier := test_utils.BuildVerifier(dir, types.PartyID(1), logger)

	// 6. Creates a broadcast client to submit transactions and submits a configuration transaction based on the genesis block
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	// Create config tx
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	submittingPartyID := 1
	configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, genesisBlockPath)
	defer cleanUp()

	configUpdatePbData := configUpdateBuilder.UpdateBatchSizeConfig(t, configutil.NewBatchSizeConfig(configutil.BatchSizeConfigName.MaxMessageCount, 500))
	require.NotEmpty(t, configUpdatePbData)

	env := configutil.CreateConfigTX(t, dir, []types.PartyID{1}, submittingPartyID, configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTx(env)
	require.NoError(t, err)
	totalBlocks := 2 // genesis + config block

	// 7. Pulls and verifies blocks from assemblers, ensuring the configuration block's signatures are valid
	statusSuccess := common.Status_UNKNOWN
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig: uc,
		Parties:    []types.PartyID{types.PartyID(submittingPartyID)},
		StartBlock: uint64(0),
		Status:     &statusSuccess,
		Verifier:   verifier,
		Blocks:     totalBlocks,
		ErrString:  "cancelled pull from assembler: %d",
		LogString:  "configuration block 1 partyID %d verified with",
		Signer:     signutil.CreateTestSigner(t, "org1", dir),
	})
}
