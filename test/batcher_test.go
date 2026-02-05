/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	config "github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// Number of parties in the test
	numOfParties = 4
)

// Simulates a scenario where the primary batcher
// is stopped and then restarted, while ensuring that the system can recover and
// continue processing transactions. The test involves the following steps:
//  1. Compile and run the arma nodes.
//  2. Submit a batch of transactions to the network.
//  3. Identify and stop the primary batcher for one of the parties.
//  4. Continue submitting transactions, expecting the stopped party's router to stall.
//  5. Verify that the correct number of transactions have been processed by the assemblers.
//  6. Restart the stopped primary batcher and ensure the stalled transactions are processed.
//  7. Submit additional transactions and verify that all parties receive the expected number
//     of transactions.
func TestPrimaryBatcherRestartRecover(t *testing.T) {
	// 1. compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	t.Logf("Running test with %d parties and %d shards", numOfParties, 1)

	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 1, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	// 2. Send To Routers
	totalTxNumber := 1000
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	rate := 500
	totalTxSent := 0

	capacity := rate / fillFrequency
	rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter, err: %v\n", err)
		os.Exit(3)
	}

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	t.Log("Finished submit")
	broadcastClient.Stop()

	parties := []types.PartyID{}
	for partyID := 1; partyID <= numOfParties; partyID++ {
		parties = append(parties, types.PartyID(partyID))
	}

	totalTxSent += totalTxNumber

	// Pull from Assemblers
	infos := PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:       uc,
		Parties:          parties,
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxSent,
		Timeout:          60,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})

	// Get the primary batcher
	primaryBatcherId := infos[types.PartyID(1)].Primary[types.ShardID(1)]
	primaryBatcher := armaNetwork.GetBatcher(t, primaryBatcherId, types.ShardID(1))

	// 3. Stop the primary batcher
	t.Logf("Stopping primary batcher: party %d", primaryBatcher.PartyId)
	primaryBatcher.StopArmaNode()

	stalled := false
	routerToStall := armaNetwork.GetRouter(t, primaryBatcher.PartyId)

	// 4.
	// make sure 2f+1 routers are receiving TXs w/o problems
	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(totalTxSent+i, 64, []byte("sessionNumber"))
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		if err != nil {
			require.ErrorContains(t, err, fmt.Sprintf("received error response from %s: INTERNAL_SERVER_ERROR", routerToStall.Listener.Addr().String()))
			stalled = true
		}
	}

	// make sure the router of the faulty party got stalled
	require.True(t, stalled, "expected router to stall but it did not")
	broadcastClient.Stop()

	totalTxSent += totalTxNumber

	// 5.
	// make sure assemblers of correct parties continue to get transactions (expect 2000 TXs).

	correctParties := []types.PartyID{}
	for partyID := 1; partyID <= numOfParties; partyID++ {
		if primaryBatcherId != types.PartyID(partyID) {
			correctParties = append(correctParties, types.PartyID(partyID))
		}
	}
	infos = PullFromAssemblers(t, &BlockPullerOptions{
		Parties:          correctParties,
		UserConfig:       uc,
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxSent,
		Timeout:          60,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})

	// check that the primary batcher has changed
	require.True(t, infos[correctParties[0]].TermChanged, "expected primary batcher not to remain the same")

	// 6.
	t.Logf("Restarting Batcher: party %d", primaryBatcher.PartyId)
	// restart the batcher
	primaryBatcher.RestartArmaNode(t, readyChan)

	testutil.WaitReady(t, readyChan, 1, 10)

	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:       uc,
		Parties:          []types.PartyID{primaryBatcher.PartyId},
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxSent,
		Timeout:          60,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})

	// 7.
	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(totalTxSent+i, 64, []byte("sessionNumber"))
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to send tx %d: %v", totalTxSent+i, err)
			// we expect the batcher to be restarted and the router to be back online
			// so we should not get an error here
			// however, if the primary batcher is not restarted, we expect an error
			// with INTERNAL_SERVER_ERROR from the router
			require.ErrorContains(t, err, fmt.Sprintf("received error response from %s: INTERNAL_SERVER_ERROR", routerToStall.Listener.Addr().String())) // only such errors are permitted
		}
	}

	t.Log("Finished submit")
	broadcastClient.Stop()

	totalTxSent += totalTxNumber

	// Pull from Assemblers
	// make sure assemblers of all the parties get transactions (expect 3000 TXs).
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:       uc,
		Parties:          parties,
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxSent,
		Timeout:          60,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})
}

// Simulates a scenario where a secondary batcher node is stopped and restarted.
// The test ensures that even after stopping a secondary batcher, the system continues to process transactions
// correctly, and once the node is restarted, it recovers and resumes normal operation. The steps include:
// 1. Compile and run the arma nodes.
// 2. Submit a batch of transactions to the network.
// 3. Identify and stop the secondary batcher for one of the parties.
// 4. Continue submitting transactions, expecting the stopped party's router to stall.
// 5. Verify that the correct number of transactions have been processed by the assemblers.
// 6. Restart the stopped secondary batcher and ensure the stalled transactions are processed.
// 7. Submit additional transactions and verify that all parties receive the expected number
//    of transactions.

func TestSecondaryBatcherRestartRecover(t *testing.T) {
	// 1. compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	t.Logf("Running test with %d parties and %d shards", numOfParties, 1)

	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 1, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	// 2. Send To Routers
	totalTxNumber := 1000
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	rate := 500
	totalTxSent := 0

	capacity := rate / fillFrequency
	rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter, err: %v\n", err)
		os.Exit(3)
	}

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)

	for i := range totalTxNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	t.Log("Finished submit")
	broadcastClient.Stop()

	parties := []types.PartyID{}
	for partyID := 1; partyID <= numOfParties; partyID++ {
		parties = append(parties, types.PartyID(partyID))
	}

	totalTxSent += totalTxNumber

	// Pull from Assemblers
	infos := PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:       uc,
		Parties:          parties,
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxSent,
		Timeout:          60,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})

	primaryBatcherId := infos[types.PartyID(1)].Primary[types.ShardID(1)]
	correctParties := []types.PartyID{}

	var secondaryBatcher *testutil.ArmaNodeInfo = nil

	// 3. Identify and stop the secondary batcher
	for partyId := 1; partyId <= numOfParties; partyId++ {
		if primaryBatcherId != types.PartyID(partyId) && secondaryBatcher == nil {
			secondaryBatcher = armaNetwork.GetBatcher(t, types.PartyID(partyId), types.ShardID(1))
		} else {
			correctParties = append(correctParties, types.PartyID(partyId))
		}
	}

	require.NotNil(t, secondaryBatcher, "Secondary batcher not found for party %d", secondaryBatcher.PartyId)

	t.Logf("Stopping secondary batcher: party %d", secondaryBatcher.PartyId)
	secondaryBatcher.StopArmaNode()

	stalled := false
	routerToStall := armaNetwork.GetRouter(t, secondaryBatcher.PartyId)

	// 4. Send To Routers
	// make sure 2f+1 routers are receiving TXs w/o problems
	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(totalTxSent+i, 64, []byte("sessionNumber"))
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		if err != nil {
			require.ErrorContains(t, err, fmt.Sprintf("received error response from %s: INTERNAL_SERVER_ERROR", routerToStall.Listener.Addr().String()))
			stalled = true
		}
	}

	// make sure the router of the faulty party got stalled
	require.True(t, stalled, "expected router to stall but it did not")
	broadcastClient.Stop()

	totalTxSent += totalTxNumber

	// 5.
	// make sure assemblers of correct parties continue to get transactions (expect 2000 TXs).
	infos = PullFromAssemblers(t, &BlockPullerOptions{
		Parties:          correctParties,
		UserConfig:       uc,
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxSent,
		Timeout:          60,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})

	// make sure the primary batcher did not change
	require.False(t, infos[correctParties[0]].TermChanged, "expected primary batcher to remain the same")

	// 6.
	t.Logf("Restarting Batcher %d of party %d", secondaryBatcher.PartyId, secondaryBatcher.PartyId)
	// restart the batcher
	secondaryBatcher.RestartArmaNode(t, readyChan)

	testutil.WaitReady(t, readyChan, 1, 10)

	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:       uc,
		Parties:          []types.PartyID{secondaryBatcher.PartyId},
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxSent,
		Timeout:          60,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})

	// 7.
	// make sure 2f+1 routers are receiving TXs w/o problems
	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(totalTxSent+i, 64, []byte("sessionNumber"))
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		if err != nil {
			require.ErrorContains(t, err, fmt.Sprintf("received error response from %s: INTERNAL_SERVER_ERROR", routerToStall.Listener.Addr().String())) // only such errors are permitted
		}
	}

	t.Log("Finished submit")
	broadcastClient.Stop()

	totalTxSent += totalTxNumber

	// Pull from Assemblers
	// make sure assemblers of all the parties get transactions (expect 3000 TXs).
	PullFromAssemblers(t, &BlockPullerOptions{
		Parties:          parties,
		UserConfig:       uc,
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxSent,
		Timeout:          60,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})
}

// TestVerifySignedTxsByBatcherSingleParty verifies that a single-party batcher network correctly processes
// signed transactions with client signature verification enabled.
func TestVerifySignedTxsByBatcherSingleParty(t *testing.T) {
	// 1. compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	t.Logf("Running test with %d parties and %d shards", 1, 1)

	// 2. Create a temporary directory for the test.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	shardsNumber := 2

	// 3. Create a config YAML file in the temporary directory.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 1, shardsNumber, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	// 4. Generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	for shardID := range shardsNumber {
		configFilePath := filepath.Join(dir, fmt.Sprintf("config/party%d/local_config_batcher%d.yaml", types.PartyID(1), types.ShardID(shardID+1)))
		conf, _, err := config.LoadLocalConfig(configFilePath)
		require.NoError(t, err)

		// Modify the batcher configuration to require client signature verification.
		conf.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = true
		utils.WriteToYAML(conf.NodeLocalConfig, configFilePath)
	}

	// 5. Run the arma nodes.
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	// Obtains a test user configuration and constructs a broadcast client.
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, types.PartyID(1))
	assert.NoError(t, err)
	assert.NotNil(t, uc)

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

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)

	org := fmt.Sprintf("org%d", types.PartyID(1))

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

	broadcastClient.Stop()

	// 6. make sure assemblers of all the parties get transactions (expect 1000 TXs).
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:       uc,
		Parties:          []types.PartyID{1},
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxNumber,
		Timeout:          60,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})

	// 7. Verify that the batchers have received the transactions.
	txsReceived := 0

	for shardId := range shardsNumber {
		batcherToMonitor := armaNetwork.GetBatcher(t, types.PartyID(1), types.ShardID(shardId+1))
		url := testutil.CaptureArmaNodePrometheusServiceURL(t, batcherToMonitor)

		pattern := fmt.Sprintf(`batcher_router_txs_total\{party_id="%d",shard_id="%d"\} \d+`, types.PartyID(1), types.ShardID(shardId+1))
		regex := regexp.MustCompile(pattern)
		txsReceived += testutil.FetchPrometheusMetricValue(t, regex, url)
	}

	require.GreaterOrEqual(t, txsReceived, totalTxNumber, "Expected %d transactions to be received by the batchers, but got %d", totalTxNumber, txsReceived)
}

// TestVerifySignedTxsByBatcherForwardRequest tests the batcher's ability to forward
// signed transactions from a non-primary batcher to a primary batcher while verifying
// client signatures.
func TestVerifySignedTxsByBatcherForwardRequest(t *testing.T) {
	// 1. compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	numOfParties := 2
	numShards := 1

	t.Logf("Running test with %d parties and %d shards", numOfParties, numShards)

	// 2. Create a temporary directory for the test.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 3. Create a config YAML file in the temporary directory.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numShards, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	// 4. Generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	for partyID := range numOfParties {
		configFilePath := filepath.Join(dir, fmt.Sprintf("config/party%d/local_config_batcher%d.yaml", partyID+1, types.ShardID(1)))
		conf, _, err := config.LoadLocalConfig(configFilePath)
		require.NoError(t, err)

		// Modify the batcher configuration to require client signature verification.
		conf.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = true
		utils.WriteToYAML(conf.NodeLocalConfig, configFilePath)
	}

	// 5. Run the arma nodes.
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	// Obtains a test user configuration and constructs a broadcast client.
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, types.PartyID(1))
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	txNumber := 10
	// rate limiter parameters
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	rate := 100

	capacity := rate / fillFrequency
	rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
		os.Exit(3)
	}

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)

	// 6. Determine primary and non-primary party IDs for shard 1 by calculating the primary party ID using the formula:
	// types.PartyID((uint64(b.Shard) + term) % uint64(b.N)) where term is assumed to be 0 and b.N is the number of parties.
	primaryPartyID := types.PartyID((uint64(1))%uint64(numOfParties) + 1)
	nonPrimaryPartyID := types.PartyID(uint64(primaryPartyID)%uint64(numOfParties) + 1)

	t.Logf("Non-primary party ID is %d", nonPrimaryPartyID)
	t.Logf("Primary party ID is %d", primaryPartyID)

	// 7. Send TXs to a non-primary batcher
	for i := range txNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, "org1")
		err = broadcastClient.SendTxTo(env, nonPrimaryPartyID)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	// 8. Verify that the non-primary batcher have received all the transactions.
	nonPrimaryBatcherToMonitor := armaNetwork.GetBatcher(t, nonPrimaryPartyID, types.ShardID(1))
	url := testutil.CaptureArmaNodePrometheusServiceURL(t, nonPrimaryBatcherToMonitor)

	pattern := fmt.Sprintf(`batcher_router_txs_total\{party_id="%d",shard_id="%d"\} \d+`, nonPrimaryPartyID, types.ShardID(1))
	re := regexp.MustCompile(pattern)

	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, url) == txNumber
	}, 30*time.Second, 100*time.Millisecond)

	primaryBatcherToMonitor := armaNetwork.GetBatcher(t, primaryPartyID, types.ShardID(1))
	url = testutil.CaptureArmaNodePrometheusServiceURL(t, primaryBatcherToMonitor)

	// 9. Verify that the non-primary batcher have forwarded all the transactions to the primary batcher and the primary one have batched all the transactions.
	pattern = fmt.Sprintf(`batcher_batched_txs_total\{party_id="%d",shard_id="%d"\} \d+`, primaryPartyID, types.ShardID(1))
	re = regexp.MustCompile(pattern)

	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, url) == txNumber
	}, 30*time.Second, 100*time.Millisecond)

	// 10. Verify that the non-primary batcher did not receive any transactions.
	pattern = fmt.Sprintf(`batcher_router_txs_total\{party_id="%d",shard_id="%d"\} \d+`, primaryPartyID, types.ShardID(1))
	re = regexp.MustCompile(pattern)

	txsReceived := testutil.FetchPrometheusMetricValue(t, re, url)
	require.Equal(t, txsReceived, 0, "Expected %d transactions to be received by the non-primary batcher, but got %d", 0, txsReceived)
}
