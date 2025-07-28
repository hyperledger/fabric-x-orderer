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
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// Number of shards in the test
	numOfShards = 1
	// Number of parties in the test
	numOfParties = 4
)

// Simulates a scenario where the primary batcher
// is stopped and then restarted, while ensuring that the system can recover and
// continue processing transactions. The test involves the following steps:
// 1. Compile and run the arma nodes.
// 2. Submit a batch of transactions to the network.
// 3. Identify and stop the primary batcher for one of the parties.
// 4. Continue submitting transactions, expecting the stopped party's router to stall.
// 5. Verify that the correct number of transactions have been processed by the assemblers.
// 6. Restart the stopped primary batcher and ensure the stalled transactions are processed.
// 7. Submit additional transactions and verify that all parties receive the expected number
//    of transactions.

func TestPrimaryBatcherRestartRecover(t *testing.T) {
	// 1. compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	t.Logf("Running test with %d parties and %d shards", numOfParties, numOfShards)

	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2"})

	readyChan := make(chan struct{}, numOfArmaNodes)
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
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
		os.Exit(3)
	}

	broadcastClient := client.NewBroadCastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := prepareTx(i, 64, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
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
	blockInfos := PullFromAssemblers(t, uc, parties, 0, math.MaxUint64, totalTxSent, -1, "cancelled pull from assembler: %d")

	// Get the primary batcher
	partyBlockInfos := blockInfos[types.PartyID(1)]
	primaryBatcherId := partyBlockInfos[len(partyBlockInfos)-1].Primary()
	primaryBatcher := armaNetwork.GeBatcher(t, primaryBatcherId, types.ShardID(1))
	correctParties := []types.PartyID{}

	// 3. Stop the primary batcher
	t.Logf("Stopping primary batcher: party %d", primaryBatcher.PartyId)
	primaryBatcher.StopArmaNode()

	for partyID := 1; partyID <= numOfParties; partyID++ {
		if primaryBatcherId != types.PartyID(partyID) {
			correctParties = append(correctParties, types.PartyID(partyID))
		}
	}

	stalled := false
	routerToStall := armaNetwork.GetRouter(t, primaryBatcher.PartyId)

	// 4.
	// make sure 2f+1 routers are receiving TXs w/o problems
	broadcastClient = client.NewBroadCastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := prepareTx(i, 64, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
		if err != nil {
			require.ErrorContains(t, err, fmt.Sprintf("received error response from %s: INTERNAL_SERVER_ERROR", routerToStall.Listener.Addr().String()))
			stalled = true
		}
	}

	// test that the router of party get stalled in the some point
	require.True(t, stalled, "expected router to stall but it did not")
	broadcastClient.Stop()

	totalTxSent += totalTxNumber

	// 5.
	// make sure clients of correct parties continue to get transactions (expect 2000 TXs).
	blockInfos = PullFromAssemblers(t, uc, correctParties, 0, math.MaxUint64, totalTxSent, -1, "cancelled pull from assembler: %d")
	partyBlockInfos = blockInfos[correctParties[0]]
	newPrimaryBatcherId := partyBlockInfos[len(partyBlockInfos)-1].Primary()

	// check that the primary batcher has changed
	require.NotEqual(t, primaryBatcherId, newPrimaryBatcherId, "expected primary batcher not to remain the same")

	// 6.
	t.Logf("Restarting Batcher: party %d", primaryBatcher.PartyId)
	// restart the batcher
	primaryBatcher.RestartArmaNode(t, readyChan, numOfParties)

	testutil.WaitReady(t, readyChan, 1, 10)

	PullFromAssemblers(t, uc, []types.PartyID{primaryBatcher.PartyId}, 0, math.MaxUint64, totalTxSent, -1, "cancelled pull from assembler: %d")

	// 7.
	broadcastClient = client.NewBroadCastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := prepareTx(i, 64, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
		if err != nil {
			require.ErrorContains(t, err, fmt.Sprintf("received error response from %s: INTERNAL_SERVER_ERROR", routerToStall.Listener.Addr().String())) // only such errors are permitted
		}
	}

	t.Log("Finished submit")
	broadcastClient.Stop()

	totalTxSent += totalTxNumber

	// Pull from Assemblers
	// make sure clients of all the parties get transactions (expect 3000 TXs).
	PullFromAssemblers(t, uc, parties, 0, math.MaxUint64, totalTxSent, -1, "cancelled pull from assembler: %d")
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

	t.Logf("Running test with %d parties and %d shards", numOfParties, numOfShards)

	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2"})

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, numOfArmaNodes)
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
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
		os.Exit(3)
	}

	broadcastClient := client.NewBroadCastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := prepareTx(i, 64, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
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
	blockInfos := PullFromAssemblers(t, uc, parties, 0, math.MaxUint64, totalTxSent, -1, "cancelled pull from assembler: %d")

	partyBlockInfos := blockInfos[types.PartyID(1)]
	primaryBatcherId := partyBlockInfos[len(partyBlockInfos)-1].Primary()
	correctParties := []types.PartyID{}

	var secondaryBatcher *testutil.ArmaNodeInfo = nil

	// 3. Identify and stop the secondary batcher
	for partyId := 1; partyId <= numOfParties; partyId++ {
		if primaryBatcherId != types.PartyID(partyId) && secondaryBatcher == nil {
			secondaryBatcher = armaNetwork.GeBatcher(t, types.PartyID(partyId), types.ShardID(1))
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
	broadcastClient = client.NewBroadCastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := prepareTx(i, 64, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
		if err != nil {
			require.ErrorContains(t, err, fmt.Sprintf("received error response from %s: INTERNAL_SERVER_ERROR", routerToStall.Listener.Addr().String()))
			stalled = true
		}
	}

	// test that the router of party get stalled in the some point
	require.True(t, stalled, "expected router to stall but it did not")
	broadcastClient.Stop()

	totalTxSent += totalTxNumber

	// 5.
	// make sure clients of correct parties continue to get transactions (expect 2000 TXs).
	blockInfos = PullFromAssemblers(t, uc, correctParties, 0, math.MaxUint64, totalTxSent, -1, "cancelled pull from assembler: %d")
	partyBlockInfos = blockInfos[correctParties[0]]
	newPrimaryBatcherId := partyBlockInfos[len(partyBlockInfos)-1].Primary()

	// make sure the primary batcher did not change
	require.Equal(t, primaryBatcherId, newPrimaryBatcherId, "expected primary batcher to remain the same")

	// 6.
	t.Logf("Restarting Batcher %d of party %d", secondaryBatcher.PartyId, secondaryBatcher.PartyId)
	// restart the batcher
	secondaryBatcher.RestartArmaNode(t, readyChan, numOfParties)

	testutil.WaitReady(t, readyChan, 1, 10)

	PullFromAssemblers(t, uc, []types.PartyID{secondaryBatcher.PartyId}, 0, math.MaxUint64, totalTxSent, -1, "cancelled pull from assembler: %d")

	// 7.
	// make sure 2f+1 routers are receiving TXs w/o problems
	broadcastClient = client.NewBroadCastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := prepareTx(i, 64, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
		if err != nil {
			require.ErrorContains(t, err, fmt.Sprintf("received error response from %s: INTERNAL_SERVER_ERROR", routerToStall.Listener.Addr().String())) // only such errors are permitted
		}
	}

	t.Log("Finished submit")
	broadcastClient.Stop()

	totalTxSent += totalTxNumber

	// Pull from Assemblers
	// make sure clients of all the parties get transactions (expect 3000 TXs).
	PullFromAssemblers(t, uc, parties, 0, math.MaxUint64, totalTxSent, -1, "cancelled pull from assembler: %d")
}
