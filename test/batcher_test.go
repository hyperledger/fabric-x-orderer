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
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// Number of shards in the test
	numOfShards = 4
	// Number of parties in the test
	numOfParties = 4
	// Placeholder for a value that doesn't matter in the test
	doesntMatter = -1
)

func TestBatcherRestartRecover(t *testing.T) {
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	t.Logf("Running test with %d parties and %d shards", numOfParties, numOfShards)

	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)
	// 2.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2"})

	// 3.
	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	// 4. Send To Routers
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
	testutil.WaitForAssemblersReady(t, armaNetwork, parties, totalTxSent, 15)

	// Pull from Assemblers
	PullFromAssemblers(t, uc, parties, 0, math.MaxUint64, totalTxSent, doesntMatter, "cancelled pull from assembler: %d")

	partyToRestart := types.PartyID(3)
	correctParties := []types.PartyID{types.PartyID(1), types.PartyID(2), types.PartyID(4)}
	routerToStall := armaNetwork.GetRouter(t, partyToRestart)

	for shardId := 2; shardId <= 2; shardId++ {
		batcherToStop := armaNetwork.GeBatcher(t, partyToRestart, types.ShardID(shardId))
		isPrimary, _ := gbytes.Say("acting as primary").Match(batcherToStop.RunInfo.Session.Err)

		batcherToStop.StopArmaNode()

		if !isPrimary {
			isSecondary, _ := gbytes.Say("acting as secondary").Match(batcherToStop.RunInfo.Session.Err)
			require.True(t, isSecondary)
		}

		t.Logf("Batcher %d of party %d is down", shardId, partyToRestart)
		stalled := false

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

		// wait for the transactions to be processed by correct assemblers
		testutil.WaitForAssemblersReady(t, armaNetwork, correctParties, totalTxSent, 60)
		if isPrimary {
			testutil.WaitForComplaint(t, armaNetwork, parties, types.ShardID(shardId), 60)
			testutil.WaitForTermChange(t, armaNetwork, parties, types.ShardID(shardId), 60)
		}

		// make sure clients of correct parties continue to get transactions (expect 2000 TXs).
		PullFromAssemblers(t, uc, correctParties, 0, math.MaxUint64, totalTxSent, doesntMatter, "cancelled pull from assembler: %d")
	}

	for shardId := 2; shardId <= 2; shardId++ {
		t.Logf("Restarting Batcher %d of party %d", shardId, partyToRestart)
		// restart the batcher
		batcherToRestart := armaNetwork.GeBatcher(t, partyToRestart, types.ShardID(shardId))
		batcherToRestart.RestartArmaNode(t, readyChan, numOfParties)

		testutil.WaitReady(t, readyChan, 1, 10)

		testutil.WaitForAssemblersReady(t, armaNetwork, []types.PartyID{partyToRestart}, totalTxSent, 15)

		PullFromAssemblers(t, uc, []types.PartyID{partyToRestart}, 0, math.MaxUint64, totalTxSent, doesntMatter, "cancelled pull from assembler: %d")
	}

	broadcastClient = client.NewBroadCastTxClient(uc, 10*time.Second)

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

	totalTxSent += totalTxNumber

	// wait for the transactions to be processed
	testutil.WaitForAssemblersReady(t, armaNetwork, parties, totalTxSent, 15)

	// Pull from Assemblers
	// make sure clients of all the parties get transactions (expect 3000 TXs).
	PullFromAssemblers(t, uc, parties, 0, math.MaxUint64, totalTxSent, doesntMatter, "cancelled pull from assembler: %d")
}
