/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Scenario:
// 1. Create a config YAML file to be an input to armageddon
// 2. Run armageddon generate command to create config files in a folder structure
// 3. Run arma with the generated config files to run each of the nodes for all parties
// 4. Submit 500 to all routers
// 5. In parallel, run armageddon receive command to pull blocks from the assembler and report results
func TestSubmitAndReceive(t *testing.T) {
	type networkParams struct {
		numOfShards, numOfParties int
	}
	// Define the network parameters for the test cases
	// The first two test cases will always run, the rest will be randomly selected
	// from the sometimes slice.
	always := []networkParams{
		{2, 4},
		{2, 7},
	}
	sometimes := []networkParams{
		{1, 1},
		{2, 1},
		{4, 1},
		{8, 1},
		{1, 4},
		{4, 4},
		{8, 4},
		{1, 7},
		{4, 7},
		{8, 7},
	}

	tts := append([]networkParams{}, always...)
	tts = append(tts, sometimes[rand.Intn(len(sometimes))])

	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	for _, tt := range tts {
		t.Logf("Running test with %d parties and %d shards", tt.numOfParties, tt.numOfShards)

		t.Run(fmt.Sprintf("%d parties - %d shards", tt.numOfParties, tt.numOfShards), func(t *testing.T) {
			dir, err := os.MkdirTemp("", fmt.Sprintf("%s_%d_%d_", "TestSubmitAndReceive", tt.numOfParties, tt.numOfShards))
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			// 1.
			configPath := filepath.Join(dir, "config.yaml")
			netInfo := testutil.CreateNetwork(t, configPath, tt.numOfParties, tt.numOfShards, "none", "none")
			require.NoError(t, err)
			numOfArmaNodes := len(netInfo)
			// 2.
			armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

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
			totalTxNumber := 500
			fillInterval := 10 * time.Millisecond
			fillFrequency := 1000 / int(fillInterval.Milliseconds())
			rate := 500

			capacity := rate / fillFrequency
			rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
			require.NoError(t, err)

			broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
			defer broadcastClient.Stop()

			for i := 0; i < totalTxNumber; i++ {
				status := rl.GetToken()
				require.True(t, status)
				txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
				err = broadcastClient.SendTx(txContent)
				require.NoError(t, err)
			}

			// 5. Check If Transaction is sent to all parties
			t.Log("Finished submit")

			parties := []types.PartyID{}
			for partyID := 1; partyID <= tt.numOfParties; partyID++ {
				parties = append(parties, types.PartyID(partyID))
			}

			startBlock := uint64(0)
			endBlock := uint64(tt.numOfShards)

			PullFromAssemblers(t, &BlockPullerOptions{
				UserConfig: uc,
				Parties:    parties,
				StartBlock: startBlock,
				EndBlock:   endBlock,
				Blocks:     tt.numOfShards + 1,
				ErrString:  "cancelled pull from assembler: %d",
			})

			// Pull first two blocks and count them.
			startBlock = uint64(0)
			endBlock = uint64(1)

			PullFromAssemblers(t, &BlockPullerOptions{
				UserConfig: uc,
				Parties:    parties,
				StartBlock: startBlock,
				EndBlock:   endBlock,
				Blocks:     int((endBlock - startBlock) + 1),
				ErrString:  "cancelled pull from assembler: %d",
			})

			// Pull more block, then cancel.
			startBlock = uint64(1)
			endBlock = uint64(1000)

			PullFromAssemblers(t, &BlockPullerOptions{
				UserConfig: uc,
				Parties:    parties,
				StartBlock: startBlock,
				EndBlock:   endBlock,
				ErrString:  "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
			})
		})
	}
}

// TestSubmitReceiveAndGetStatus tests submitting transactions to the orderer,
// receiving blocks from the assembler, and getting the status from the assembler.
// It also tests edge cases such as pulling with endBlock < startBlock to ensure proper error handling.
func TestSubmitAndReceiveStatus(t *testing.T) {
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	numOfShards := 2
	numOfParties := 4

	// create temp dir

	dir, err := os.MkdirTemp("", fmt.Sprintf("%s_%d_%d_", "TestSubmitAndReceive", numOfParties, numOfShards))
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)
	// 2.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

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
	totalTxNumber := 500
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	rate := 500

	capacity := rate / fillFrequency
	rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	require.NoError(t, err)

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		require.True(t, status)
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
		require.NoError(t, err)
	}

	// 5. Check If Transaction is sent to all parties
	t.Log("Finished submit")

	parties := []types.PartyID{}
	for partyID := 1; partyID <= numOfParties; partyID++ {
		parties = append(parties, types.PartyID(partyID))
	}

	startBlock := uint64(0)
	endBlock := uint64(numOfShards)

	statusSuccess := common.Status_SUCCESS
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: startBlock,
		EndBlock:   endBlock,
		Status:     &statusSuccess,
	})

	statusUknown := common.Status_UNKNOWN
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: startBlock,
		Blocks:     numOfShards + 1,
		ErrString:  "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:     &statusUknown,
	})

	// Pull with endBlock < startBlock, then cancel.
	startBlock = uint64(3)
	endBlock = uint64(2)

	statusBadRequest := common.Status_BAD_REQUEST
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: startBlock,
		EndBlock:   endBlock,
		ErrString:  "pull from assembler: %d ended: received a non block message: status:BAD_REQUEST",
		Status:     &statusBadRequest,
	})
}
