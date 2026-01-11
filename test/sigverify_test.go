/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSubmitReceiveAndVerifySignaturesAssemblerBlocks is an integration test that verifies the end-to-end
// process of submitting transactions, receiving them, and verifying the signatures on assembler blocks
//
// The test performs the following steps:
//  1. Builds the ARMA binary and sets up a temporary directory for test artifacts.
//  2. Generates a network configuration for a specified number of parties and shards.
//  3. Starts ARMA nodes and waits for them to become ready.
//  4. Constructs a verifier using the collected consenter information.
//  5. Submits a specified number of transactions to the network at a controlled rate.
//  6. Pulls blocks from all parties and verifies that the transactions have been correctly
//     propagated and signed.
func TestSubmitReceiveAndVerifySignaturesAssemblerBlocks(t *testing.T) {
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	numOfShards := 2
	numOfParties := 4

	// create temp dir
	dir, err := os.MkdirTemp("", fmt.Sprintf("%s_%d_%d_", "TestSubmitReceiveAndVerifySignaturesAssemblerBlocks", numOfParties, numOfShards))
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

	logger := testutil.CreateLogger(t, 0)

	// 4.
	verifier := BuildVerifier(dir, types.PartyID(1), logger)

	// 5.
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
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	// 6.
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
		Verifier:   verifier,
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
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	// 3. Generates necessary cryptographic materials and network artifacts using the arma CLI
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// 4. Starts arma nodes and waits for them to be ready
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	logger := testutil.CreateLogger(t, 0)

	// 5. Builds a signature verifier from the consenter information
	verifier := BuildVerifier(dir, types.PartyID(1), logger)

	// 6. Creates a broadcast client to submit transactions and submits a configuration transaction based on the genesis block
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	// Create config tx
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	submittingPartyID := 1
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, genesisBlockPath)
	defer cleanUp()

	configUpdatePbData := configUpdateBuilder.UpdateBatchSizeConfig(t, cfgutil.NewBatchSizeConfig(cfgutil.BatchSizeConfigName.MaxMessageCount, 500))
	require.NotEmpty(t, configUpdatePbData)

	env := cfgutil.CreateConfigTX(t, dir, numOfParties, genesisBlockPath, submittingPartyID, configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTx(env)
	require.NoError(t, err)
	totalBlocks := 2 // genesis + config block

	// 7. Pulls and verifies blocks from assemblers, ensuring the configuration block's signatures are valid
	statusSuccess := common.Status_UNKNOWN
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig: uc,
		Parties:    []types.PartyID{types.PartyID(submittingPartyID)},
		StartBlock: uint64(0),
		Status:     &statusSuccess,
		Verifier:   verifier,
		Blocks:     totalBlocks,
		ErrString:  "cancelled pull from assembler: %d",
		LogString:  "configuration block 1 partyID %d verified with",
	})
}
