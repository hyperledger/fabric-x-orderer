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
	config "github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
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
//  4. Loads user configuration and collects the public keys of all consenters.
//  5. Constructs a verifier using the collected consenter information.
//  6. Submits a specified number of transactions to the network at a controlled rate.
//  7. Pulls blocks from all parties and verifies that the transactions have been correctly
//     propagated and signed.
//
// The test ensures that all parties receive the submitted transactions and that the signatures
// on the assembler blocks are valid.
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
	localConfig, _, err := config.LoadLocalConfig(filepath.Join(dir, "config/party1/local_config_consenter.yaml"))
	assert.NoError(t, err)
	sharedConfig, _, err := config.LoadSharedConfig(filepath.Join(dir, "bootstrap", "shared_config.yaml"))
	assert.NoError(t, err)
	conf := &config.Configuration{
		LocalConfig:  localConfig,
		SharedConfig: sharedConfig,
	}
	consenterInfos := conf.ExtractConsenters()

	// 5.
	verifier := BuildVerifier(consenterInfos, logger)

	// 6.
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

	// 7.
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
		Verifier:   &verifier,
	})
}
