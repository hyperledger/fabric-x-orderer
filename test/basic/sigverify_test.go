/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package basic

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	test_utils "github.com/hyperledger/fabric-x-orderer/test/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
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
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)
	numOfArmaNodes := len(netInfo)

	// 2.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// 3.
	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	logger := testutil.CreateLogger(t, 0)

	// 4.
	verifier := test_utils.BuildVerifier(dir, types.PartyID(1), logger)

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
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: startBlock,
		EndBlock:   endBlock,
		Status:     &statusSuccess,
		Verifier:   verifier,
		Signer:     signutil.CreateTestSigner(t, "org1", dir),
	})
}

// TestSubmitTXWithVerification runs a network of Arma with clientSignatureVerificationRequired being true, i.e. the client signature on data transaction is required.
// A data TX with a fake signature did not satisfy the policy /Channel/Writer and is rejected as a result, while a well signed data TX is submitted and eventually appear in a block.
func TestSubmitTXWithVerification(t *testing.T) {
	t.Log("Compile Arma")
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	t.Log("Generate the configuration with clientSignatureVerificationRequired = True")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 2
	parties := []types.PartyID{}
	for partyID := 1; partyID <= numOfParties; partyID++ {
		parties = append(parties, types.PartyID(partyID))
	}
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)
	numOfArmaNodes := len(netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	t.Log("Run Arma nodes")
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()
	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	t.Log("Create a signer")
	submittingParty := 1
	uc, err := testutil.GetUserConfig(dir, types.PartyID(submittingParty))
	require.NoError(t, err)
	require.NotNil(t, uc)
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	require.NotNil(t, signer)
	require.NotNil(t, certBytes)

	t.Log("Create a broadcast client")
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	t.Log("Send a data tx that is not well signed, i.e. signature did not satisfy the policy /Channel/Writers")
	txContent := tx.PrepareTxWithTimestamp(2, 64, []byte("dataTX"))
	env := tx.CreateStructuredEnvelope(txContent)
	err = broadcastClient.SendTx(env)
	require.Error(t, err)
	require.ErrorContains(t, err, "INTERNAL_SERVER_ERROR, Info: request structure verification error: signature did not satisfy policy /Channel/Writers")

	t.Log("Send one more well signed data tx")
	env = tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, fmt.Sprintf("org%d", submittingParty))
	err = broadcastClient.SendTx(env)
	require.NoError(t, err)

	t.Log("Pull from assembler")
	pullRequestSigner := signutil.CreateTestSigner(t, "org1", dir)
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: 0,
		EndBlock:   uint64(2),
		Blocks:     2, // genesis + block with one tx
		ErrString:  "cancelled pull from assembler: %d",
		Signer:     pullRequestSigner,
	})
}
