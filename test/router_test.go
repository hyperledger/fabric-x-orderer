/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"crypto/ecdsa"
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
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRouterRestartRecover tests the ability of the router to restart and recover from a shutdown.
// The test sends transactions to the router and then stops it. After that, it waits for the router to
// be restarted and verifies that all transactions were successfully delivered.
//
// The test consists of the following steps:
// 1. Compile the arma binary.
// 2. Create a temporary directory for the test.
// 3. Create a config YAML file in the temporary directory.
// 4. Generate the config files in the temporary directory using the armageddon generate command.
// 5. Run the arma nodes.
// 6. Send transactions to the router.
// 7. Stop a secondary router.
// 8. Pull blocks from all assemblers.
// 9. Start the secondary router.
// 10. Pull blocks from all assemblers again.
// 11. Stop a primary router.
// 12. Pull blocks from all assemblers again.
// 13. Start the primary router.
// 14. Stop the broadcast client.
// 15. Pull blocks from all assemblers again.
func TestRouterRestartRecover(t *testing.T) {
	// 1. compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Number of parties in the test
	numOfParties := 4

	t.Logf("Running test with %d parties and %d shards", numOfParties, 1)

	// 2. Create a temporary directory for the test.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 3. Create a config YAML file in the temporary directory.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 1, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	// 4. Generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// 5. Run the arma nodes.
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	// 6. Send transactions to the routers.
	totalTxNumber := 1000
	// rate limiter parameters
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

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(totalTxSent+i, 64, []byte("sessionNumber"))
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	totalTxSent += totalTxNumber

	parties := []types.PartyID{}
	for partyID := 1; partyID <= numOfParties; partyID++ {
		parties = append(parties, types.PartyID(partyID))
	}

	pullerInfos := PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:       uc,
		Parties:          parties,
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxSent,
		Timeout:          60,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})

	primaryPartyId := pullerInfos[types.PartyID(1)].Primary[types.ShardID(1)]
	secondaryPartyId := primaryPartyId%types.PartyID(numOfParties) + 1

	// 7. Stop a secondary router.
	routerToStop := armaNetwork.GetRouter(t, secondaryPartyId)

	t.Logf("Stopping router: party %d", routerToStop.PartyId)
	routerToStop.StopArmaNode()
	gotError := false

	for i := range totalTxNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(totalTxSent+i, 64, []byte("sessionNumber"))
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		if err != nil {
			require.ErrorContains(t, err, "EOF")
			gotError = true
		}
	}

	require.True(t, gotError, "expected to get an error when sending tx to a stopped router")

	totalTxSent += totalTxNumber

	// 8. Pull blocks from all assemblers.
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

	// 9. Restart the secondary router.
	routerToStop.RestartArmaNode(t, readyChan)
	testutil.WaitReady(t, readyChan, 1, 10)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(totalTxSent+i, 64, []byte("sessionNumber"))
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	totalTxSent += totalTxNumber

	// 10. Pull blocks from all assemblers.
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

	// 11. Stop a primary router.
	primaryRouterToStop := armaNetwork.GetRouter(t, primaryPartyId)
	t.Logf("Stopping router: party %d", primaryRouterToStop.PartyId)
	primaryRouterToStop.StopArmaNode()

	gotError = false

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
			require.ErrorContains(t, err, "EOF")
			gotError = true
		}
	}

	require.True(t, gotError, "expected to get an error when sending tx to a stopped router")

	totalTxSent += totalTxNumber

	// 12. Pull blocks from all assemblers.
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

	// 13. Restart the primary router.
	primaryRouterToStop.RestartArmaNode(t, readyChan)
	testutil.WaitReady(t, readyChan, 1, 10)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(totalTxSent+i, 64, []byte("sessionNumber"))
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	totalTxSent += totalTxNumber

	// 14. Stop the broadcast client.
	broadcastClient.Stop()

	// 15. Pull blocks from all assemblers.
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

// TestSubmitToRouterGetMetrics tests the end-to-end flow of submitting transactions to the router
// and verifying that the metrics endpoint correctly reflects the number of incoming transactions.
// The test performs the following steps:
//  1. Compiles the arma binary.
//  2. Creates a temporary directory for test artifacts.
//  3. Generates a network configuration YAML file.
//  4. Uses the armageddon CLI to generate config files for the network.
//  5. Starts the arma nodes and waits for them to be ready.
//  6. Sends a specified number of transactions to the router using a rate limiter.
//  7. Queries the router's metrics endpoint and asserts that the number of incoming transactions
//     matches the number sent, within a timeout period.
func TestSubmitToRouterGetMetrics(t *testing.T) {
	// 1. compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Number of parties in the test
	numOfParties := 1

	t.Logf("Running test with %d parties and %d shards", numOfParties, 1)

	// 2. Create a temporary directory for the test.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 3. Create a config YAML file in the temporary directory.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 1, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	// 4. Generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// 5. Run the arma nodes.
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	require.NoError(t, err)
	require.NotNil(t, uc)

	// 6. Send transactions to the routers.
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
	defer broadcastClient.Stop()

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
	// 7. Query the router's metrics endpoint and assert the incoming transaction count.
	routerToMonitor := armaNetwork.GetRouter(t, 1)
	url := testutil.CaptureArmaNodePrometheusServiceURL(t, routerToMonitor)

	pattern := fmt.Sprintf(`router_requests_completed\{party_id="%d"\} \d+`, types.PartyID(1))
	re := regexp.MustCompile(pattern)

	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, url) == totalTxNumber
	}, 30*time.Second, 100*time.Millisecond)
}

// TestVerifySignedTxsByRouterSingleParty verifies that a router running in a single-party,
// single-shard configuration correctly enforces client signature verification when accepting
// and broadcasting transactions.
func TestVerifySignedTxsByRouterSingleParty(t *testing.T) {
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

	// 3. Create a config YAML file in the temporary directory.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 1, 1, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	// 4. Generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	configFilePath := filepath.Join(dir, fmt.Sprintf("config/party%d/local_config_router.yaml", types.PartyID(1)))
	conf, _, err := config.LoadLocalConfig(configFilePath)
	require.NoError(t, err)

	// Modify the router configuration to require client signature verification.
	conf.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = true
	utils.WriteToYAML(conf.NodeLocalConfig, configFilePath)

	conf, _, err = config.LoadLocalConfig(configFilePath)
	require.NoError(t, err)
	require.True(t, conf.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired)

	// 5. Run the arma nodes.
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	// Obtains a test user configuration and constructs a broadcast client.
	readyChan := make(chan struct{}, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, types.PartyID(1))
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	totalTxNumber := 1000
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

	routerToMonitor := armaNetwork.GetRouter(t, 1)
	url := testutil.CaptureArmaNodePrometheusServiceURL(t, routerToMonitor)

	pattern := fmt.Sprintf(`router_requests_completed\{party_id="%d"\} \d+`, types.PartyID(1))
	re := regexp.MustCompile(pattern)

	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, url) == totalTxNumber
	}, 30*time.Second, 100*time.Millisecond)

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

	// Attempt to send a transaction with an invalid signature and expect rejection.
	txContent := tx.PrepareTxWithTimestamp(totalTxNumber+1, 64, []byte("sessionNumber"))
	fakeCA, err := tlsgen.NewCA()
	require.NoError(t, err)
	fakeSigner := fakeCA.Signer().(*ecdsa.PrivateKey)
	env := tx.CreateSignedStructuredEnvelope(txContent, (*crypto.ECDSASigner)(fakeSigner), fakeCA.CertBytes(), org)
	err = broadcastClient.SendTx(env)
	require.ErrorContains(t, err, "signature did not satisfy policy /Channel/Writers\n")
}

// TestMTLSFromClientNotSpecifiedInLocalConfig tests that a router and assebmler configured to use mTLS rejects transactions
// and delivery requests from a client whose certificate is not specified in the router's local configuration.
func TestMTLSFromClientNotSpecifiedInLocalConfig(t *testing.T) {
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Number of parties in the test
	numOfParties := 1
	numOfShards := 1

	t.Logf("Running test with %d parties and %d shards", numOfParties, numOfShards)

	// create a temporary directory for the test.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// create a config YAML file in the temporary directory.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	// generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// run the arma nodes.
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	// 6. Send transactions to the routers.
	totalTxNumber := 10
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
	defer broadcastClient.Stop()

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

	parties := []types.PartyID{1}
	for partyID := range numOfParties {
		parties = append(parties, types.PartyID(partyID+1))
	}

	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:       uc,
		Parties:          parties,
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxNumber,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})

	// Attempt to send a transaction from a client whose certificate is not specified in the router's local config.
	// Expect the transaction to be rejected due to mTLS verification failure.
	fakeCA, err := tlsgen.NewCA()
	require.NoError(t, err)
	fakeClientCertKeyPair, err := fakeCA.NewClientCertKeyPair()
	require.NoError(t, err)

	uc.TLSPrivateKey = fakeClientCertKeyPair.Key
	uc.TLSCertificate = fakeClientCertKeyPair.Cert

	broadcastClientInvalid := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClientInvalid.Stop()

	txContent := tx.PrepareTxWithTimestamp(0, 64, []byte("sessionNumber"))
	env := tx.CreateStructuredEnvelope(txContent)
	err = broadcastClientInvalid.SendTx(env)
	require.ErrorContains(t, err, "failed to create a gRPC client connection to routers")

	// Attempt to pull blocks from assemblers and expect failure due to mTLS verification.
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		ErrString:  "failed to create a gRPC client connection to assembler: %d",
	})
}
