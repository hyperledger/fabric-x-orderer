/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package basic

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging/httpadmin"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	test_utils "github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/metadata"
	"github.com/hyperledger/fabric-x-orderer/test/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
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
				env := tx.CreateStructuredEnvelope(txContent)
				err = broadcastClient.SendTx(env)
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

			signer := signutil.CreateTestSigner(t, "org1", dir)

			utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
				UserConfig: uc,
				Parties:    parties,
				StartBlock: startBlock,
				EndBlock:   endBlock,
				Blocks:     tt.numOfShards + 1,
				ErrString:  "cancelled pull from assembler: %d",
				Signer:     signer,
			})

			// Pull first two blocks and count them.
			startBlock = uint64(0)
			endBlock = uint64(1)

			utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
				UserConfig: uc,
				Parties:    parties,
				StartBlock: startBlock,
				EndBlock:   endBlock,
				Blocks:     int((endBlock - startBlock) + 1),
				ErrString:  "cancelled pull from assembler: %d",
				Signer:     signer,
			})

			// Pull more block, then cancel.
			startBlock = uint64(1)
			endBlock = uint64(1000)

			utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
				UserConfig: uc,
				Parties:    parties,
				StartBlock: startBlock,
				EndBlock:   endBlock,
				ErrString:  "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
				Signer:     signer,
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
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
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
	signer := signutil.CreateTestSigner(t, "org1", dir)

	statusSuccess := common.Status_SUCCESS
	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: startBlock,
		EndBlock:   endBlock,
		Status:     &statusSuccess,
		Signer:     signer,
	})

	statusUknown := common.Status_UNKNOWN
	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: startBlock,
		Blocks:     numOfShards + 1,
		ErrString:  "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:     &statusUknown,
		Signer:     signer,
	})

	// Pull with endBlock < startBlock, then cancel.
	startBlock = uint64(3)
	endBlock = uint64(2)

	statusBadRequest := common.Status_BAD_REQUEST
	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: startBlock,
		EndBlock:   endBlock,
		ErrString:  "pull from assembler: %d ended: received a non block message: status:BAD_REQUEST",
		Status:     &statusBadRequest,
		Signer:     signer,
	})
}

// TestRunNodesAndGetResponseFromOperationEndpoints verifies that the nodes respond correctly to operation endpoints INSECURED.
func TestRunNodesAndGetResponseFromOperationEndpoints(t *testing.T) {
	// 1. compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	t.Cleanup(func() {
		gexec.CleanupBuildArtifacts()
	})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// 2. create network with 1 parties and 1 shard
	parties := 1
	shards := 1

	t.Logf("Running test with %d parties and %d shards", parties, shards)

	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	// 3. create config.yaml and generate network artifacts
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, parties, shards, "none", "none")
	t.Cleanup(func() {
		netInfo.CleanUp()
	})
	require.NotNil(t, netInfo)
	numOfArmaNodes := len(netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// 4. run arma nodes
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	t.Cleanup(func() {
		armaNetwork.Stop()
	})

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	debugRe := regexp.MustCompile("DEBU")

	batcherToTest := armaNetwork.GetBatcher(t, types.PartyID(1), types.ShardID(1))

	// 5. Verify that the batcher is running and has no DEBUG logs in its output
	matches := debugRe.FindStringSubmatch(string(batcherToTest.RunInfo.Session.Err.Contents()))
	require.Len(t, matches, 0, "expected to not find DEBUG logs in batcher output")

	batcherLogSpecURL := testutil.CaptureArmaNodeLogSpecServiceURL(t, batcherToTest)
	logSpecRe := regexp.MustCompile(`^\{\s*"spec"\s*:\s*"([^"]*)"\s*\}`)

	require.Eventually(t, func() bool {
		logSpec := testutil.FetchLogSpecValue(t, logSpecRe, batcherLogSpecURL)
		if logSpec == nil {
			return false
		}
		if logSpec.Spec == "info" {
			return true
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	consenterToTest := armaNetwork.GetConsenter(t, 1)

	// 6. Verify that the consenter is running and has no DEBUG logs in its output
	matches = debugRe.FindStringSubmatch(string(consenterToTest.RunInfo.Session.Err.Contents()))
	require.Len(t, matches, 0, "expected to not find DEBUG logs in consenter output")

	consenterLogSpecURL := testutil.CaptureArmaNodeLogSpecServiceURL(t, consenterToTest)

	require.Eventually(t, func() bool {
		logSpec := testutil.FetchLogSpecValue(t, logSpecRe, consenterLogSpecURL)
		if logSpec == nil {
			return false
		}
		if logSpec.Spec == "info" {
			return true
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	// 7. Update log spec to DEBUG for both batcher and consenter, and verify the change
	testutil.UpdateLogSpecValue(t, batcherLogSpecURL, &httpadmin.LogSpec{Spec: "debug"})

	require.Eventually(t, func() bool {
		logSpec := testutil.FetchLogSpecValue(t, logSpecRe, batcherLogSpecURL)
		if logSpec == nil {
			return false
		}
		if logSpec.Spec == "debug" {
			return true
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	testutil.UpdateLogSpecValue(t, consenterLogSpecURL, &httpadmin.LogSpec{Spec: "debug"})

	require.Eventually(t, func() bool {
		logSpec := testutil.FetchLogSpecValue(t, logSpecRe, consenterLogSpecURL)
		if logSpec == nil {
			return false
		}
		if logSpec.Spec == "debug" {
			return true
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	// 8. Verify that the assembler is running and has no DEBUG logs in its output
	assemblerToTest := armaNetwork.GetAssembler(t, types.PartyID(1))
	assemblerLogSpecURL := testutil.CaptureArmaNodeLogSpecServiceURL(t, assemblerToTest)

	matches = debugRe.FindStringSubmatch(string(assemblerToTest.RunInfo.Session.Err.Contents()))
	require.Len(t, matches, 0, "expected to not find DEBUG logs in assembler output")

	require.Eventually(t, func() bool {
		logSpec := testutil.FetchLogSpecValue(t, logSpecRe, assemblerLogSpecURL)
		if logSpec == nil {
			return false
		}
		if logSpec.Spec == "info" {
			return true
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	testutil.UpdateLogSpecValue(t, assemblerLogSpecURL, &httpadmin.LogSpec{Spec: "debug"})

	// 9. Verify that the router is running and has no DEBUG logs in its output
	routerToTest := armaNetwork.GetRouter(t, 1)
	routerLogSpecURL := testutil.CaptureArmaNodeLogSpecServiceURL(t, routerToTest)

	matches = debugRe.FindStringSubmatch(string(routerToTest.RunInfo.Session.Err.Contents()))
	require.Len(t, matches, 0, "expected to not find DEBUG logs in router output")

	require.Eventually(t, func() bool {
		logSpec := testutil.FetchLogSpecValue(t, logSpecRe, routerLogSpecURL)
		if logSpec == nil {
			return false
		}
		if logSpec.Spec == "info" {
			return true
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	// 9. Update the router's log specification to "debug" and assert the change is reflected.
	testutil.UpdateLogSpecValue(t, routerLogSpecURL, &httpadmin.LogSpec{Spec: "debug"})

	require.Eventually(t, func() bool {
		logSpec := testutil.FetchLogSpecValue(t, logSpecRe, routerLogSpecURL)
		if logSpec == nil {
			return false
		}
		if logSpec.Spec == "debug" {
			return true
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	// 10. Send transactions to the routers.
	totalTxNumber := 10
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	rate := 500

	capacity := rate / fillFrequency
	rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	require.NoError(t, err, "failed to start a rate limiter")

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)

	for i := range totalTxNumber {
		status := rl.GetToken()
		require.Truef(t, status, "failed to send tx %d", i+1)
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateStructuredEnvelope(txContent)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	t.Log("Finished submit")
	broadcastClient.Stop()

	// 11. Verify that the batcher, consenter, assembler and router have DEBUG logs in their output after sending transactions
	require.Eventually(t, func() bool {
		matches := debugRe.FindStringSubmatch(string(batcherToTest.RunInfo.Session.Err.Contents()))
		return len(matches) > 0
	}, 30*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		matches := debugRe.FindStringSubmatch(string(consenterToTest.RunInfo.Session.Err.Contents()))
		return len(matches) > 0
	}, 30*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		matches := debugRe.FindStringSubmatch(string(assemblerToTest.RunInfo.Session.Err.Contents()))
		return len(matches) > 0
	}, 30*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		matches := debugRe.FindStringSubmatch(string(routerToTest.RunInfo.Session.Err.Contents()))
		return len(matches) > 0
	}, 30*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		matches := debugRe.FindStringSubmatch(string(routerToTest.RunInfo.Session.Err.Contents()))
		return len(matches) > 0
	}, 30*time.Second, 100*time.Millisecond)

	// 12. Verify that the batcher and consenter Prometheus metrics reflect the transactions sent
	batcherPrometheusURL := testutil.CaptureArmaNodePrometheusServiceURL(t, batcherToTest)
	batcherPrometheusRe := regexp.MustCompile(fmt.Sprintf(`batcher_router_txs_total\{party_id="%d",shard_id="%d"\} \d+`, types.PartyID(1), types.ShardID(1)))

	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, batcherPrometheusRe, batcherPrometheusURL) == totalTxNumber
	}, 30*time.Second, 100*time.Millisecond)

	consenterPrometheusURL := testutil.CaptureArmaNodePrometheusServiceURL(t, consenterToTest)
	consenterPrometheusRe := regexp.MustCompile(fmt.Sprintf(`consensus_bafs_count\{party_id="%d"\} \d+`, types.PartyID(1)))

	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, consenterPrometheusRe, consenterPrometheusURL) >= parties*shards
	}, 30*time.Second, 100*time.Millisecond)

	// 13. Verify that the batcher and consenter health check endpoints respond with status "OK"
	batcherHealthCheckURL := testutil.CaptureArmaNodeHealthCheckServiceURL(t, batcherToTest)
	healthCheckRe := regexp.MustCompile(`^\{\s*"status"\s*:\s*"([^"]+)"(?:\s*,\s*"time"\s*:\s*"[^"]*")?\s*\}$`)

	require.Eventually(t, func() bool {
		return testutil.GetHealthCheckStatus(t, healthCheckRe, batcherHealthCheckURL)
	}, 30*time.Second, 100*time.Millisecond)

	consenterHealthCheckURL := testutil.CaptureArmaNodeHealthCheckServiceURL(t, consenterToTest)

	require.Eventually(t, func() bool {
		return testutil.GetHealthCheckStatus(t, healthCheckRe, consenterHealthCheckURL)
	}, 30*time.Second, 100*time.Millisecond)

	// 14. Verify that the batcher and consenter version info endpoints respond with the correct commit SHA and version
	consenterVersionInfoUrl := testutil.CaptureArmaNodeVersionInfoServiceURL(t, consenterToTest)

	versionInfoRe := regexp.MustCompile(`^\{\s*"CommitSHA"\s*:\s*"([^"]*)"\s*,\s*"Version"\s*:\s*"([^"]*)"\s*\}$`)

	require.Eventually(t, func() bool {
		val := testutil.FetchVersionInfoValue(t, versionInfoRe, consenterVersionInfoUrl)
		if val == nil {
			return false
		}
		t.Logf("Fetched version info: CommitSHA=%s, Version=%s", val.CommitSHA, val.Version)
		return val.CommitSHA == metadata.CommitSHA && val.Version == metadata.Version
	}, 30*time.Second, 100*time.Millisecond)

	// 15. Verify that the batcher version info endpoint responds with the correct commit SHA and version
	batcherVersionInfoUrl := testutil.CaptureArmaNodeVersionInfoServiceURL(t, batcherToTest)

	require.Eventually(t, func() bool {
		val := testutil.FetchVersionInfoValue(t, versionInfoRe, batcherVersionInfoUrl)
		if val == nil {
			return false
		}
		t.Logf("Fetched version info: CommitSHA=%s, Version=%s", val.CommitSHA, val.Version)
		return val.CommitSHA == metadata.CommitSHA && val.Version == metadata.Version
	}, 30*time.Second, 100*time.Millisecond)
}

func TestSecuredTLSOperationsService(t *testing.T) {
	// 1. compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	t.Cleanup(func() {
		gexec.CleanupBuildArtifacts()
	})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// 2. create network with 2 parties and 1 shard
	parties := 1
	shards := 1

	t.Logf("Running test with %d parties and %d shards", parties, shards)

	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, parties, shards, "none", "none")
	t.Cleanup(func() {
		netInfo.CleanUp()
	})
	require.NotNil(t, netInfo)
	numOfArmaNodes := len(netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	tlsCAFilePath := path.Join(dir, "crypto/ordererOrganizations/org1/tlsca/tlsorg1-CA-cert.pem")
	routerConfigPath := path.Join(dir, "config/party1/local_config_router.yaml")
	routerLocalConfig := testutil.ReadNodeConfigFromYaml(t, routerConfigPath)
	routerLocalConfig.OperationsConfig.TLSConfig.Enabled = true
	routerLocalConfig.OperationsConfig.TLSConfig.ClientAuthRequired = true
	routerLocalConfig.OperationsConfig.TLSConfig.ClientRootCAs = []string{tlsCAFilePath}
	err = test_utils.WriteToYAML(&routerLocalConfig, routerConfigPath)
	require.NoError(t, err)

	batcherConfigPath := path.Join(dir, "config/party1/local_config_batcher1.yaml")
	batcherLocalConfig := testutil.ReadNodeConfigFromYaml(t, batcherConfigPath)
	batcherLocalConfig.OperationsConfig.TLSConfig.Enabled = true
	batcherLocalConfig.OperationsConfig.TLSConfig.ClientAuthRequired = true
	batcherLocalConfig.OperationsConfig.TLSConfig.ClientRootCAs = []string{tlsCAFilePath}
	err = test_utils.WriteToYAML(&batcherLocalConfig, batcherConfigPath)
	require.NoError(t, err)

	consenterConfigPath := path.Join(dir, "config/party1/local_config_consenter.yaml")
	consenterLocalConfig := testutil.ReadNodeConfigFromYaml(t, consenterConfigPath)
	consenterLocalConfig.OperationsConfig.TLSConfig.Enabled = true
	consenterLocalConfig.OperationsConfig.TLSConfig.ClientAuthRequired = true
	consenterLocalConfig.OperationsConfig.TLSConfig.ClientRootCAs = []string{tlsCAFilePath}
	err = test_utils.WriteToYAML(&consenterLocalConfig, consenterConfigPath)
	require.NoError(t, err)

	assemblerConfigPath := path.Join(dir, "config/party1/local_config_assembler.yaml")
	assemblerLocalConfig := testutil.ReadNodeConfigFromYaml(t, assemblerConfigPath)
	assemblerLocalConfig.OperationsConfig.TLSConfig.Enabled = true
	assemblerLocalConfig.OperationsConfig.TLSConfig.ClientAuthRequired = true
	assemblerLocalConfig.OperationsConfig.TLSConfig.ClientRootCAs = []string{tlsCAFilePath}
	err = test_utils.WriteToYAML(&assemblerLocalConfig, assemblerConfigPath)
	require.NoError(t, err)

	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	t.Cleanup(func() {
		armaNetwork.Stop()
	})

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	routerToMonitor := armaNetwork.GetRouter(t, 1)
	routerPrometheusURL := testutil.CaptureArmaNodePrometheusServiceURL(t, routerToMonitor)

	batcherToMonitor := armaNetwork.GetBatcher(t, 1, 1)
	batcherPrometheusURL := testutil.CaptureArmaNodePrometheusServiceURL(t, batcherToMonitor)

	consenterToMonitor := armaNetwork.GetConsenter(t, 1)
	consenterPrometheusURL := testutil.CaptureArmaNodePrometheusServiceURL(t, consenterToMonitor)

	assemblerToMonitor := armaNetwork.GetAssembler(t, 1)
	assemblerPrometheusURL := testutil.CaptureArmaNodePrometheusServiceURL(t, assemblerToMonitor)

	tlsOpts := []func(config *tls.Config){
		func(config *tls.Config) {
			cert, err := tls.LoadX509KeyPair(
				path.Join(dir, "crypto/ordererOrganizations/org1/users/client@org1/tls/client.crt"),
				path.Join(dir, "crypto/ordererOrganizations/org1/users/client@org1/tls/client.key"),
			)
			require.NoError(t, err)
			config.Certificates = []tls.Certificate{cert}
		},
		func(config *tls.Config) {
			pemBytes, err := os.ReadFile(tlsCAFilePath)
			require.NoError(t, err)

			clientCAPool := x509.NewCertPool()
			clientCAPool.AppendCertsFromPEM(pemBytes)
			config.RootCAs = clientCAPool
		},
	}

	re := regexp.MustCompile(fmt.Sprintf(`router_requests_completed\{party_id="%d"\} \d+`, types.PartyID(1)))
	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, routerPrometheusURL, tlsOpts...) == 0
	}, 30*time.Second, 100*time.Millisecond)

	re = regexp.MustCompile(fmt.Sprintf(`batcher_router_txs_total\{party_id="%d",shard_id="%d"\} \d+`, types.PartyID(1), types.ShardID(1)))
	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, batcherPrometheusURL, tlsOpts...) == 0
	}, 30*time.Second, 100*time.Millisecond)

	re = regexp.MustCompile(fmt.Sprintf(`consensus_bafs_count\{party_id="%d"\} \d+`, types.PartyID(1)))
	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, consenterPrometheusURL, tlsOpts...) == 0
	}, 30*time.Second, 100*time.Millisecond)

	re = regexp.MustCompile(fmt.Sprintf(`assembler_ledger_transaction_count_total\{party_id="%d"\} \d+`, types.PartyID(1)))
	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, assemblerPrometheusURL, tlsOpts...) == 1
	}, 30*time.Second, 100*time.Millisecond)
}
