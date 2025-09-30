/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
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
// 4. Submit 1000 txs to all routers at a specified rate
// 5. Stop one of the assemblers node
// 6. Submit another 1000 txs
// 7. Restart the assembler node
// 8. In parallel, pull blocks from the assembler and report results
func TestSubmitStopThenRestartAssembler(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	nodesNumber := len(netInfo)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, nodesNumber)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)

	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, nodesNumber, 10)

	// 4.
	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
	rate := "500"
	transactions := 1000
	txSize := "64"

	var waitForTxSent sync.WaitGroup
	waitForTxSent.Add(1)
	go func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", strconv.Itoa(transactions), "--rate", rate, "--txSize", txSize})
		waitForTxSent.Done()
	}()

	waitForTxSent.Wait()

	// 5.
	partyToRestart := types.PartyID(3)
	nodeToRestart := armaNetwork.GetAssembler(t, partyToRestart)
	nodeToRestart.StopArmaNode()

	// 6.
	waitForTxSent.Add(1)
	go func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", strconv.Itoa(transactions), "--rate", rate, "--txSize", txSize})
		waitForTxSent.Done()
	}()

	waitForTxSent.Wait()

	// 7 + 8.
	nodeToRestart.RestartArmaNode(t, readyChan)

	testutil.WaitReady(t, readyChan, 1, 10)

	totalTxs := uint64(0)
	totalBlocks := uint64(0)
	expectedNumOfTxs := uint64(transactions*2 + 1)

	userConfig, err := testutil.GetUserConfig(dir, partyToRestart)
	assert.NoError(t, err)
	assert.NotNil(t, userConfig)

	dc := client.NewDeliverClient(userConfig)
	toCtx, toCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer toCancel()

	handler := func(block *common.Block) error {
		if block == nil {
			return errors.New("nil block")
		}
		if block.Header == nil {
			return errors.New("nil block header")
		}

		atomic.AddUint64(&totalTxs, uint64(len(block.GetData().GetData())))
		atomic.AddUint64(&totalBlocks, uint64(1))

		if atomic.CompareAndSwapUint64(&totalTxs, expectedNumOfTxs, uint64(transactions*2)) {
			toCancel()
		}

		return nil
	}

	_, err = dc.PullBlocks(toCtx, partyToRestart, 0, math.MaxUint64, handler)
	require.ErrorContains(t, err, "cancelled pull from assembler: 3")
	require.GreaterOrEqual(t, totalTxs, uint64(transactions*2))

	t.Logf("Finished pull and count: %d, %d", totalBlocks, totalTxs)
}

// TestStartAssemblerGetMetrics verifies that the assembler node correctly exposes
// transaction and block count metrics via Prometheus.
//
// The test performs the following steps:
// 1. Creates a test network configuration with 1 node
// 2. Generates network artifacts using armageddon CLI
// 3. Builds and starts the arma node binary
// 4. Sends a configurable number of transactions (10) using a rate-limited broadcast client
// 5. Monitors Prometheus metrics to verify transaction count (totalTxNumber+1) and block count (2)
// 6. Stops and restarts the monitored assembler node
// 7. Verifies that the metrics remain accurate after the node restart
func TestStartAssemblerGetMetrics(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 1, 1, "TLS", "TLS")

	// 2.
	armageddonCLI := armageddon.NewCLI()
	armageddonCLI.Run([]string{"generate", "--config", configPath, "--output", dir})

	// 3.
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	nodesNumber := len(netInfo)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, nodesNumber)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)

	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, nodesNumber, 10)

	// 4.
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

	uc, err := testutil.GetUserConfig(dir, 1)
	require.NoError(t, err)
	require.NotNil(t, uc)

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

	// 5.
	assemblerToMonitor := armaNetwork.GetAssembler(t, types.PartyID(1))
	url := testutil.CaptureArmaNodePrometheusServiceURL(t, assemblerToMonitor)

	txsCountPattern := fmt.Sprintf(`assembler_ledger_transaction_count_total\{party_id="%d"\} \d+`, types.PartyID(1))
	txsCountRe := regexp.MustCompile(txsCountPattern)

	blocksCountPattern := fmt.Sprintf(`assembler_ledger_blocks_count_total\{party_id="%d"\} \d+`, types.PartyID(1))
	blocksCountRe := regexp.MustCompile(blocksCountPattern)

	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, txsCountRe, url) == totalTxNumber+1
	}, 30*time.Second, 100*time.Millisecond)

	require.GreaterOrEqual(t, testutil.FetchPrometheusMetricValue(t, blocksCountRe, url), 2)

	// 6.
	assemblerToMonitor.StopArmaNode()
	assemblerToMonitor.RestartArmaNode(t, readyChan)

	// 7.
	testutil.WaitReady(t, readyChan, 1, 10)
	url = testutil.CaptureArmaNodePrometheusServiceURL(t, assemblerToMonitor)

	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, txsCountRe, url) == totalTxNumber+1
	}, 30*time.Second, 100*time.Millisecond)

	require.GreaterOrEqual(t, testutil.FetchPrometheusMetricValue(t, blocksCountRe, url), 2)
}
