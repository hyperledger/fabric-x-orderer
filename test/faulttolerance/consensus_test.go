/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package faulttolerance

import (
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/metadata"
	"github.com/hyperledger/fabric-x-orderer/test/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
)

// Scenario:
// 1. Run 4 parties
// 2. Submit 1000 txs to all
// 3. Pull from all
// 4. Stop one of the consenter nodes
// 5. Submit another 500 txs
// 6. Pull from all correct parties
// 7. Restart the consenter node
// 8. Submit 500 more txs
// 9. Pull from all
func TestSubmitStopThenRestartConsenter(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 2
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "TLS", "TLS")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)
	armageddon := armageddon.NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir})

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	readyChan := make(chan string, len(netInfo))
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)

	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, len(netInfo), 10)

	time.Sleep(10 * time.Second)

	uc, err := testutil.GetUserConfig(dir, 1)
	require.NoError(t, err)
	require.NotNil(t, uc)

	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
	rate := "500"
	txSize := "64"

	var waitForTxSent sync.WaitGroup
	waitForTxSent.Go(func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", strconv.Itoa(1000), "--rate", rate, "--txSize", txSize})
	})

	waitForTxSent.Wait()

	parties := make([]types.PartyID, numOfParties)
	for i := range numOfParties {
		parties[i] = types.PartyID(i + 1)
	}

	signer := signutil.CreateTestSigner(t, "org1", dir)

	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		StartBlock:   0,
		EndBlock:     math.MaxUint64,
		Transactions: 1000,
		ErrString:    "cancelled pull from assembler: %d",
		Timeout:      120,
		Signer:       signer,
	})

	partyToRestart := types.PartyID(3)
	consenterToRestart := armaNetwork.GetConsenter(t, partyToRestart)
	consenterToRestart.StopArmaNode()

	waitForTxSent.Go(func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", strconv.Itoa(500), "--rate", rate, "--txSize", txSize})
	})

	waitForTxSent.Wait()

	correctParties := make([]types.PartyID, 0)
	for i := range numOfParties {
		if types.PartyID(i+1) != partyToRestart {
			correctParties = append(correctParties, types.PartyID(i+1))
		}
	}
	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      correctParties,
		StartBlock:   0,
		EndBlock:     math.MaxUint64,
		Transactions: 1500,
		ErrString:    "cancelled pull from assembler: %d",
		Timeout:      120,
		Signer:       signer,
	})

	consenterToRestart.RestartArmaNode(t, readyChan)
	testutil.WaitReady(t, readyChan, 1, 10)

	waitForTxSent.Go(func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", strconv.Itoa(500), "--rate", rate, "--txSize", txSize})
	})

	waitForTxSent.Wait()

	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		StartBlock:   0,
		EndBlock:     math.MaxUint64,
		Transactions: 2000,
		ErrString:    "cancelled pull from assembler: %d",
		Timeout:      120,
		Signer:       signer,
	})
}

// TestRunArmaNetworkAndGetConsenterVersionInfo tests that the consenter's version info endpoint is up and returns the correct version information after the consenter is started.
func TestRunArmaNetworkAndGetConsenterVersionInfo(t *testing.T) {
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
	defer netInfo.CleanUp()
	numOfArmaNodes := len(netInfo)

	// 4. Generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// 5. Run the arma nodes.
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	// 6. Query the consenter's version info endpoint and assert the version information.
	consenter := armaNetwork.GetConsenter(t, types.PartyID(1))
	url := testutil.CaptureArmaNodeVersionInfoServiceURL(t, consenter)

	re := regexp.MustCompile(`^\{\s*"CommitSHA"\s*:\s*"([^"]*)"\s*,\s*"Version"\s*:\s*"([^"]*)"\s*\}$`)

	require.Eventually(t, func() bool {
		val := testutil.FetchVersionInfoValue(t, re, url)
		if val == nil {
			return false
		}
		return val.CommitSHA == metadata.CommitSHA && val.Version == metadata.Version
	}, 30*time.Second, 100*time.Millisecond)
}
