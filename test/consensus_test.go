/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package test

import (
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/testutil"
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

	armageddon := armageddon.NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2"})

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	readyChan := make(chan struct{}, len(netInfo))
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)

	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, len(netInfo), 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	require.NoError(t, err)
	require.NotNil(t, uc)

	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
	rate := "500"
	txSize := "64"

	var waitForTxSent sync.WaitGroup
	waitForTxSent.Add(1)
	go func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", strconv.Itoa(1000), "--rate", rate, "--txSize", txSize})
		waitForTxSent.Done()
	}()

	waitForTxSent.Wait()

	parties := make([]types.PartyID, numOfParties)
	for i := 0; i < numOfParties; i++ {
		parties[i] = types.PartyID(i + 1)
	}
	errString := "cancelled pull from assembler: %d"
	PullFromAssemblers(t, uc, parties, 0, math.MaxUint64, 1000, 0, errString)

	partyToRestart := types.PartyID(3)
	consenterToRestart := armaNetwork.GetConsenter(t, partyToRestart)
	consenterToRestart.StopArmaNode()

	waitForTxSent.Add(1)
	go func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", strconv.Itoa(500), "--rate", rate, "--txSize", txSize})
		waitForTxSent.Done()
	}()

	waitForTxSent.Wait()

	correctParties := make([]types.PartyID, 0)
	for i := 0; i < numOfParties; i++ {
		if types.PartyID(i+1) != partyToRestart {
			correctParties = append(correctParties, types.PartyID(i+1))
		}
	}
	PullFromAssemblers(t, uc, correctParties, 0, math.MaxUint64, 1500, 0, errString)

	consenterToRestart.RestartArmaNode(t, readyChan, numOfParties)
	testutil.WaitReady(t, readyChan, 1, 10)

	waitForTxSent.Add(1)
	go func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", strconv.Itoa(500), "--rate", rate, "--txSize", txSize})
		waitForTxSent.Done()
	}()

	waitForTxSent.Wait()

	PullFromAssemblers(t, uc, parties, 0, math.MaxUint64, 2000, 0, errString)
}
