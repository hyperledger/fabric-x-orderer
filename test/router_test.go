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
		err = broadcastClient.SendTx(txContent)
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

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(totalTxSent+i, 64, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
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
	routerToStop.RestartArmaNode(t, readyChan, numOfParties)
	testutil.WaitReady(t, readyChan, 1, 10)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(totalTxSent+i, 64, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
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
		err = broadcastClient.SendTx(txContent)
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
	primaryRouterToStop.RestartArmaNode(t, readyChan, numOfParties)
	testutil.WaitReady(t, readyChan, 1, 10)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(totalTxSent+i, 64, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
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
