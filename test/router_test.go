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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
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
// 7. Stop a router.
// 8. Pull blocks only from correct assemblers.
// 9. Restart the router.
// 10. Pull blocks from all assemblers again.
// 11. Stop the broadcast client.
// 12. Pull blocks from all assemblers again.
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
	startPullingChannel := make(chan int, 1)
	defer close(startPullingChannel)
	clientResponseChannel := make(chan client.BroadcastClientResponse, 10000)
	wait := sync.WaitGroup{}

	wait.Add(1)
	go func() {
		defer wait.Done()
		// send transactions until the broadcast client is stopped
		// or a router is stopped
		wasRouterStopped := false
		txSent := 0

		for {
			status := rl.GetToken()
			if !status {
				fmt.Fprintf(os.Stderr, "failed to send tx %d", txSent+1)
				os.Exit(3)
			}
			txContent := armageddon.PrepareTx(txSent, 64, []byte("sessionNumber"))
			err = broadcastClient.FastSendTx(uint32(txSent), txContent, clientResponseChannel)
			if err != nil {
				// if a stream is closed, we assume the client is stopped
				if strings.Contains(err.Error(), "rpc error: code = Internal desc = SendMsg called after CloseSend") ||
					strings.Contains(err.Error(), "broadcast client is stopped") {
					break
				}

				// EOF error is expected when a router is stopped
				if err.Error() == "EOF" {
					startPullingChannel <- txSent
					require.False(t, wasRouterStopped)
					wasRouterStopped = true
				}
				// we can continue sending transactions
			} else if wasRouterStopped { // wait for a router to be restarted
				startPullingChannel <- txSent
				wasRouterStopped = false
			}

			txSent++
		}

		startPullingChannel <- txSent
	}()

	totalSuccessfulTx := 0

	wait.Add(1)
	go func() {
		defer wait.Done()
		// process responses
		for response := range clientResponseChannel {
			if response.Error != nil {
				fmt.Fprintf(os.Stderr, "some error occurred during getting response for tx %d: %v", response.Id, response.Error)
				continue
			}
			if response.Status != common.Status_SUCCESS {
				fmt.Fprintf(os.Stderr, "received error on response for: tx %d %v", response.Id, response.Error)
				continue
			}

			fmt.Fprintf(os.Stdout, "received successful response for tx %d from router: %s\n", response.Id, response.Endpoint)
			totalSuccessfulTx++
		}

		fmt.Fprintf(os.Stdout, "total successful tx reponses: %d\n", totalSuccessfulTx)
	}()

	parties := []types.PartyID{}
	for partyID := 1; partyID <= numOfParties; partyID++ {
		parties = append(parties, types.PartyID(partyID))
	}

	time.Sleep(5 * time.Second)

	// 7. Stop a router.
	routerToStop := armaNetwork.GetRouter(t, types.PartyID(1))

	t.Logf("Stopping router: party %d", routerToStop.PartyId)
	routerToStop.StopArmaNode()

	select {
	case totalTxSent = <-startPullingChannel:
		t.Logf("Router %d stopped successfully", routerToStop.PartyId)
	case <-time.After(10 * time.Second):
		require.Fail(t, "Router did not stop in time")
	}

	// 8. Pull blocks from all assemblers sent before a router was stopped.
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

	time.Sleep(5 * time.Second)

	// 9. Restart the router.
	routerToStop.RestartArmaNode(t, readyChan, numOfParties)
	testutil.WaitReady(t, readyChan, 1, 10)

	select {
	case totalTxSent = <-startPullingChannel:
		t.Logf("Router %d restarted successfully", routerToStop.PartyId)
	case <-time.After(10 * time.Second):
		require.Fail(t, "Router did not restart in time")
	}
	correctParties := []types.PartyID{types.PartyID(2), types.PartyID(3), types.PartyID(4)}

	// 10. Pull blocks only from correct assemblers sent before a router was restarted.
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:       uc,
		Parties:          correctParties,
		StartBlock:       0,
		EndBlock:         math.MaxUint64,
		Transactions:     totalTxSent + 1,
		Timeout:          60,
		NeedVerification: true,
		ErrString:        "cancelled pull from assembler: %d",
	})

	time.Sleep(5 * time.Second)

	// 11. Stop the broadcast client.
	broadcastClient.Stop()
	select {
	case totalTxSent = <-startPullingChannel:
		t.Log("Broadcast client stopped successfully")
	case <-time.After(10 * time.Second):
		require.Fail(t, "Broadcast client did not stop in time")
	}

	// 12. Pull blocks from all assemblers sent before the broadcast client was stopped.
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

	close(clientResponseChannel)
	wait.Wait()
}
