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
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatcherRestartRecover(t *testing.T) {

	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	numOfParties := 4
	numOfShards := 4

	t.Logf("Running test with %d parties and %d shards", numOfParties, numOfShards)

	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)
	// 2.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2"})

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

	// 4. Send To Routers
	totalTxNumber := 500
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	rate := 500

	capacity := rate / fillFrequency
	rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
		os.Exit(3)
	}

	broadcastClient := client.NewBroadCastTxClient(uc, 10*time.Second)

	for i := 0; i < totalTxNumber; i++ {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := prepareTx(i, 64, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
		require.NoError(t, err)
	}

	// 5. Check If Transaction is sent to all parties
	t.Log("Finished submit")
	broadcastClient.Stop()

	parties := []types.PartyID{}
	for partyID := 1; partyID <= numOfParties; partyID++ {
		parties = append(parties, types.PartyID(partyID))
	}

	PullFromAssemblers(t, uc, parties, 0, math.MaxUint64, totalTxNumber, numOfShards+1, "cancelled pull from assembler: %d")

	for batcherDown := 0; batcherDown < 4; batcherDown++ {
		partyToRestart := types.PartyID(3)
		batcherToRestart := armaNetwork.GeBatcher(t, partyToRestart, types.ShardID(batcherDown+1))
		batcherToRestart.StopArmaNode()

		newUc := armageddon.UserConfig{TLSCACerts: uc.TLSCACerts, TLSPrivateKey: uc.TLSPrivateKey, TLSCertificate: uc.TLSCertificate, UseTLSRouter: uc.UseTLSRouter}
		routerToStall := armaNetwork.GetRouter(t, partyToRestart)
		newUc.RouterEndpoints = append(newUc.RouterEndpoints, routerToStall.Listener.Addr().String())

		broadcastClient = client.NewBroadCastTxClient(&newUc, 10*time.Second)

		for i := 0; i < totalTxNumber; i++ {
			status := rl.GetToken()
			if !status {
				fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
				os.Exit(3)
			}
			txContent := prepareTx(i, 64, []byte("sessionNumber"))
			err = broadcastClient.SendTx(txContent)
			if err != nil {
				break
			}
		}
		require.Error(t, err)

		broadcastClient.Stop()
	}
}
