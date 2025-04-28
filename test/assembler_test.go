package test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/cmd/armageddon"
	"github.ibm.com/decentralized-trust-research/arma/cmd/testutils"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/testutil"
	"github.ibm.com/decentralized-trust-research/arma/testutil/client"
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
	netInfo := testutils.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2"})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutils.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)

	defer armaNetwork.Stop()

	testutils.WaitReady(t, readyChan, 20, 10)

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

	testutils.WaitReady(t, readyChan, 1, 10)

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

	err = dc.PullBlocks(toCtx, partyToRestart, 0, math.MaxUint64, handler)
	require.ErrorContains(t, err, "cancelled pull from assembler: 3")
	require.True(t, totalTxs == uint64(transactions*2))

	t.Logf("Finished pull and count: %d, %d", totalBlocks, totalTxs)
}
