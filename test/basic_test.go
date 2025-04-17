package test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/cmd/armageddon"
	"github.ibm.com/decentralized-trust-research/arma/cmd/testutils"
	"github.ibm.com/decentralized-trust-research/arma/testutil"
	"github.ibm.com/decentralized-trust-research/arma/testutil/client"
)

// Scenario:
// 1. Create a config YAML file to be an input to armageddon
// 2. Run armageddon generate command to create config files in a folder structure
// 3. Run arma with the generated config files to run each of the nodes for all parties
// 4. Run armageddon submit command to make 10000 txs and send them to all routers at a specified rate
// 5. In parallel, run armageddon receive command to pull blocks from the assembler and report results
func TestSubmitAndReceive(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	listeners := testutils.CreateNetwork(t, configPath, 4, "TLS", "TLS")

	// 2.
	armageddonCLI := armageddon.NewCLI()
	armageddonCLI.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2"})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotEmpty(t, armaBinaryPath)
	defer os.RemoveAll(armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	sessions := testutils.RunArmaNodes(t, dir, armaBinaryPath, readyChan, listeners)
	defer func() {
		for i := range sessions {
			sessions[i].Kill()
		}
	}()

	startTimeout := time.After(10 * time.Second)
	for i := 0; i < 20; i++ {
		select {
		case <-readyChan:
		case <-startTimeout:
			require.Fail(t, "arma nodes failed to start in time")
		}
	}

	// 4. + 5.
	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
	rate := "200"
	txs := "1000"
	txSize := "64"

	var waitForTxToBeSentAndReceived sync.WaitGroup
	waitForTxToBeSentAndReceived.Add(1)
	go func() {
		armageddonCLI.Run([]string{"submit", "--config", userConfigPath, "--transactions", txs, "--rate", rate, "--txSize", txSize})
		waitForTxToBeSentAndReceived.Done()
	}()

	waitForTxToBeSentAndReceived.Wait()
	t.Log("Finished submit")

	// Pull some block from the middle and count them
	startBlock := uint64(3)
	endBlock := uint64(5)
	totalTxs := 0
	totalBlocks := 0

	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	dc := client.NewDeliverClient(uc)
	handler := func(block *common.Block) error {
		if block == nil {
			return errors.New("nil block")
		}
		if block.Header == nil {
			return errors.New("nil block header")
		}

		totalTxs += len(block.GetData().GetData())
		totalBlocks++
		return nil
	}

	toCtx, toCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer toCancel()

	err = dc.PullBlocks(toCtx, 1, startBlock, endBlock, handler)
	assert.NoError(t, err)
	assert.Equal(t, 3, totalBlocks)
	assert.True(t, totalTxs >= 3)

	t.Logf("Finished pull and count: %d, %d", totalBlocks, totalTxs)
}
