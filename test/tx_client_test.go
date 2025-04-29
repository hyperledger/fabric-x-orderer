package test

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"github.ibm.com/decentralized-trust-research/arma/cmd/testutils"
	"github.ibm.com/decentralized-trust-research/arma/common/tools/armageddon"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.com/stretchr/testify/require"

	"github.ibm.com/decentralized-trust-research/arma/testutil/client"
)

func TestTxClientSend(t *testing.T) {
	totalTxNumber := 100
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	var broadcastClient *client.BroadCastTxClient

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	listeners := testutils.CreateNetwork(t, configPath, 4, 2, "none", "none")
	require.NoError(t, err)
	// 2.
	armageddon := armageddon.NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2"})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutils.RunArmaNodes(t, dir, armaBinaryPath, readyChan, listeners)
	defer armaNetwork.Stop()

	startTimeout := time.After(30 * time.Second)
	for i := 0; i < 20; i++ {
		select {
		case <-readyChan:
		case <-startTimeout:
			require.Fail(t, "arma nodes failed to start in time")
		}
	}
	// 4. Send To Routers
	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)
	broadcastClient = client.NewBroadCastTxClient(uc, 10*time.Second)
	require.NoError(t, err)
	for i := 0; i < totalTxNumber; i++ {
		txContent := prepareTx(i, 100, []byte("sessionNumber"))
		err = broadcastClient.SendTx(txContent)
		require.NoError(t, err)
	}
	// 5. Check If Transaction is sent
	t.Log("Finished submit")

	// Pull some block from the middle and count them
	startBlock := uint64(0)
	endBlock := uint64(5)
	totalTxs := 0
	totalBlocks := 0

	dc := client.NewDeliverClient(uc)
	cnx, cancel := context.WithCancel(context.Background())
	handler := func(block *common.Block) error {
		totalTxs += len(block.Data.Data)
		totalBlocks++
		if totalTxs == totalTxNumber+1 {
			cancel()
			return context.Canceled
		}
		return nil
	}
	dc.PullBlocks(cnx, 1, startBlock, endBlock, handler)
	defer broadcastClient.Stop()
	assert.Equal(t, totalTxNumber+1, totalTxs)

	t.Logf("Finished pull and count: %d, %d", totalBlocks, totalTxs)
}

func prepareTx(txNumber int, txSize int, sessionNumber []byte) []byte {
	// create timestamp (8 bytes)
	timeStamp := uint64(time.Now().UnixNano())

	// prepare the payload
	buffer := make([]byte, txSize)
	buff := bytes.NewBuffer(buffer[:0])
	buff.Write(sessionNumber)
	binary.Write(buff, binary.BigEndian, uint64(txNumber))
	binary.Write(buff, binary.BigEndian, timeStamp)
	result := buff.Bytes()
	if len(buff.Bytes()) < txSize {
		padding := make([]byte, txSize-len(result))
		result = append(result, padding...)
	}
	return result
}
