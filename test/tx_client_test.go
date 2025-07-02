/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

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
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxClientSend(t *testing.T) {
	totalTxNumber := 100
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	var broadcastClient *client.BroadCastTxClient

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	listeners := testutil.CreateNetwork(t, configPath, 4, 2, "none", "none")
	require.NoError(t, err)
	// 2.
	armageddon := armageddon.NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2"})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, listeners)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, 20, 10)

	// 4. Send To Routers
	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)
	broadcastClient = client.NewBroadCastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()
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
	assert.Equal(t, totalTxNumber+1, totalTxs)
	assert.True(t, totalBlocks > 2)
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
