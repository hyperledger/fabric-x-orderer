/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// package test contains integration tests.
package test

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/router"
	"github.com/hyperledger/fabric-x-orderer/testutil"

	_ "github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/grpclog"
)

func TestABCR(t *testing.T) {
	grpclog.SetLoggerV2(&testutil.SilentLogger{})

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	numParties := 4

	batcherNodes, batcherInfos := createBatcherNodesAndInfo(t, ca, numParties)
	consenterNodes, consenterInfos := createConsenterNodesAndInfo(t, ca, numParties)

	for i := 0; i < numParties; i++ {
		t.Logf("batcher: %v, %s", batcherInfos[i], batcherNodes[i].ToString())
	}

	shards := []config.ShardInfo{{ShardId: 1, Batchers: batcherInfos}}

	genesisBlock := utils.EmptyGenesisBlock("arma")

	_, _, _, cleanConsenters := createConsenters(t, numParties, consenterNodes, consenterInfos, shards, genesisBlock)

	_, _, _, cleanBatchers := createBatchersForShard(t, numParties, batcherNodes, shards, consenterInfos, shards[0].ShardId, genesisBlock)

	routers, _, _, _ := createRouters(t, numParties, batcherInfos, ca, shards[0].ShardId, make([]string, numParties), genesisBlock)

	for i := range routers {
		routers[i].StartRouterService()
	}

	assemblers, _, _, _, cleanAssemblers := createAssemblers(t, numParties, ca, shards, consenterInfos, genesisBlock)

	defer func() {
		for i := range routers {
			routers[i].Stop()
		}
		cleanBatchers()
		cleanConsenters()
		cleanAssemblers()
	}()

	sendTransactions(t, routers, assemblers[3])
}

func sendTransactions(t *testing.T, routers []*router.Router, assembler *assembler.Assembler) {
	sendTxn(runtime.NumCPU()+1, 0, routers)

	time.Sleep(time.Second)

	var wg sync.WaitGroup
	wg.Add(runtime.NumCPU())

	workPerWorker := 100

	initialCount := int(assembler.GetTxCount())

	start := time.Now()

	for workerID := 0; workerID < runtime.NumCPU(); workerID++ {
		go func(workerID int) {
			defer wg.Done()

			for txNum := 0; txNum < workPerWorker; txNum++ {
				sendTxn(workerID, initialCount+txNum, routers)
			}
		}(workerID)
	}

	wg.Wait()

	totalTxn := workPerWorker * runtime.NumCPU()
	expected := initialCount + totalTxn
	t.Logf("Expecting %d TXs (%d to %d)", totalTxn, initialCount, expected)
	require.Eventually(t, func() bool {
		n := assembler.GetTxCount()
		t.Logf("Received TXs: %d", n)
		return int(n) >= expected
	}, time.Minute, time.Second)

	elapsed := int(time.Since(start).Seconds())
	if elapsed == 0 {
		elapsed = 1
	}

	t.Logf("%f (totalTxn / elapsed)\n", float32(totalTxn)/float32(elapsed))
}

// func runPerf(t *testing.T, routerTLSCA, assemblerTLSCA [][]byte, routerEndpoints []string, assemblerEndpoint string, clientPath string) {
// 	perfConfigDir, err := os.MkdirTemp("", fmt.Sprintf("%s-perf", t.Name()))
// 	require.NoError(t, err)

// 	configFile := template
// 	for i := 0; i < len(routerEndpoints); i++ {
// 		configFile = strings.Replace(configFile, fmt.Sprintf("{ORDERER%d}", i+1), routerEndpoints[i], -1)
// 	}

// 	tlsCABuff := bytes.Buffer{}
// 	for _, rtca := range routerTLSCA {
// 		tlsCABuff.Write(rtca)
// 	}
// 	for _, atca := range assemblerTLSCA {
// 		tlsCABuff.Write(atca)
// 	}

// 	defer os.RemoveAll(perfConfigDir)

// 	tlsCAFilePath := filepath.Join(perfConfigDir, "tlsCAs.pem")
// 	err = os.WriteFile(tlsCAFilePath, tlsCABuff.Bytes(), 0o644)
// 	require.NoError(t, err)

// 	configFile = strings.Replace(configFile, "{TLSCACERTS}", tlsCAFilePath, -1)
// 	configFile = strings.Replace(configFile, "{ASSEMBLER}", assemblerEndpoint, -1)

// 	configFilePath := filepath.Join(perfConfigDir, "config.yaml")
// 	err = os.WriteFile(configFilePath, []byte(configFile), 0o644)
// 	require.NoError(t, err)

// 	cmd := exec.Command(clientPath, "--configs", configFilePath)
// 	var processOut safeBuff
// 	cmd.Stderr = &processOut
// 	cmd.Stdout = &processOut

// 	go func() {
// 		err = cmd.Start()
// 		require.NoError(t, err)
// 	}()

// 	for {
// 		time.Sleep(time.Second * 1)
// 		buffContent := processOut.String()
// 		if strings.Contains(buffContent, "Received block 10 from orderer") {
// 			break
// 		}
// 	}

// 	fmt.Println(processOut.String())
// }

// type safeBuff struct {
// 	lock sync.Mutex
// 	bytes.Buffer
// }

// func (sb *safeBuff) Write(p []byte) (n int, err error) {
// 	sb.lock.Lock()
// 	defer sb.lock.Unlock()
// 	return sb.Buffer.Write(p)
// }

// func (sb *safeBuff) String() string {
// 	sb.lock.Lock()
// 	defer sb.lock.Unlock()
// 	return sb.Buffer.String()
// }
