/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// package test contains integration tests.
package test

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	node2 "github.ibm.com/decentralized-trust-research/arma/node"
	"github.ibm.com/decentralized-trust-research/arma/node/assembler"
	"github.ibm.com/decentralized-trust-research/arma/node/comm/tlsgen"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	"github.ibm.com/decentralized-trust-research/arma/node/router"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
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

	for i := 0; i < 4; i++ {
		t.Logf("batcher: %v, %s", batcherInfos[i], batcherNodes[i].ToString())
	}

	shards := []config.ShardInfo{{ShardId: 1, Batchers: batcherInfos}}

	_, clean := createConsenters(t, numParties, consenterNodes, consenterInfos, shards)
	defer clean()

	_, _, _, clean = createBatchersForShard(t, numParties, batcherNodes, shards, consenterInfos, shards[0].ShardId)
	defer clean()

	routers := createRouters(t, numParties, batcherInfos, ca, shards[0].ShardId)

	for i := range routers {
		routers[i].StartRouterService()
	}

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	assemblerDir, err := os.MkdirTemp("", fmt.Sprintf("%s-assembler", t.Name()))
	require.NoError(t, err)

	assemblerConf := &config.AssemblerNodeConfig{
		TLSPrivateKeyFile:         ckp.Key,
		TLSCertificateFile:        ckp.Cert,
		PartyId:                   1,
		Directory:                 assemblerDir,
		ListenAddress:             "0.0.0.0:0",
		PrefetchBufferMemoryBytes: 1 * 1024 * 1024 * 1024, // 1GB
		RestartLedgerScanTimeout:  5 * time.Second,
		PrefetchEvictionTtl:       time.Hour,
		ReplicationChannelSize:    100,
		BatchRequestsChannelSize:  1000,
		Shards:                    shards,
		Consenter:                 consenterInfos[0],
		UseTLS:                    true,
		ClientAuthRequired:        false,
	}

	aLogger := testutil.CreateLogger(t, 1)

	assemblerGRPC := node2.CreateGRPCAssembler(assemblerConf)
	assembler := assembler.NewAssembler(assemblerConf, assemblerGRPC, nil, aLogger)

	orderer.RegisterAtomicBroadcastServer(assemblerGRPC.Server(), assembler)

	go func() {
		assemblerGRPC.Start()
	}()

	//_, assemblerPort, err := net.SplitHostPort(assemblerGRPC.Address())
	//require.NoError(t, err)

	// runPerf(t, [][]byte{ca.CertBytes()}, [][]byte{ca.CertBytes()}, routerEndpoints, fmt.Sprintf("127.0.0.1:%s", assemblerPort), clientPath)
	sendTransactions(t, routers, assembler)
}

func sendTransactions(t *testing.T, routers []*router.Router, assembler *assembler.Assembler) {
	sendTxn(runtime.NumCPU()+1, 0, routers)

	time.Sleep(time.Second)

	var wg sync.WaitGroup
	wg.Add(runtime.NumCPU())

	workPerWorker := 100

	start := time.Now()

	for workerID := 0; workerID < runtime.NumCPU(); workerID++ {
		go func(workerID int) {
			defer wg.Done()

			for txNum := 0; txNum < workPerWorker; txNum++ {
				sendTxn(workerID, txNum, routers)
			}
		}(workerID)
	}

	wg.Wait()

	totalTxn := workPerWorker * runtime.NumCPU()
	t.Logf("Expecting %d TXs", totalTxn)
	require.Eventually(t, func() bool {
		n := assembler.GetTxCount()
		t.Logf("Received TXs: %d", n)
		return int(n) >= totalTxn
	}, 30*time.Second, 1000*time.Millisecond)

	elapsed := int(time.Since(start).Seconds())
	if elapsed == 0 {
		elapsed = 1
	}

	fmt.Println(totalTxn / elapsed)
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
