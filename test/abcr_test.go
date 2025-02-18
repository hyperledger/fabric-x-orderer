// package test contains integration tests.
package test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"arma/common/types"
	node2 "arma/node"
	"arma/node/assembler"
	"arma/node/batcher"
	"arma/node/comm"
	"arma/node/comm/tlsgen"
	"arma/node/config"
	"arma/node/consensus"
	protos "arma/node/protos/comm"
	"arma/node/router"
	"arma/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	_ "github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/grpclog"
)

func TestABCR(t *testing.T) {
	grpclog.SetLoggerV2(&testutil.SilentLogger{})

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	batcherInfos, consenterInfos, batcherNodes, consenterNodes := createConsentersAndBatchers(t, ca)
	for i := 0; i < 4; i++ {
		t.Logf("batcher: %v, %s", batcherInfos[i], batcherNodes[i].ToString())
	}

	shards := []config.ShardInfo{{ShardId: 1, Batchers: batcherInfos}}

	_, clean := createConsenters(t, consenterNodes, consenterInfos, shards)
	defer clean()

	batchers := createBatchers(t, batcherNodes, shards, consenterInfos)

	routers, configs := createRouters(t, batcherInfos, ca)

	for _, b := range batchers {
		b.Run()
		defer b.Stop()
	}

	for i, rConf := range configs {
		gRPC := node2.CreateGRPCRouter(rConf)
		orderer.RegisterAtomicBroadcastServer(gRPC.Server(), routers[i])
		go func() {
			gRPC.Start()
		}()
	}

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	assemblerDir, err := os.MkdirTemp("", fmt.Sprintf("%s-assembler", t.Name()))
	require.NoError(t, err)

	assemblerConf := config.AssemblerNodeConfig{
		ListenAddress:      "0.0.0.0:0",
		PartyId:            1,
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		Shards:             shards,
		Consenter:          consenterInfos[0],
		Directory:          assemblerDir,
		UseTLS:             true,
	}

	aLogger := testutil.CreateLogger(t, 1)
	assembler := assembler.NewAssembler(assemblerConf, nil, aLogger)

	assemblerGRPC := node2.CreateGRPCAssembler(assemblerConf)
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

func sendTxn(workerID int, txnNum int, routers []*router.Router) {
	txn := make([]byte, 32)
	binary.BigEndian.PutUint64(txn, uint64(txnNum))
	binary.BigEndian.PutUint16(txn[30:], uint16(workerID))

	for routerId := 0; routerId < 4; routerId++ {
		routers[routerId].Submit(context.Background(), &protos.Request{Payload: txn})
	}
}

func createRouters(t *testing.T, batcherInfos []config.BatcherInfo, ca tlsgen.CA) ([]*router.Router, []config.RouterNodeConfig) {
	var configs []config.RouterNodeConfig
	var routers []*router.Router
	for i := 0; i < 4; i++ {
		l := testutil.CreateLogger(t, i)
		kp, err := ca.NewServerCertKeyPair("127.0.0.1")
		require.NoError(t, err)
		config := config.RouterNodeConfig{
			ListenAddress:      "0.0.0.0:0",
			TLSPrivateKeyFile:  kp.Key,
			TLSCertificateFile: kp.Cert,
			PartyID:            types.PartyID(i + 1),
			Shards: []config.ShardInfo{{
				ShardId:  1,
				Batchers: batcherInfos,
			}},
			UseTLS: true,
		}
		configs = append(configs, config)
		router := router.NewRouter(config, l)
		routers = append(routers, router)
	}
	return routers, configs
}

func createConsenters(t *testing.T, consenterNodes []*node, consenterInfos []config.ConsenterInfo, shardInfo []config.ShardInfo) ([]*consensus.Consensus, func()) {
	var consensuses []*consensus.Consensus

	var cleans []func()

	for i := 0; i < 4; i++ {

		gRPCServer := consenterNodes[i].Server()

		partyID := types.PartyID(i + 1)

		logger := testutil.CreateLogger(t, int(partyID))

		sk, err := x509.MarshalPKCS8PrivateKey(consenterNodes[i].sk)
		require.NoError(t, err)

		dir, err := os.MkdirTemp("", fmt.Sprintf("%s-consenter%d", t.Name(), i+1))
		require.NoError(t, err)

		cleans = append(cleans, func() {
			defer os.RemoveAll(dir)
		})

		conf := config.ConsenterNodeConfig{
			ListenAddress:      "0.0.0.0:0",
			Shards:             shardInfo,
			Consenters:         consenterInfos,
			PartyId:            partyID,
			TLSPrivateKeyFile:  consenterNodes[i].TLSKey,
			TLSCertificateFile: consenterNodes[i].TLSCert,
			SigningPrivateKey:  pem.EncodeToMemory(&pem.Block{Bytes: sk}),
			Directory:          dir,
		}

		c := consensus.CreateConsensus(conf, nil, logger)
		c.Net = consenterNodes[i].GRPCServer

		consensuses = append(consensuses, c)
		protos.RegisterConsensusServer(gRPCServer, c)
		orderer.RegisterAtomicBroadcastServer(gRPCServer, c.DeliverService)
		orderer.RegisterClusterNodeServiceServer(gRPCServer, c)
		go consenterNodes[i].Start()
		err = c.Start()
		require.NoError(t, err)
		t.Log("Consenter gRPC service listening on", consenterNodes[i].Address())
	}

	return consensuses, func() {
		for i, clean := range cleans {
			clean()
			consensuses[i].Stop()
		}
	}
}

func createBatchers(t *testing.T, batcherNodes []*node, shards []config.ShardInfo, consenterInfos []config.ConsenterInfo) []*batcher.Batcher {
	var batchers []*batcher.Batcher
	var lock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(4)

	for i := 0; i < 4; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("%s-batcher%d", t.Name(), i+1))
		require.NoError(t, err)

		key, err := x509.MarshalPKCS8PrivateKey(batcherNodes[i].sk)
		require.NoError(t, err)

		batcherConf := config.BatcherNodeConfig{
			ListenAddress:      "0.0.0.0:0",
			Shards:             shards,
			ShardId:            1,
			PartyId:            types.PartyID(i + 1),
			Consenters:         consenterInfos,
			TLSPrivateKeyFile:  batcherNodes[i].TLSKey,
			TLSCertificateFile: batcherNodes[i].TLSCert,
			SigningPrivateKey:  config.RawBytes(pem.EncodeToMemory(&pem.Block{Bytes: key})),
			Directory:          dir,
		}

		go func(i int) {
			defer wg.Done()

			batcher := batcher.CreateBatcher(batcherConf, testutil.CreateLogger(t, i+1), batcherNodes[i], &batcher.ConsensusStateReplicatorFactory{})
			lock.Lock()
			batchers = append(batchers, batcher)
			lock.Unlock()
			protos.RegisterRequestTransmitServer(batcherNodes[i].Server(), batcher)
			protos.RegisterAckServiceServer(batcherNodes[i].Server(), batcher)
			orderer.RegisterAtomicBroadcastServer(batcherNodes[i].Server(), batcher)
			go batcherNodes[i].Start()
			t.Log("Batcher gRPC service listening on", batcherNodes[i].Address())
		}(i)
	}

	wg.Wait()

	return batchers
}

func createConsentersAndBatchers(t *testing.T, ca tlsgen.CA) ([]config.BatcherInfo, []config.ConsenterInfo, []*node, []*node) {
	batcherNodes := createNodes(t, ca)
	consenterNodes := createNodes(t, ca)

	var batchers []config.BatcherInfo
	for i := 0; i < 4; i++ {
		batchers = append(batchers, config.BatcherInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   batcherNodes[i].Address(),
			TLSCert:    batcherNodes[i].TLSCert,
			TLSCACerts: []config.RawBytes{ca.CertBytes()},
			PublicKey:  batcherNodes[i].pk,
		})
	}

	var consenters []config.ConsenterInfo
	for i := 0; i < 4; i++ {
		consenters = append(consenters, config.ConsenterInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   consenterNodes[i].Address(),
			TLSCACerts: []config.RawBytes{ca.CertBytes()},
			PublicKey:  consenterNodes[i].pk,
		})
	}
	return batchers, consenters, batcherNodes, consenterNodes
}

func keygen(t *testing.T) (*ecdsa.PrivateKey, []byte) {
	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	rawPK, err := x509.MarshalPKIXPublicKey(&sk.PublicKey)
	require.NoError(t, err)
	return sk, rawPK
}

func createNodes(t *testing.T, ca tlsgen.CA) []*node {
	var result []*node

	var sks []*ecdsa.PrivateKey
	var pks []config.RawBytes

	for i := 0; i < 4; i++ {
		sk, rawPK := keygen(t)
		sks = append(sks, sk)
		pks = append(pks, pem.EncodeToMemory(&pem.Block{Bytes: rawPK, Type: "PUBLIC KEY"}))

	}

	for i := 0; i < 4; i++ {
		kp, err := ca.NewServerCertKeyPair("127.0.0.1")
		require.NoError(t, err)

		srv, err := comm.NewGRPCServer("127.0.0.1:0", comm.ServerConfig{
			SecOpts: comm.SecureOptions{
				ClientRootCAs:     [][]byte{ca.CertBytes()},
				Key:               kp.Key,
				Certificate:       kp.Cert,
				RequireClientCert: true,
				UseTLS:            true,
				ServerRootCAs:     [][]byte{ca.CertBytes()},
			},
		})
		require.NoError(t, err)

		result = append(result, &node{GRPCServer: srv, TLSKey: kp.Key, TLSCert: kp.Cert, pk: pks[i], sk: sks[i]})
	}
	return result
}

type node struct {
	*comm.GRPCServer
	TLSCert []byte
	TLSKey  []byte
	sk      *ecdsa.PrivateKey
	pk      config.RawBytes
}

func (n *node) ToString() string {
	return fmt.Sprintf("GRPC.Address: %s", n.GRPCServer.Address())
}
