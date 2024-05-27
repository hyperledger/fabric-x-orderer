package node

import (
	arma "arma/pkg"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go/orderer"
	_ "github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm/tlsgen"
	protos "github.ibm.com/Yacov-Manevich/ARMA/node/protos/comm"
	"google.golang.org/grpc/grpclog"
)

func TestABCR(t *testing.T) {
	grpclog.SetLoggerV2(&silentLogger{})
	//clientPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/orderingservice-experiments/clients/cmd/client", []string{"GOPRIVATE=github.ibm.com"}, "-mod=mod")
	//require.NoError(t, err)
	//clientPath := "ordererclient"

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	batcherInfos, consenterInfos, batcherNodes, consenterNodes := createConsentersAndBatchers(t, ca)

	shards := []ShardInfo{{ShardId: 1, Batchers: batcherInfos}}

	batchers := createBatchers(t, batcherNodes, shards, consenterInfos)

	_, clean := createConsenters(t, consenterNodes, consenterInfos, shards)
	defer clean()

	routers, configs := createRouters(t, batcherInfos, ca)

	for _, b := range batchers {
		go b.b.Run()
	}

	var routerEndpoints []string
	for i, rConf := range configs {
		gRPC := CreateGRPCRouter(rConf)
		_, port, _ := net.SplitHostPort(gRPC.Address())
		routerEndpoints = append(routerEndpoints, fmt.Sprintf("127.0.0.1:%s", port))
		orderer.RegisterAtomicBroadcastServer(gRPC.Server(), routers[i])
		go func() {
			gRPC.Start()
		}()
	}

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	assemblerDir, err := os.MkdirTemp("", fmt.Sprintf("%s-assembler", t.Name()))
	require.NoError(t, err)

	assemberConf := AssemblerNodeConfig{
		ListenAddress:      "0.0.0.0:0",
		PartyId:            1,
		TLSPrivateKeyFile:  ckp.Key,
		TLSCertificateFile: ckp.Cert,
		Shards:             shards,
		Consenter:          consenterInfos[0],
		Directory:          assemblerDir,
	}

	aLogger := createLogger(t, 1)
	assembler := CreateAssembler(assemberConf, aLogger)

	assemblerGRPC := CreateGRPCAssembler(assemberConf)
	orderer.RegisterAtomicBroadcastServer(assemblerGRPC.Server(), assembler)

	go func() {
		assemblerGRPC.Start()
	}()

	//_, assemblerPort, err := net.SplitHostPort(assemblerGRPC.Address())
	//require.NoError(t, err)

	//runPerf(t, [][]byte{ca.CertBytes()}, [][]byte{ca.CertBytes()}, routerEndpoints, fmt.Sprintf("127.0.0.1:%s", assemblerPort), clientPath)
	sendTransactions(routers, assembler)
}

func sendTransactions(routers []*Router, assembler *Assembler) {
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

	txCount := &assembler.assembler.Ledger.(*AssemblerLedger).TransactionCount

	totalTxn := workPerWorker * runtime.NumCPU()
	for int(atomic.LoadUint64(txCount)) < totalTxn {
		time.Sleep(time.Millisecond * 100)
	}

	elapsed := int(time.Since(start).Seconds())
	if elapsed == 0 {
		elapsed = 1
	}

	fmt.Println(totalTxn / elapsed)
}

func runPerf(t *testing.T, routerTLSCA, assemblerTLSCA [][]byte, routerEndpoints []string, assemblerEndpoint string, clientPath string) {

	perfConfigDir, err := os.MkdirTemp("", fmt.Sprintf("%s-perf", t.Name()))
	require.NoError(t, err)

	configFile := template
	for i := 0; i < len(routerEndpoints); i++ {
		configFile = strings.Replace(configFile, fmt.Sprintf("{ORDERER%d}", i+1), routerEndpoints[i], -1)
	}

	tlsCABuff := bytes.Buffer{}
	for _, rtca := range routerTLSCA {
		tlsCABuff.Write(rtca)
	}
	for _, atca := range assemblerTLSCA {
		tlsCABuff.Write(atca)
	}

	defer os.RemoveAll(perfConfigDir)

	tlsCAFilePath := filepath.Join(perfConfigDir, "tlsCAs.pem")
	err = os.WriteFile(tlsCAFilePath, tlsCABuff.Bytes(), 0644)
	require.NoError(t, err)

	configFile = strings.Replace(configFile, "{TLSCACERTS}", tlsCAFilePath, -1)
	configFile = strings.Replace(configFile, "{ASSEMBLER}", assemblerEndpoint, -1)

	configFilePath := filepath.Join(perfConfigDir, "config.yaml")
	err = os.WriteFile(configFilePath, []byte(configFile), 0644)
	require.NoError(t, err)

	cmd := exec.Command(clientPath, "--configs", configFilePath)
	var processOut safeBuff
	cmd.Stderr = &processOut
	cmd.Stdout = &processOut

	go func() {
		err = cmd.Start()
		require.NoError(t, err)
	}()

	for {
		time.Sleep(time.Second * 1)
		buffContent := processOut.String()
		if strings.Contains(buffContent, "Received block 10 from orderer") {
			break
		}
	}

	fmt.Println(processOut.String())

}

type safeBuff struct {
	lock sync.Mutex
	bytes.Buffer
}

func (sb *safeBuff) Write(p []byte) (n int, err error) {
	sb.lock.Lock()
	defer sb.lock.Unlock()
	return sb.Buffer.Write(p)
}

func (sb *safeBuff) String() string {
	sb.lock.Lock()
	defer sb.lock.Unlock()
	return sb.Buffer.String()
}

func sendTxn(workerID int, txnNum int, routers []*Router) {
	txn := make([]byte, 32)
	binary.BigEndian.PutUint64(txn, uint64(txnNum))
	binary.BigEndian.PutUint16(txn[30:], uint16(workerID))

	for routerId := 0; routerId < 4; routerId++ {
		routers[routerId].Submit(context.Background(), &protos.Request{Payload: txn})
	}
}

func createRouters(t *testing.T, batcherInfos []BatcherInfo, ca tlsgen.CA) ([]*Router, []RouterNodeConfig) {
	var configs []RouterNodeConfig
	var routers []*Router
	for i := 0; i < 4; i++ {
		l := createLogger(t, i)
		kp, err := ca.NewServerCertKeyPair("127.0.0.1")
		require.NoError(t, err)
		config := RouterNodeConfig{
			ListenAddress:      "0.0.0.0:0",
			TLSPrivateKeyFile:  kp.Key,
			TLSCertificateFile: kp.Cert,
			PartyID:            uint16(i + 1),
			Shards: []ShardInfo{{
				ShardId:  1,
				Batchers: batcherInfos,
			}},
		}
		configs = append(configs, config)
		router := CreateRouter(config, l)
		routers = append(routers, router)
	}
	return routers, configs
}

func createConsenters(t *testing.T, consenterNodes []*node, consenterInfos []ConsenterInfo, shardInfo []ShardInfo) ([]*Consensus, func()) {
	var consensuses []*Consensus

	var cleans []func()

	for i := 0; i < 4; i++ {

		gRPCServer := consenterNodes[i].Server()

		partyID := arma.PartyID(i + 1)

		logger := createLogger(t, int(partyID))

		sk, err := x509.MarshalPKCS8PrivateKey(consenterNodes[i].sk)
		require.NoError(t, err)

		dir, err := os.MkdirTemp("", fmt.Sprintf("%s-consenter%d", t.Name(), i+1))
		require.NoError(t, err)

		cleans = append(cleans, func() {
			defer os.RemoveAll(dir)
		})

		conf := ConsenterNodeConfig{
			ListenAddress:      "0.0.0.0:0",
			Shards:             shardInfo,
			Consenters:         consenterInfos,
			PartyId:            uint16(partyID),
			TLSPrivateKeyFile:  consenterNodes[i].TLSKey,
			TLSCertificateFile: consenterNodes[i].TLSCert,
			SigningPrivateKey:  pem.EncodeToMemory(&pem.Block{Bytes: sk}),
			Directory:          dir,
		}

		c := CreateConsensus(conf, logger)

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
		for _, clean := range cleans {
			clean()
		}
	}
}

func createBatchers(t *testing.T, batcherNodes []*node, shards []ShardInfo, consenterInfos []ConsenterInfo) []*Batcher {
	var batchers []*Batcher

	for i := 0; i < 4; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("%s-batcher%d", t.Name(), i+1))
		require.NoError(t, err)

		key, err := x509.MarshalPKCS8PrivateKey(batcherNodes[i].sk)
		require.NoError(t, err)

		batcherConf := BatcherNodeConfig{
			ListenAddress:      "0.0.0.0:0",
			Shards:             shards,
			ShardId:            1,
			PartyId:            uint16(i + 1),
			Consenters:         consenterInfos,
			TLSPrivateKeyFile:  batcherNodes[i].TLSKey,
			TLSCertificateFile: batcherNodes[i].TLSCert,
			SigningPrivateKey:  RawBytes(pem.EncodeToMemory(&pem.Block{Bytes: key})),
			Directory:          dir,
		}

		batcher := CreateBatcher(batcherConf, createLogger(t, i+1))
		batchers = append(batchers, batcher)
		protos.RegisterRequestTransmitServer(batcherNodes[i].Server(), batcher)
		protos.RegisterAckServiceServer(batcherNodes[i].Server(), batcher)
		orderer.RegisterAtomicBroadcastServer(batcherNodes[i].Server(), batcher)
		go batcherNodes[i].Start()
		t.Log("Batcher gRPC service listening on", batcherNodes[i].Address())
	}

	return batchers
}

func createConsentersAndBatchers(t *testing.T, ca tlsgen.CA) ([]BatcherInfo, []ConsenterInfo, []*node, []*node) {
	batcherNodes := createNodes(t, ca)
	consenterNodes := createNodes(t, ca)

	var batchers []BatcherInfo
	for i := 0; i < 4; i++ {
		batchers = append(batchers, BatcherInfo{
			PartyID:    uint16(i + 1),
			Endpoint:   batcherNodes[i].Address(),
			TLSCert:    batcherNodes[i].TLSCert,
			TLSCACerts: []RawBytes{ca.CertBytes()},
			PublicKey:  batcherNodes[i].pk,
		})
	}

	var consenters []ConsenterInfo
	for i := 0; i < 4; i++ {
		consenters = append(consenters, ConsenterInfo{
			PartyID:    uint16(i + 1),
			Endpoint:   consenterNodes[i].Address(),
			TLSCACerts: []RawBytes{ca.CertBytes()},
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
	var pks []RawBytes

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
	pk      RawBytes
}

type silentLogger struct {
}

func (s *silentLogger) Info(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Infoln(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Infof(format string, args ...any) {
	//TODO implement me

}

func (s *silentLogger) Warning(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Warningln(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Warningf(format string, args ...any) {
	//TODO implement me

}

func (s *silentLogger) Error(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Errorln(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Errorf(format string, args ...any) {
	//TODO implement me

}

func (s *silentLogger) Fatal(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Fatalln(args ...any) {
	//TODO implement me

}

func (s *silentLogger) Fatalf(format string, args ...any) {
	//TODO implement me

}

func (s *silentLogger) V(l int) bool {
	return false

}
