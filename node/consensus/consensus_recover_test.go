package consensus_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"testing"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/node/batcher"
	"arma/node/comm"
	"arma/node/comm/tlsgen"
	"arma/node/config"
	"arma/node/consensus"
	"arma/node/ledger"
	protos "arma/node/protos/comm"
	"arma/testutil"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/grpclog"
)

var (
	digest    = make([]byte, 32-3)
	digest123 = append([]byte{1, 2, 3}, digest...)
	digest124 = append([]byte{1, 2, 4}, digest...)
	digest125 = append([]byte{1, 2, 5}, digest...)
)

func TestCreateOneConsensusNode(t *testing.T) {
	grpclog.SetLoggerV2(&testutil.SilentLogger{})

	dir := t.TempDir()

	partyID := types.PartyID(1)

	logger := testutil.CreateLogger(t, int(partyID))

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	consenterNodes := createNodes(t, ca, 1, "127.0.0.1:0")

	consentersInfo := createConsentersInfo(1, consenterNodes, ca)

	batcherNodes := createNodes(t, ca, 1, "127.0.0.1:0")

	batchersInfo := createBatchersInfo(1, batcherNodes, ca)

	conf := makeConf(dir, consenterNodes[0], 1, consentersInfo, batchersInfo)

	c := consensus.CreateConsensus(conf, logger)
	grpcRegisterAndStart(c, consenterNodes[0])

	listener := &storageListener{c: make(chan *common.Block, 100)}

	c.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(listener)

	err = c.Start()
	require.NoError(t, err)

	baf123id1p1s1, err := batcher.CreateBAF(batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	assert.NoError(t, err)
	ce := &core.ControlEvent{BAF: baf123id1p1s1}

	err = c.SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b := <-listener.c
	require.Equal(t, uint64(1), b.Header.Number)

	baf124id1p1s2, err := batcher.CreateBAF(batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf124id1p1s2}

	err = c.SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b = <-listener.c
	require.Equal(t, uint64(2), b.Header.Number)

	c.Stop()

	consenterNodes[0].GRPCServer, err = newGRPCServer(consenterNodes[0].Address(), ca, &tlsgen.CertKeyPair{Key: consenterNodes[0].TLSKey, Cert: consenterNodes[0].TLSCert})
	require.NoError(t, err)

	c = consensus.CreateConsensus(conf, logger)
	grpcRegisterAndStart(c, consenterNodes[0])

	c.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(listener)

	err = c.Start()
	require.NoError(t, err)

	baf125id1p1s2, err := batcher.CreateBAF(batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf125id1p1s2}

	err = c.SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b = <-listener.c
	require.Equal(t, uint64(3), b.Header.Number)

	c.Stop()
}

func makeConf(dir string, n *node, partyID types.PartyID, consentersInfo []config.ConsenterInfo, batchersInfo []config.BatcherInfo) config.ConsenterNodeConfig {
	sk, err := x509.MarshalPKCS8PrivateKey(n.sk)
	if err != nil {
		panic(err)
	}

	return config.ConsenterNodeConfig{
		Shards:             []config.ShardInfo{{ShardId: 1, Batchers: batchersInfo}},
		Consenters:         consentersInfo,
		PartyId:            partyID,
		TLSPrivateKeyFile:  n.TLSKey,
		TLSCertificateFile: n.TLSCert,
		SigningPrivateKey:  pem.EncodeToMemory(&pem.Block{Bytes: sk}),
		Directory:          dir,
	}
}

func grpcRegisterAndStart(c *consensus.Consensus, n *node) {
	c.Net = n.GRPCServer
	gRPCServer := n.Server()

	protos.RegisterConsensusServer(gRPCServer, c)
	orderer.RegisterAtomicBroadcastServer(gRPCServer, c.DeliverService)
	orderer.RegisterClusterNodeServiceServer(gRPCServer, c)

	go func() {
		err := n.Start()
		if err != nil {
			panic(err)
		}
	}()
}

func createConsentersInfo(num int, nodes []*node, ca tlsgen.CA) []config.ConsenterInfo {
	var consentersInfo []config.ConsenterInfo
	for i := 0; i < num; i++ {
		consentersInfo = append(consentersInfo, createConsenterInfo(types.PartyID(i+1), nodes[i], ca))
	}
	return consentersInfo
}

func createConsenterInfo(partyID types.PartyID, n *node, ca tlsgen.CA) config.ConsenterInfo {
	return config.ConsenterInfo{
		PartyID:    partyID,
		Endpoint:   n.Address(),
		TLSCACerts: []config.RawBytes{ca.CertBytes()},
		PublicKey:  n.pk,
	}
}

func createBatchersInfo(num int, nodes []*node, ca tlsgen.CA) []config.BatcherInfo {
	var batchersInfo []config.BatcherInfo
	for i := 0; i < num; i++ {
		batchersInfo = append(batchersInfo, config.BatcherInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   nodes[i].Address(),
			TLSCert:    nodes[i].TLSCert,
			TLSCACerts: []config.RawBytes{ca.CertBytes()},
			PublicKey:  nodes[i].pk,
		})
	}
	return batchersInfo
}

type node struct {
	*comm.GRPCServer
	TLSCert []byte
	TLSKey  []byte
	sk      *ecdsa.PrivateKey
	pk      config.RawBytes
}

func createNodes(t *testing.T, ca tlsgen.CA, num int, addr string) []*node {
	var result []*node

	var sks []*ecdsa.PrivateKey
	var pks []config.RawBytes

	for i := 0; i < num; i++ {
		sk, rawPK := keygen(t)
		sks = append(sks, sk)
		pks = append(pks, pem.EncodeToMemory(&pem.Block{Bytes: rawPK, Type: "PUBLIC KEY"}))

	}

	for i := 0; i < num; i++ {
		kp, err := ca.NewServerCertKeyPair("127.0.0.1")
		require.NoError(t, err)

		srv, err := newGRPCServer(addr, ca, kp)
		require.NoError(t, err)

		result = append(result, &node{GRPCServer: srv, TLSKey: kp.Key, TLSCert: kp.Cert, pk: pks[i], sk: sks[i]})
	}
	return result
}

func newGRPCServer(addr string, ca tlsgen.CA, kp *tlsgen.CertKeyPair) (*comm.GRPCServer, error) {
	return comm.NewGRPCServer(addr, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     [][]byte{ca.CertBytes()},
			Key:               kp.Key,
			Certificate:       kp.Cert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     [][]byte{ca.CertBytes()},
		},
	})
}

func keygen(t *testing.T) (*ecdsa.PrivateKey, []byte) {
	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	rawPK, err := x509.MarshalPKIXPublicKey(&sk.PublicKey)
	require.NoError(t, err)
	return sk, rawPK
}

type storageListener struct {
	c chan *common.Block
}

func (l *storageListener) OnAppend(block *common.Block) {
	l.c <- block
}

// This test starts a cluster of 4 consensus nodes.
// Sending a couple of requests and waiting for 2 blocks to be written to the ledger.
// Then taking down the leader node (ID=1).
// The rest of the nodes get a new request and we make sure another block is committed (a view change occurs).
// Then we restart the stopped node, send 2 more requests, and we make sure the nodes commit 2 more blocks.
// Finally, we make sure the restarted node is synced with all blocks.
func TestCreateMultipleConsensusNodes(t *testing.T) {
	// grpclog.SetLoggerV2(&testutil.SilentLogger{})

	parties := 4

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	consenterNodes := createNodes(t, ca, parties, "127.0.0.1:0")

	consentersInfo := createConsentersInfo(parties, consenterNodes, ca)

	batcherNodes := createNodes(t, ca, parties, "127.0.0.1:0")

	batchersInfo := createBatchersInfo(parties, batcherNodes, ca)

	var consensusNodes []*consensus.Consensus
	var loggers []*zap.SugaredLogger
	var configs []config.ConsenterNodeConfig
	for i := 0; i < parties; i++ {
		partyID := types.PartyID(i + 1)

		logger := testutil.CreateLogger(t, int(partyID))
		loggers = append(loggers, logger)

		dir, err := os.MkdirTemp("", fmt.Sprintf("%s-consenter%d", t.Name(), i+1))
		require.NoError(t, err)

		defer os.RemoveAll(dir)

		conf := makeConf(dir, consenterNodes[i], types.PartyID(i+1), consentersInfo, batchersInfo)
		configs = append(configs, conf)

		c := consensus.CreateConsensus(conf, logger)
		grpcRegisterAndStart(c, consenterNodes[i])

		err = c.Start()
		require.NoError(t, err)

		consensusNodes = append(consensusNodes, c)
	}

	listener := &storageListener{c: make(chan *common.Block, 100)}
	consensusNodes[0].Storage.(*ledger.ConsensusLedger).RegisterAppendListener(listener)

	listener1 := &storageListener{c: make(chan *common.Block, 100)}
	consensusNodes[1].Storage.(*ledger.ConsensusLedger).RegisterAppendListener(listener1)

	baf123id1p1s1, err := batcher.CreateBAF(batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	assert.NoError(t, err)
	ce := &core.ControlEvent{BAF: baf123id1p1s1}

	err = consensusNodes[0].SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b := <-listener.c
	require.Equal(t, uint64(1), b.Header.Number)
	b1 := <-listener1.c
	require.Equal(t, uint64(1), b1.Header.Number)

	baf124id1p1s2, err := batcher.CreateBAF(batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf124id1p1s2}

	err = consensusNodes[0].SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b = <-listener.c
	require.Equal(t, uint64(2), b.Header.Number)
	b1 = <-listener1.c
	require.Equal(t, uint64(2), b1.Header.Number)

	consensusNodes[0].Stop()

	time.Sleep(1 * time.Second)

	baf125id1p1s1, err := batcher.CreateBAF(batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf125id1p1s1}

	err = consensusNodes[1].SubmitRequest(ce.Bytes())
	require.NoError(t, err)
	err = consensusNodes[2].SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b1 = <-listener1.c
	require.Equal(t, uint64(3), b1.Header.Number)

	consensusNodes[0] = consensus.CreateConsensus(configs[0], loggers[0])
	newConsenterNode := &node{TLSCert: consenterNodes[0].TLSCert, TLSKey: consenterNodes[0].TLSKey, sk: consenterNodes[0].sk, pk: consenterNodes[0].pk}
	newConsenterNode.GRPCServer, err = newGRPCServer(consenterNodes[0].Address(), ca, &tlsgen.CertKeyPair{Key: consenterNodes[0].TLSKey, Cert: consenterNodes[0].TLSCert})
	require.NoError(t, err)
	grpcRegisterAndStart(consensusNodes[0], newConsenterNode)

	newListener := &storageListener{c: make(chan *common.Block, 100)}
	consensusNodes[0].Storage.(*ledger.ConsensusLedger).RegisterAppendListener(newListener)

	err = consensusNodes[0].Start()
	require.NoError(t, err)

	time.Sleep(10 * time.Second)

	baf125id2p1s1, err := batcher.CreateBAF(batcherNodes[1].sk, 2, 1, digest125, 1, 3)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf125id2p1s1}

	err = consensusNodes[1].SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b1 = <-listener1.c
	require.Equal(t, uint64(4), b1.Header.Number)

	baf123id2p1s1, err := batcher.CreateBAF(batcherNodes[1].sk, 2, 1, digest123, 1, 1)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf123id2p1s1}

	err = consensusNodes[1].SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b1 = <-listener1.c
	require.Equal(t, uint64(5), b1.Header.Number)

	b = <-newListener.c
	require.Equal(t, uint64(3), b.Header.Number)
	b = <-newListener.c
	require.Equal(t, uint64(4), b.Header.Number)
	b = <-newListener.c
	require.Equal(t, uint64(5), b.Header.Number)

	for _, c := range consensusNodes {
		c.Stop()
	}
}
