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

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/batcher"
	"github.ibm.com/decentralized-trust-research/arma/node/comm"
	"github.ibm.com/decentralized-trust-research/arma/node/comm/tlsgen"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus"
	"github.ibm.com/decentralized-trust-research/arma/node/ledger"
	protos "github.ibm.com/decentralized-trust-research/arma/node/protos/comm"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type node struct {
	*comm.GRPCServer
	TLSCert []byte
	TLSKey  []byte
	sk      *ecdsa.PrivateKey
	pk      config.RawBytes
}

type storageListener struct {
	c chan *common.Block
}

func (l *storageListener) OnAppend(block *common.Block) {
	l.c <- block
}

type consensusTestSetup struct {
	consenterNodes []*node
	consentersInfo []config.ConsenterInfo
	batcherNodes   []*node
	batchersInfo   []config.BatcherInfo
	consensusNodes []*consensus.Consensus
	loggers        []*zap.SugaredLogger
	configs        []config.ConsenterNodeConfig
	listeners      []*storageListener
}

func keygen(t *testing.T) (*ecdsa.PrivateKey, []byte) {
	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	rawPK, err := x509.MarshalPKIXPublicKey(&sk.PublicKey)
	require.NoError(t, err)
	return sk, rawPK
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

func setupConsensusTest(t *testing.T, ca tlsgen.CA, numParties int) consensusTestSetup {
	consenterNodes := createNodes(t, ca, numParties, "127.0.0.1:0")
	consentersInfo := createConsentersInfo(numParties, consenterNodes, ca)
	batcherNodes := createNodes(t, ca, numParties, "127.0.0.1:0")
	batchersInfo := createBatchersInfo(numParties, batcherNodes, ca)

	var consensusNodes []*consensus.Consensus
	var loggers []*zap.SugaredLogger
	var configs []config.ConsenterNodeConfig
	var listeners []*storageListener

	for i := 0; i < numParties; i++ {
		partyID := types.PartyID(i + 1)
		logger := testutil.CreateLogger(t, int(partyID))
		loggers = append(loggers, logger)

		dir, err := os.MkdirTemp("", fmt.Sprintf("%s-consenter%d", t.Name(), i+1))
		require.NoError(t, err)

		conf := makeConf(dir, consenterNodes[i], partyID, consentersInfo, batchersInfo)
		configs = append(configs, conf)

		c := consensus.CreateConsensus(conf, nil, logger)
		grpcRegisterAndStart(c, consenterNodes[i])

		listener := &storageListener{c: make(chan *common.Block, 100)}
		c.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(listener)
		listeners = append(listeners, listener)

		err = c.Start()
		require.NoError(t, err)

		consensusNodes = append(consensusNodes, c)
	}

	return consensusTestSetup{
		consenterNodes: consenterNodes,
		consentersInfo: consentersInfo,
		batcherNodes:   batcherNodes,
		batchersInfo:   batchersInfo,
		consensusNodes: consensusNodes,
		loggers:        loggers,
		configs:        configs,
		listeners:      listeners,
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

func recoverNode(t *testing.T, setup consensusTestSetup, nodeIndex int, ca tlsgen.CA) error {
	setup.consensusNodes[nodeIndex] = consensus.CreateConsensus(setup.configs[nodeIndex], nil, setup.loggers[nodeIndex])

	newConsenterNode := &node{
		TLSCert: setup.consenterNodes[nodeIndex].TLSCert,
		TLSKey:  setup.consenterNodes[nodeIndex].TLSKey,
		sk:      setup.consenterNodes[nodeIndex].sk,
		pk:      setup.consenterNodes[nodeIndex].pk,
	}
	var err error
	newConsenterNode.GRPCServer, err = newGRPCServer(setup.consenterNodes[nodeIndex].Address(), ca, &tlsgen.CertKeyPair{
		Key:  setup.consenterNodes[nodeIndex].TLSKey,
		Cert: setup.consenterNodes[nodeIndex].TLSCert,
	})
	require.NoError(t, err)

	grpcRegisterAndStart(setup.consensusNodes[nodeIndex], newConsenterNode)

	newListener := &storageListener{c: make(chan *common.Block, 100)}
	setup.consensusNodes[nodeIndex].Storage.(*ledger.ConsensusLedger).RegisterAppendListener(newListener)
	setup.listeners[nodeIndex] = newListener

	err = setup.consensusNodes[nodeIndex].Start()
	require.NoError(t, err)

	return nil
}

// helper function to create and submit a request for testing
func createAndSubmitRequest(node *consensus.Consensus, sk *ecdsa.PrivateKey, id types.PartyID, shard types.ShardID, digest []byte, primary types.PartyID, sequence types.BatchSequence) error {
	baf, err := batcher.CreateBAF(sk, id, shard, digest, primary, sequence)
	if err != nil {
		return err
	}

	controlEvent := &core.ControlEvent{BAF: baf}
	return node.SubmitRequest(controlEvent.Bytes())
}
