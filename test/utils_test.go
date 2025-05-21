/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

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
	"testing"
	"time"

	config "github.ibm.com/decentralized-trust-research/arma/config"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/node/batcher"
	"github.ibm.com/decentralized-trust-research/arma/node/comm"
	"github.ibm.com/decentralized-trust-research/arma/node/comm/tlsgen"
	nodeconfig "github.ibm.com/decentralized-trust-research/arma/node/config"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus"
	protos "github.ibm.com/decentralized-trust-research/arma/node/protos/comm"
	"github.ibm.com/decentralized-trust-research/arma/node/router"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type node struct {
	*comm.GRPCServer
	TLSCert []byte
	TLSKey  []byte
	sk      *ecdsa.PrivateKey
	pk      nodeconfig.RawBytes
}

func (n *node) ToString() string {
	return fmt.Sprintf("GRPC.Address: %s", n.GRPCServer.Address())
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

func createRouters(t *testing.T, num int, batcherInfos []nodeconfig.BatcherInfo, ca tlsgen.CA, shardId types.ShardID) []*router.Router {
	var routers []*router.Router
	for i := 0; i < num; i++ {
		l := testutil.CreateLogger(t, i)
		kp, err := ca.NewServerCertKeyPair("127.0.0.1")
		require.NoError(t, err)

		config := &nodeconfig.RouterNodeConfig{
			ListenAddress:      "0.0.0.0:0",
			TLSPrivateKeyFile:  kp.Key,
			TLSCertificateFile: kp.Cert,
			PartyID:            types.PartyID(i + 1),
			Shards: []nodeconfig.ShardInfo{{
				ShardId:  shardId,
				Batchers: batcherInfos,
			}},
			UseTLS: true,
		}

		router := router.NewRouter(config, l)
		routers = append(routers, router)
	}

	return routers
}

func createConsenters(t *testing.T, num int, consenterNodes []*node, consenterInfos []nodeconfig.ConsenterInfo, shardInfo []nodeconfig.ShardInfo) ([]*consensus.Consensus, func()) {
	var consensuses []*consensus.Consensus
	var cleans []func()

	for i := 0; i < num; i++ {

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

		BFTConfig := config.DefaultArmaBFTConfig()
		BFTConfig.SelfID = uint64(partyID)

		conf := &nodeconfig.ConsenterNodeConfig{
			ListenAddress:      "0.0.0.0:0",
			Shards:             shardInfo,
			Consenters:         consenterInfos,
			PartyId:            partyID,
			TLSPrivateKeyFile:  consenterNodes[i].TLSKey,
			TLSCertificateFile: consenterNodes[i].TLSCert,
			SigningPrivateKey:  pem.EncodeToMemory(&pem.Block{Bytes: sk}),
			Directory:          dir,
			BFTConfig:          BFTConfig,
		}

		net := consenterNodes[i].GRPCServer
		c := consensus.CreateConsensus(conf, net, nil, logger)

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

func createBatchersForShard(t *testing.T, num int, batcherNodes []*node, shards []nodeconfig.ShardInfo, consenterInfos []nodeconfig.ConsenterInfo, shardID types.ShardID) ([]*batcher.Batcher, []*nodeconfig.BatcherNodeConfig, []*zap.SugaredLogger, func()) {
	var batchers []*batcher.Batcher
	var loggers []*zap.SugaredLogger
	var configs []*nodeconfig.BatcherNodeConfig

	for i := 0; i < num; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("%s-batcher%d", t.Name(), i+1))
		require.NoError(t, err)

		key, err := x509.MarshalPKCS8PrivateKey(batcherNodes[i].sk)
		require.NoError(t, err)

		batcherConf := &nodeconfig.BatcherNodeConfig{
			ListenAddress:         "0.0.0.0:0",
			Shards:                shards,
			ShardId:               shardID,
			PartyId:               types.PartyID(i + 1),
			Consenters:            consenterInfos,
			TLSPrivateKeyFile:     batcherNodes[i].TLSKey,
			TLSCertificateFile:    batcherNodes[i].TLSCert,
			SigningPrivateKey:     nodeconfig.RawBytes(pem.EncodeToMemory(&pem.Block{Bytes: key})),
			Directory:             dir,
			MemPoolMaxSize:        1000000,
			BatchMaxSize:          10000,
			BatchMaxBytes:         1024 * 1024 * 10,
			RequestMaxBytes:       1024 * 1024,
			SubmitTimeout:         time.Millisecond * 500,
			FirstStrikeThreshold:  time.Second * 10,
			SecondStrikeThreshold: time.Second * 10,
			AutoRemoveTimeout:     time.Second * 10,
			BatchCreationTimeout:  time.Millisecond * 500,
			BatchSequenceGap:      types.BatchSequence(10),
		}

		configs = append(configs, batcherConf)

		logger := testutil.CreateLogger(t, i+int(shardID)*10)
		loggers = append(loggers, logger)

		batcher := batcher.CreateBatcher(batcherConf, logger, batcherNodes[i], &batcher.ConsensusStateReplicatorFactory{}, &batcher.ConsenterControlEventSenderFactory{})
		batchers = append(batchers, batcher)
		batcher.Run()

		protos.RegisterRequestTransmitServer(batcherNodes[i].Server(), batcher)
		protos.RegisterBatcherControlServiceServer(batcherNodes[i].Server(), batcher)
		orderer.RegisterAtomicBroadcastServer(batcherNodes[i].Server(), batcher)

		go func() {
			err := batcherNodes[i].Start()
			if err != nil {
				panic(err)
			}
		}()

		t.Log("Batcher gRPC service listening on", batcherNodes[i].Address())
	}

	return batchers, configs, loggers, func() {
		for _, b := range batchers {
			b.Stop()
		}
	}
}

func createBatcherNodesAndInfo(t *testing.T, ca tlsgen.CA, num int) ([]*node, []nodeconfig.BatcherInfo) {
	nodes := createNodes(t, num, ca)

	var batchersInfo []nodeconfig.BatcherInfo
	for i := 0; i < num; i++ {
		batchersInfo = append(batchersInfo, nodeconfig.BatcherInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   nodes[i].Address(),
			TLSCert:    nodes[i].TLSCert,
			TLSCACerts: []nodeconfig.RawBytes{ca.CertBytes()},
			PublicKey:  nodes[i].pk,
		})
	}

	return nodes, batchersInfo
}

func createConsenterNodesAndInfo(t *testing.T, ca tlsgen.CA, num int) ([]*node, []nodeconfig.ConsenterInfo) {
	nodes := createNodes(t, num, ca)

	var consentersInfo []nodeconfig.ConsenterInfo
	for i := 0; i < num; i++ {
		consentersInfo = append(consentersInfo, nodeconfig.ConsenterInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   nodes[i].Address(),
			TLSCACerts: []nodeconfig.RawBytes{ca.CertBytes()},
			PublicKey:  nodes[i].pk,
		})
	}

	return nodes, consentersInfo
}

func createNodes(t *testing.T, num int, ca tlsgen.CA) []*node {
	var result []*node
	var sks []*ecdsa.PrivateKey
	var pks []nodeconfig.RawBytes

	for i := 0; i < num; i++ {
		sk, rawPK := keygen(t)
		sks = append(sks, sk)
		pks = append(pks, pem.EncodeToMemory(&pem.Block{Bytes: rawPK, Type: "PUBLIC KEY"}))
	}

	for i := 0; i < num; i++ {
		kp, err := ca.NewServerCertKeyPair("127.0.0.1")
		require.NoError(t, err)
		srv, err := newGRPCServer("127.0.0.1:0", ca, kp)
		require.NoError(t, err)

		result = append(result, &node{GRPCServer: srv, TLSKey: kp.Key, TLSCert: kp.Cert, pk: pks[i], sk: sks[i]})
	}

	return result
}

func recoverBatcher(t *testing.T, ca tlsgen.CA, logger *zap.SugaredLogger, conf *nodeconfig.BatcherNodeConfig, batcherNode *node) *batcher.Batcher {
	newBatcherNode := &node{
		TLSCert: batcherNode.TLSCert,
		TLSKey:  batcherNode.TLSKey,
		sk:      batcherNode.sk,
		pk:      batcherNode.pk,
	}
	var err error

	kp := &tlsgen.CertKeyPair{
		Key:  batcherNode.TLSKey,
		Cert: batcherNode.TLSCert,
	}

	newBatcherNode.GRPCServer, err = newGRPCServer(batcherNode.Address(), ca, kp)

	require.NoError(t, err)
	batcher := batcher.CreateBatcher(conf, logger, newBatcherNode, &batcher.ConsensusStateReplicatorFactory{}, &batcher.ConsenterControlEventSenderFactory{})
	batcher.Run()

	gRPCServer := newBatcherNode.Server()
	protos.RegisterRequestTransmitServer(gRPCServer, batcher)
	protos.RegisterBatcherControlServiceServer(gRPCServer, batcher)
	orderer.RegisterAtomicBroadcastServer(gRPCServer, batcher)

	go func() {
		err := newBatcherNode.Start()
		if err != nil {
			panic(err)
		}
	}()

	return batcher
}

func sendTxn(workerID int, txnNum int, routers []*router.Router) {
	txn := make([]byte, 32)
	binary.BigEndian.PutUint64(txn, uint64(txnNum))
	binary.BigEndian.PutUint16(txn[30:], uint16(workerID))

	for routerId := 0; routerId < 4; routerId++ {
		routers[routerId].Submit(context.Background(), &protos.Request{Payload: txn})
	}
}
