/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
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

func createBatchers(t *testing.T, num int, shardID types.ShardID, batcherNodes []*node, batchersInfo []config.BatcherInfo, consentersInfo []config.ConsenterInfo, stubConsenters []*stubConsenter) ([]*batcher.Batcher, []*zap.SugaredLogger, []*config.BatcherNodeConfig, func()) {
	var batchers []*batcher.Batcher
	var loggers []*zap.SugaredLogger
	var configs []*config.BatcherNodeConfig

	var parties []types.PartyID
	for i := 0; i < num; i++ {
		parties = append(parties, types.PartyID(i+1))
	}

	for i := 0; i < num; i++ {
		logger := testutil.CreateLogger(t, int(parties[i]))
		loggers = append(loggers, logger)

		key, err := x509.MarshalPKCS8PrivateKey(batcherNodes[i].sk)
		require.NoError(t, err)
		signer := crypto.ECDSASigner(*batcherNodes[i].sk)

		bundle := &configMocks.FakeConfigResources{}
		configtxValidator := &policyMocks.FakeConfigtxValidator{}
		configtxValidator.ChannelIDReturns("arma")
		bundle.ConfigtxValidatorReturns(configtxValidator)

		conf := &config.BatcherNodeConfig{
			Shards:                              []config.ShardInfo{{ShardId: shardID, Batchers: batchersInfo}},
			Consenters:                          consentersInfo,
			Directory:                           t.TempDir(),
			ConfigStorePath:                     t.TempDir(),
			PartyId:                             parties[i],
			ShardId:                             shardID,
			SigningPrivateKey:                   config.RawBytes(pem.EncodeToMemory(&pem.Block{Bytes: key})),
			TLSPrivateKeyFile:                   batcherNodes[i].TLSKey,
			TLSCertificateFile:                  batcherNodes[i].TLSCert,
			MemPoolMaxSize:                      1000000,
			BatchMaxSize:                        10000,
			BatchMaxBytes:                       1024 * 1024 * 10,
			RequestMaxBytes:                     1024 * 1024,
			SubmitTimeout:                       time.Millisecond * 500,
			FirstStrikeThreshold:                10 * time.Second,
			SecondStrikeThreshold:               10 * time.Second,
			AutoRemoveTimeout:                   10 * time.Second,
			BatchCreationTimeout:                time.Millisecond * 500,
			BatchSequenceGap:                    types.BatchSequence(10),
			ClientSignatureVerificationRequired: false,
			Bundle:                              bundle,
		}
		configs = append(configs, conf)

		batcher := batcher.CreateBatcher(conf, logger, batcherNodes[i], stubConsenters[i], &batcher.ConsenterControlEventSenderFactory{}, signer)
		batchers = append(batchers, batcher)
		batcher.Run()

		grpcRegisterAndStart(batcher, batcherNodes[i])
	}

	return batchers, loggers, configs, func() {
		for _, b := range batchers {
			b.Stop()
		}
	}
}

func createConsenterStubs(t *testing.T, consenterNodes []*node, num int) ([]*stubConsenter, func()) {
	var stubConsenters []*stubConsenter

	for i := 0; i < num; i++ {
		cs := NewStubConsenter(t, types.PartyID(i+1), consenterNodes[i])
		stubConsenters = append(stubConsenters, cs)
	}

	return stubConsenters, func() {
		for _, sc := range stubConsenters {
			sc.StopNet()
		}
	}
}

func recoverBatcher(t *testing.T, ca tlsgen.CA, logger *zap.SugaredLogger, conf *config.BatcherNodeConfig, batcherNode *node, sc *stubConsenter) *batcher.Batcher {
	newBatcherNode := &node{
		TLSCert: batcherNode.TLSCert,
		TLSKey:  batcherNode.TLSKey,
		sk:      batcherNode.sk,
		pk:      batcherNode.pk,
	}
	var err error
	newBatcherNode.GRPCServer, err = newGRPCServer(batcherNode.Address(), ca, &tlsgen.CertKeyPair{
		Key:  batcherNode.TLSKey,
		Cert: batcherNode.TLSCert,
	})
	require.NoError(t, err)

	signer := crypto.ECDSASigner(*newBatcherNode.sk)

	batcher := batcher.CreateBatcher(conf, logger, newBatcherNode, sc, &batcher.ConsenterControlEventSenderFactory{}, signer)
	batcher.Run()

	grpcRegisterAndStart(batcher, newBatcherNode)

	return batcher
}

func grpcRegisterAndStart(b *batcher.Batcher, n *node) {
	gRPCServer := n.Server()

	protos.RegisterRequestTransmitServer(gRPCServer, b)
	protos.RegisterBatcherControlServiceServer(gRPCServer, b)
	orderer.RegisterAtomicBroadcastServer(gRPCServer, b)

	go func() {
		err := n.Start()
		if err != nil {
			panic(err)
		}
	}()
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
