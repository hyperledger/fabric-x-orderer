package batcher_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"testing"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/node/batcher"
	"arma/node/batcher/mocks"
	"arma/node/comm"
	"arma/node/comm/tlsgen"
	"arma/node/config"
	"arma/node/ledger"
	protos "arma/node/protos/comm"
	"arma/testutil"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/grpclog"
)

func TestBatcherRun(t *testing.T) {
	grpclog.SetLoggerV2(&testutil.SilentLogger{})

	partyID := types.PartyID(1)
	shardID := types.ShardID(0)
	numParties := 4
	logger := testutil.CreateLogger(t, int(partyID))
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	batcherNodes := createNodes(t, ca, numParties, "127.0.0.1:0")
	batchersInfo := createBatchersInfo(numParties, batcherNodes, ca)
	consenterNodes := createNodes(t, ca, numParties, "127.0.0.1:0")
	consentersInfo := createConsentersInfo(numParties, consenterNodes, ca)

	key, err := x509.MarshalPKCS8PrivateKey(batcherNodes[0].sk)
	require.NoError(t, err)
	conf := config.BatcherNodeConfig{
		Shards:             []config.ShardInfo{{ShardId: shardID, Batchers: batchersInfo}},
		Consenters:         consentersInfo,
		Directory:          t.TempDir(),
		PartyId:            types.PartyID(partyID),
		ShardId:            types.ShardID(shardID),
		SigningPrivateKey:  config.RawBytes(pem.EncodeToMemory(&pem.Block{Bytes: key})),
		TLSPrivateKeyFile:  batcherNodes[0].TLSKey,
		TLSCertificateFile: batcherNodes[0].TLSCert,
	}
	ledger, err := ledger.NewBatchLedgerArray(conf.ShardId, conf.PartyId, []types.PartyID{conf.PartyId, conf.PartyId + 1}, conf.Directory, logger)
	require.NoError(t, err)
	require.Equal(t, uint64(0), ledger.Height(partyID))
	deliveryService := &batcher.BatcherDeliverService{
		LedgerArray: ledger,
		Logger:      logger,
	}

	bp := batcher.NewBatchPuller(conf, ledger, logger)

	sr := &mocks.FakeStateReplicator{}

	batcher := batcher.NewBatcher(logger, conf, ledger, bp, deliveryService, sr)

	stateChan := make(chan *core.State)
	sr.ReplicateStateReturns(stateChan)

	batcher.Run()

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	batcher.Submit(context.Background(), &protos.Request{Payload: req})

	require.Eventually(t, func() bool {
		return ledger.Height(partyID) == uint64(1)
	}, 10*time.Second, 10*time.Millisecond)

	require.Equal(t, partyID, batcher.GetPrimaryID())
	stateChan <- &core.State{N: 2, Shards: []core.ShardTerm{{Shard: shardID, Term: 0}}}
	require.Equal(t, partyID, batcher.GetPrimaryID())

	stateChan <- &core.State{N: 2, Shards: []core.ShardTerm{{Shard: shardID, Term: 1}}}
	require.Eventually(t, func() bool {
		return batcher.GetPrimaryID() == partyID+1
	}, 10*time.Second, 10*time.Millisecond)

	batcher.Stop()

	require.Equal(t, sr.ReplicateStateCallCount(), 1)
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
