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

	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/grpclog"
)

func TestBatcherRun(t *testing.T) {
	grpclog.SetLoggerV2(&testutil.SilentLogger{})

	shardID := types.ShardID(0)
	numParties := 4
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	batcherNodes := createNodes(t, ca, numParties, "127.0.0.1:0")
	batchersInfo := createBatchersInfo(numParties, batcherNodes, ca)
	consenterNodes := createNodes(t, ca, numParties, "127.0.0.1:0")
	consentersInfo := createConsentersInfo(numParties, consenterNodes, ca)

	batchers, ledgers, stateChannels, _ := createBatchers(t, numParties, shardID, batcherNodes, batchersInfo, consentersInfo)

	defer func() {
		for i := 0; i < numParties; i++ {
			batchers[i].Stop()
		}
	}()

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	batchers[0].Submit(context.Background(), &protos.Request{Payload: req})

	require.Eventually(t, func() bool {
		return ledgers[0].Height(1) == uint64(1) && ledgers[1].Height(1) == uint64(1)
	}, 10*time.Second, 10*time.Millisecond)

	req2 := make([]byte, 8)
	binary.BigEndian.PutUint64(req2, uint64(2))
	batchers[0].Submit(context.Background(), &protos.Request{Payload: req2})

	require.Eventually(t, func() bool {
		return ledgers[2].Height(1) == uint64(2) && ledgers[3].Height(1) == uint64(2)
	}, 10*time.Second, 10*time.Millisecond)

	require.Equal(t, types.PartyID(1), batchers[0].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())
	stateChannels[0] <- &core.State{N: uint16(numParties), Shards: []core.ShardTerm{{Shard: shardID, Term: 0}}}
	require.Equal(t, types.PartyID(1), batchers[0].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())

	for i := 0; i < numParties; i++ {
		stateChannels[i] <- &core.State{N: uint16(numParties), Shards: []core.ShardTerm{{Shard: shardID, Term: 1}}}
	}
	require.Eventually(t, func() bool {
		return batchers[0].GetPrimaryID() == types.PartyID(2) && batchers[3].GetPrimaryID() == types.PartyID(2)
	}, 10*time.Second, 10*time.Millisecond)

	require.Equal(t, uint64(0), ledgers[0].Height(2))
	require.Equal(t, uint64(0), ledgers[1].Height(2))

	req3 := make([]byte, 8)
	binary.BigEndian.PutUint64(req3, uint64(3))
	batchers[1].Submit(context.Background(), &protos.Request{Payload: req3})

	require.Eventually(t, func() bool {
		return ledgers[0].Height(2) == uint64(1) && ledgers[1].Height(2) == uint64(1)
	}, 10*time.Second, 10*time.Millisecond)
}

func createBatchers(t *testing.T, num int, shardID types.ShardID, batcherNodes []*node, batchersInfo []config.BatcherInfo, consentersInfo []config.ConsenterInfo) ([]*batcher.Batcher, []*ledger.BatchLedgerArray, []chan *core.State, []*zap.SugaredLogger) {
	var batchers []*batcher.Batcher
	var ledgers []*ledger.BatchLedgerArray
	var stateChannels []chan *core.State
	var loggers []*zap.SugaredLogger

	var parties []types.PartyID
	for i := 0; i < num; i++ {
		parties = append(parties, types.PartyID(i+1))
	}

	for i := 0; i < num; i++ {
		logger := testutil.CreateLogger(t, int(parties[i]))
		loggers = append(loggers, logger)

		key, err := x509.MarshalPKCS8PrivateKey(batcherNodes[i].sk)
		require.NoError(t, err)
		conf := config.BatcherNodeConfig{
			Shards:             []config.ShardInfo{{ShardId: shardID, Batchers: batchersInfo}},
			Consenters:         consentersInfo,
			Directory:          t.TempDir(),
			PartyId:            parties[i],
			ShardId:            shardID,
			SigningPrivateKey:  config.RawBytes(pem.EncodeToMemory(&pem.Block{Bytes: key})),
			TLSPrivateKeyFile:  batcherNodes[i].TLSKey,
			TLSCertificateFile: batcherNodes[i].TLSCert,
		}
		ledger, err := ledger.NewBatchLedgerArray(conf.ShardId, conf.PartyId, parties, conf.Directory, logger)
		require.NoError(t, err)
		require.Equal(t, uint64(0), ledger.Height(conf.PartyId))
		ledgers = append(ledgers, ledger)

		deliveryService := &batcher.BatcherDeliverService{
			LedgerArray: ledger,
			Logger:      logger,
		}

		bp := batcher.NewBatchPuller(conf, ledger, logger)

		sr := &mocks.FakeStateReplicator{}
		stateChan := make(chan *core.State)
		sr.ReplicateStateReturns(stateChan)
		stateChannels = append(stateChannels, stateChan)

		batcher := batcher.NewBatcher(logger, conf, ledger, bp, deliveryService, sr, batcherNodes[i])
		batchers = append(batchers, batcher)

		batcher.Run()

		require.Eventually(t, func() bool {
			return sr.ReplicateStateCallCount() == 1
		}, 10*time.Second, 10*time.Millisecond)

	}

	for i := 0; i < num; i++ {
		grpcRegisterAndStart(batchers[i], batcherNodes[i])
	}

	return batchers, ledgers, stateChannels, loggers
}

func grpcRegisterAndStart(b *batcher.Batcher, n *node) {
	gRPCServer := n.Server()

	protos.RegisterRequestTransmitServer(gRPCServer, b)
	protos.RegisterAckServiceServer(gRPCServer, b)
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
