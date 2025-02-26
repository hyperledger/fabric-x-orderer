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

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/batcher"
	"github.ibm.com/decentralized-trust-research/arma/node/batcher/mocks"
	"github.ibm.com/decentralized-trust-research/arma/node/comm"
	"github.ibm.com/decentralized-trust-research/arma/node/comm/tlsgen"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	protos "github.ibm.com/decentralized-trust-research/arma/node/protos/comm"
	"github.ibm.com/decentralized-trust-research/arma/testutil"

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

	batchers, stateChannels, srs, eventSenders, loggers, configs := createBatchers(t, numParties, shardID, batcherNodes, batchersInfo, consentersInfo)

	defer func() {
		for i := 0; i < numParties; i++ {
			batchers[i].Stop()
		}
	}()

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	batchers[0].Submit(context.Background(), &protos.Request{Payload: req})

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(1) && batchers[1].Ledger.Height(1) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return eventSenders[0].SendControlEventCallCount() == 1*numParties && eventSenders[1].SendControlEventCallCount() == 1*numParties
	}, 30*time.Second, 10*time.Millisecond)

	require.Equal(t, types.PartyID(1), eventSenders[0].SendControlEventArgsForCall(1).BAF.Primary())
	require.Equal(t, types.BatchSequence(0), eventSenders[0].SendControlEventArgsForCall(1).BAF.Seq())

	req2 := make([]byte, 8)
	binary.BigEndian.PutUint64(req2, uint64(2))
	batchers[0].Submit(context.Background(), &protos.Request{Payload: req2})

	require.Eventually(t, func() bool {
		return batchers[2].Ledger.Height(1) == uint64(2) && batchers[3].Ledger.Height(1) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return eventSenders[2].SendControlEventCallCount() == 2*numParties && eventSenders[3].SendControlEventCallCount() == 2*numParties
	}, 30*time.Second, 10*time.Millisecond)

	require.Equal(t, types.PartyID(1), eventSenders[2].SendControlEventArgsForCall(5).BAF.Primary())
	require.Equal(t, types.BatchSequence(1), eventSenders[2].SendControlEventArgsForCall(5).BAF.Seq())

	require.Equal(t, types.PartyID(1), batchers[0].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())
	stateChannels[0] <- &core.State{N: uint16(numParties), Shards: []core.ShardTerm{{Shard: shardID, Term: 0}}}
	require.Equal(t, types.PartyID(1), batchers[0].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())

	termChangeState := &core.State{N: uint16(numParties), Shards: []core.ShardTerm{{Shard: shardID, Term: 1}}}

	for i := 0; i < numParties; i++ {
		stateChannels[i] <- termChangeState
	}
	require.Eventually(t, func() bool {
		return batchers[0].GetPrimaryID() == types.PartyID(2) && batchers[3].GetPrimaryID() == types.PartyID(2)
	}, 30*time.Second, 10*time.Millisecond)

	require.Equal(t, uint64(0), batchers[0].Ledger.Height(2))
	require.Equal(t, uint64(0), batchers[1].Ledger.Height(2))

	// stop and recover secondary
	batchers[3].Stop()
	batchers[3] = recoverBatcher(t, ca, loggers[3], configs[3], srs[3], eventSenders[3], batcherNodes[3], stateChannels[3], termChangeState)

	req3 := make([]byte, 8)
	binary.BigEndian.PutUint64(req3, uint64(3))
	batchers[1].Submit(context.Background(), &protos.Request{Payload: req3})

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(2) == uint64(1) && batchers[1].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return batchers[3].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return eventSenders[0].SendControlEventCallCount() == 3*numParties && eventSenders[3].SendControlEventCallCount() == 3*numParties
	}, 30*time.Second, 10*time.Millisecond)

	for i := 0; i < numParties; i++ {
		require.Equal(t, types.PartyID(2), batchers[i].GetPrimaryID())
	}

	// stop and recover primary
	batchers[1].Stop()
	batchers[1] = recoverBatcher(t, ca, loggers[1], configs[1], srs[1], eventSenders[1], batcherNodes[1], stateChannels[1], termChangeState)

	req4 := make([]byte, 8)
	binary.BigEndian.PutUint64(req4, uint64(4))
	batchers[1].Submit(context.Background(), &protos.Request{Payload: req4})

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(2) == uint64(2) && batchers[1].Ledger.Height(2) == uint64(2) && batchers[3].Ledger.Height(2) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return eventSenders[0].SendControlEventCallCount() == 4*numParties && eventSenders[1].SendControlEventCallCount() == 4*numParties
	}, 30*time.Second, 10*time.Millisecond)

	for i := 0; i < numParties; i++ {
		require.Equal(t, types.PartyID(2), batchers[i].GetPrimaryID())
	}

	// stop secondary and recover after a batch
	batchers[2].Stop()

	req5 := make([]byte, 8)
	binary.BigEndian.PutUint64(req5, uint64(5))
	batchers[1].Submit(context.Background(), &protos.Request{Payload: req5})

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(2) == uint64(3) && batchers[1].Ledger.Height(2) == uint64(3) && batchers[3].Ledger.Height(2) == uint64(3)
	}, 30*time.Second, 10*time.Millisecond)

	// now recover the secondary
	batchers[2] = recoverBatcher(t, ca, loggers[2], configs[2], srs[2], eventSenders[2], batcherNodes[2], stateChannels[2], termChangeState)
	require.Eventually(t, func() bool {
		return batchers[2].Ledger.Height(2) == uint64(3)
	}, 30*time.Second, 10*time.Millisecond)

	for i := 0; i < numParties; i++ {
		require.Equal(t, types.PartyID(2), batchers[i].GetPrimaryID())
	}

	// stop primary, change term, and recover after a batch
	batchers[1].Stop()

	termChangeAgainState := &core.State{N: uint16(numParties), Shards: []core.ShardTerm{{Shard: shardID, Term: 2}}}

	for i := 0; i < numParties; i++ {
		if i != 1 {
			stateChannels[i] <- termChangeAgainState
		}
	}
	require.Eventually(t, func() bool {
		return batchers[0].GetPrimaryID() == types.PartyID(3) && batchers[3].GetPrimaryID() == types.PartyID(3)
	}, 30*time.Second, 10*time.Millisecond)

	require.Equal(t, uint64(0), batchers[0].Ledger.Height(3))
	require.Equal(t, uint64(0), batchers[2].Ledger.Height(3))
	require.Equal(t, uint64(0), batchers[3].Ledger.Height(3))

	req6 := make([]byte, 8)
	binary.BigEndian.PutUint64(req6, uint64(6))
	batchers[2].Submit(context.Background(), &protos.Request{Payload: req6})

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(3) == uint64(1) && batchers[2].Ledger.Height(3) == uint64(1) && batchers[3].Ledger.Height(3) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	// now recover the previous primary
	batchers[1] = recoverBatcher(t, ca, loggers[1], configs[1], srs[1], eventSenders[1], batcherNodes[1], stateChannels[1], termChangeAgainState)
	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(3) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)
}

func TestBatcherComplain(t *testing.T) {
	shardID := types.ShardID(0)
	numParties := 4
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	batcherNodes := createNodes(t, ca, numParties, "127.0.0.1:0")
	batchersInfo := createBatchersInfo(numParties, batcherNodes, ca)
	consenterNodes := createNodes(t, ca, numParties, "127.0.0.1:0")
	consentersInfo := createConsentersInfo(numParties, consenterNodes, ca)

	batchers, stateChannels, srs, eventSenders, loggers, configs := createBatchers(t, numParties, shardID, batcherNodes, batchersInfo, consentersInfo)

	defer func() {
		for i := 0; i < numParties; i++ {
			batchers[i].Stop()
		}
	}()

	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	batchers[0].Submit(context.Background(), &protos.Request{Payload: req})

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(1) && batchers[1].Ledger.Height(1) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return eventSenders[1].SendControlEventCallCount() == 1*numParties && eventSenders[2].SendControlEventCallCount() == 1*numParties
	}, 30*time.Second, 10*time.Millisecond)

	require.Equal(t, types.PartyID(1), eventSenders[1].SendControlEventArgsForCall(1).BAF.Primary())
	require.Equal(t, types.BatchSequence(0), eventSenders[1].SendControlEventArgsForCall(1).BAF.Seq())

	require.Equal(t, types.PartyID(1), batchers[1].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())

	// stop the primary
	batchers[0].Stop()

	// submit request to other batchers
	req2 := make([]byte, 8)
	binary.BigEndian.PutUint64(req2, uint64(2))
	batchers[1].Submit(context.Background(), &protos.Request{Payload: req2})
	batchers[2].Submit(context.Background(), &protos.Request{Payload: req2})

	// wait for complaints
	require.Eventually(t, func() bool {
		return eventSenders[1].SendControlEventCallCount() == 2*numParties && eventSenders[2].SendControlEventCallCount() == 2*numParties
	}, 60*time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(0), eventSenders[1].SendControlEventArgsForCall(5).Complaint.Term)
	require.Equal(t, uint64(0), eventSenders[2].SendControlEventArgsForCall(5).Complaint.Term)

	// change term
	termChangeState := &core.State{N: uint16(numParties), Shards: []core.ShardTerm{{Shard: shardID, Term: 1}}}
	stateChannels[1] <- termChangeState
	stateChannels[2] <- termChangeState
	stateChannels[3] <- termChangeState

	require.Eventually(t, func() bool {
		return batchers[1].GetPrimaryID() == types.PartyID(2) && batchers[2].GetPrimaryID() == types.PartyID(2)
	}, 30*time.Second, 10*time.Millisecond)

	// submit another request to new primary
	req3 := make([]byte, 8)
	binary.BigEndian.PutUint64(req3, uint64(3))
	batchers[1].Submit(context.Background(), &protos.Request{Payload: req3})

	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(2) == uint64(1) && batchers[2].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	// now recover old primary
	batchers[0] = recoverBatcher(t, ca, loggers[0], configs[0], srs[0], eventSenders[0], batcherNodes[0], stateChannels[0], termChangeState)
	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	for i := 0; i < numParties; i++ {
		require.Equal(t, types.PartyID(2), batchers[i].GetPrimaryID())
	}
}

func recoverBatcher(t *testing.T, ca tlsgen.CA, logger *zap.SugaredLogger, conf config.BatcherNodeConfig, sr batcher.StateReplicator, eventSender batcher.ConsenterControlEventSender, batcherNode *node, stateChan chan *core.State, latestState *core.State) *batcher.Batcher {
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

	csrc := &mocks.FakeConsensusStateReplicatorCreator{}
	csrc.CreateStateConsensusReplicatorReturns(sr)
	eventSenderCreator := &mocks.FakeConsenterControlEventSenderCreator{}
	eventSenderCreator.CreateConsenterControlEventSenderReturns(eventSender)

	batcher := batcher.CreateBatcher(conf, logger, newBatcherNode, csrc, eventSenderCreator)
	batcher.Run()
	stateChan <- latestState
	grpcRegisterAndStart(batcher, newBatcherNode)
	return batcher
}

func createBatchers(t *testing.T, num int, shardID types.ShardID, batcherNodes []*node, batchersInfo []config.BatcherInfo, consentersInfo []config.ConsenterInfo) ([]*batcher.Batcher, []chan *core.State, []batcher.StateReplicator, []*mocks.FakeConsenterControlEventSender, []*zap.SugaredLogger, []config.BatcherNodeConfig) {
	var batchers []*batcher.Batcher
	var stateChannels []chan *core.State
	var loggers []*zap.SugaredLogger
	var configs []config.BatcherNodeConfig
	var srs []batcher.StateReplicator
	var eventSenders []*mocks.FakeConsenterControlEventSender

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
		configs = append(configs, conf)

		stateChan := make(chan *core.State)
		sr := &mocks.FakeStateReplicator{}
		sr.ReplicateStateReturns(stateChan)
		csrc := &mocks.FakeConsensusStateReplicatorCreator{}
		csrc.CreateStateConsensusReplicatorReturns(sr)
		stateChannels = append(stateChannels, stateChan)
		srs = append(srs, sr)

		eventSender := &mocks.FakeConsenterControlEventSender{}
		eventSenderCreator := &mocks.FakeConsenterControlEventSenderCreator{}
		eventSenderCreator.CreateConsenterControlEventSenderReturns(eventSender)
		eventSenders = append(eventSenders, eventSender)

		batcher := batcher.CreateBatcher(conf, logger, batcherNodes[i], csrc, eventSenderCreator)
		batchers = append(batchers, batcher)

		batcher.Run()

		require.Eventually(t, func() bool {
			return sr.ReplicateStateCallCount() == 1
		}, 10*time.Second, 10*time.Millisecond)

		grpcRegisterAndStart(batcher, batcherNodes[i])
	}

	return batchers, stateChannels, srs, eventSenders, loggers, configs
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
