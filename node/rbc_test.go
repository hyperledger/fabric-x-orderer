package node

import (
	arma "arma/pkg"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"github.com/SmartBFT-Go/consensus/v2/pkg/types"
	"github.com/SmartBFT-Go/consensus/v2/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm/tlsgen"
	protos "github.ibm.com/Yacov-Manevich/ARMA/node/protos/comm"
	"os"
	"sync"
	"testing"
	"time"
)

type mockSecondaryBatchPuller chan arma.Batch

func (m mockSecondaryBatchPuller) PullBatches(arma.PartyID) <-chan arma.Batch {
	return m
}

type mockBatcherLedger struct {
	secondaries []mockSecondaryBatchPuller
	storage     chan []byte
}

func (l *mockBatcherLedger) Append(_ arma.PartyID, _ uint64, bytes []byte) {
	batch := mockBatchedRequests(arma.BatchFromRaw(bytes))
	for _, bp := range l.secondaries {
		bp <- batch
	}

	l.storage <- bytes
}

type mockBatchedRequests arma.BatchedRequests

func (mbr mockBatchedRequests) Digest() []byte {
	br := arma.BatchedRequests(mbr)
	dig := sha256.Sum256(br.ToBytes())
	return dig[:]
}

func (mbr mockBatchedRequests) Requests() arma.BatchedRequests {
	return arma.BatchedRequests(mbr)
}

func (mbr mockBatchedRequests) Party() arma.PartyID {
	return 0
}

type mockConsenterLedger chan []byte

func (l mockConsenterLedger) Append(bytes []byte) {
	l <- bytes
}

type mockBFT struct {
	consenters []*Consensus
	batches    chan []byte
	once       sync.Once
}

func (m *mockBFT) SubmitRequest(req []byte) error {
	m.batches <- req
	return nil
}

func (m *mockBFT) Start() error {
	m.once.Do(func() {
		m.batches = make(chan []byte, 1000)
		go m.batchRequestsAndDeliver()
	})
	return nil
}

func (m *mockBFT) batchRequestsAndDeliver() {
	ticker := time.NewTicker(time.Millisecond * 100)
	for range ticker.C {
		var batch [][]byte
		pendingRequests := true
		for pendingRequests {
			select {
			case req := <-m.batches:
				batch = append(batch, req)
			default:
				pendingRequests = false
			}
		}
		if len(batch) == 0 {
			continue
		}

		batch = nil
		proposal := m.consenters[0].AssembleProposal(nil, batch)
		for _, c := range m.consenters {
			c.Deliver(proposal, nil)
		}
	}
}

func (m *mockBFT) HandleMessage(targetID uint64, _ *smartbftprotos.Message) {

}

func (m *mockBFT) HandleRequest(targetID uint64, request []byte) {

}

func TestRBC(t *testing.T) {
	ca, err := tlsgen.NewCA()
	assert.NoError(t, err)

	batcherInfos, consenterInfos, batcherNodes, consenterNodes := createConsentersAndBatchers(t, ca)

	var batcherLedgers []arma.BatchLedger

	for i := 0; i < 4; i++ {
		l := &mockBatcherLedger{storage: make(chan []byte, 1)}
		batcherLedgers = append(batcherLedgers, l)
	}

	shards := []ShardInfo{{ShardId: 1, Batchers: batcherInfos}}

	batchers := createBatchers(t, batcherNodes, shards, consenterInfos, batcherLedgers)

	routers := createRouters(t, batcherInfos, ca)

	var consenterLedgers []Storage
	for i := 0; i < 4; i++ {
		l := make(mockConsenterLedger, 1)
		consenterLedgers = append(consenterLedgers, l)
	}

	_, clean := createConsenters(t, consenterNodes, consenterInfos, consenterLedgers)
	defer clean()

	for _, b := range batchers {
		go b.b.Run()
	}

	for i := 0; i < 4; i++ {
		routers[i].Submit(context.Background(), &protos.Request{Payload: []byte{1, 2, 3}})
	}

	for _, l := range batcherLedgers {
		raw := <-l.(*mockBatcherLedger).storage
		rawReq := arma.BatchFromRaw(raw)[0]
		req := &protos.Request{}
		err = proto.Unmarshal(rawReq, req)
		assert.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3}, req.Payload)
	}
}

func createRouters(t *testing.T, batcherInfos []BatcherInfo, ca tlsgen.CA) []*Router {
	var routers []*Router
	for i := 0; i < 4; i++ {
		l := createLogger(t, i)
		kp, err := ca.NewClientCertKeyPair()
		assert.NoError(t, err)
		router := NewRouter([]uint16{1}, []string{batcherInfos[i].Endpoint}, [][][]byte{{ca.CertBytes()}}, kp.Cert, kp.Key, l)
		routers = append(routers, router)
	}
	return routers
}

func createConsenters(t *testing.T, consenterNodes []*node, consenterInfos []ConsenterInfo, ledgers []Storage) ([]*Consensus, func()) {
	var consensuses []*Consensus

	initialState := (&arma.State{
		ShardCount: 1,
		N:          4,
		Shards:     []arma.ShardTerm{{Shard: 1}},
		Threshold:  2,
		Quorum:     3,
	}).Serialize()

	bft := &mockBFT{}

	defer bft.Start()

	var cleans []func()

	for i := 0; i < 4; i++ {
		partyID := arma.PartyID(i)

		l := createLogger(t, int(partyID))

		s := fmt.Sprintf("t.Name()-%s", t.Name())
		dir, err := os.MkdirTemp("", s)
		assert.NoError(t, err)

		cleans = append(cleans, func() {
			defer os.RemoveAll(dir)
		})

		db, err := NewBatchAttestationDB(dir, l)
		assert.NoError(t, err)

		verifier := buildVerifier(t, consenterInfos)

		consensus := &Consensus{
			CurrentConfig: types.Configuration{SelfID: uint64(partyID)},
			Arma: &arma.Consenter{
				State:             initialState,
				DB:                db,
				Logger:            l,
				FragmentFromBytes: BatchAttestationFromBytes,
			},
			Logger:       l,
			State:        initialState,
			CurrentNodes: []uint64{1, 2, 3, 4},
			Storage:      ledgers[i],
			BFT:          bft,
			SigVerifier:  verifier,
			Signer:       ECDSASigner(*consenterNodes[i].sk),
		}

		consensuses = append(consensuses, consensus)
		protos.RegisterConsensusServer(consenterNodes[i].Server(), consensus)
		go consenterNodes[i].Start()
		t.Log("Consenter gRPC service listening on", consenterNodes[i].Address())

	}

	bft.consenters = consensuses

	return consensuses, func() {
		for _, clean := range cleans {
			clean()
		}
	}
}

func buildVerifier(t *testing.T, consenterInfos []ConsenterInfo) ECDSAVerifier {
	verifier := make(ECDSAVerifier)
	for _, ci := range consenterInfos {
		pk := ci.PublicKey
		pk2, err := base64.StdEncoding.DecodeString(string(pk))
		assert.NoError(t, err)

		pk3, _ := pem.Decode(pk2)
		assert.NotNil(t, pk3)

		pk4, err := x509.ParsePKIXPublicKey(pk3.Bytes)
		assert.NoError(t, err)

		verifier[arma.PartyID(ci.PartyId)] = *pk4.(*ecdsa.PublicKey)
	}

	return verifier
}

func createBatchers(t *testing.T, batcherNodes []*node, shards []ShardInfo, consenterInfos []ConsenterInfo, ledgers []arma.BatchLedger) []*Batcher {
	var batchers []*Batcher

	for i := 0; i < 4; i++ {
		sk, err := x509.MarshalPKCS8PrivateKey(batcherNodes[i].sk)
		assert.NoError(t, err)

		partyID := arma.PartyID(i)
		l := createLogger(t, int(partyID))
		conf := BatcherNodeConfig{
			Shards:             shards,
			Consenters:         consenterInfos,
			ShardId:            1,
			TLSPrivateKeyFile:  batcherNodes[i].TLSKey,
			TLSCertificateFile: batcherNodes[i].TLSCert,
			PartyId:            uint16(partyID),
			SigningPrivateKey:  RawBytes(base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Bytes: sk}))),
		}

		bp := make(mockSecondaryBatchPuller, 1)
		batcher := NewBatcher(l, conf, ledgers[i], bp)
		batchers = append(batchers, batcher)
		protos.RegisterRequestTransmitServer(batcherNodes[i].Server(), batcher)
		protos.RegisterAckServiceServer(batcherNodes[i].Server(), batcher)
		go batcherNodes[i].Start()
		t.Log("Batcher gRPC service listening on", batcherNodes[i].Address())
	}

	for i := 0; i < 4; i++ {
		ledger := ledgers[1].(*mockBatcherLedger)
		if i == 1 {
			continue
		}
		ledger.secondaries = append(ledger.secondaries, batchers[i].b.BatchPuller.(mockSecondaryBatchPuller))
	}

	return batchers
}

func createConsentersAndBatchers(t *testing.T, ca tlsgen.CA) ([]BatcherInfo, []ConsenterInfo, []*node, []*node) {
	batcherNodes := createNodes(t, ca)
	consenterNodes := createNodes(t, ca)

	var batchers []BatcherInfo
	for i := 0; i < 4; i++ {
		batchers = append(batchers, BatcherInfo{
			PartyID:    uint16(i),
			Endpoint:   batcherNodes[i].Address(),
			TLSCert:    batcherNodes[i].TLSCert,
			TLSCACerts: []RawBytes{RawBytes(base64.StdEncoding.EncodeToString(ca.CertBytes()))},
			PublicKey:  batcherNodes[i].pk,
		})
	}

	var consenters []ConsenterInfo
	for i := 0; i < 4; i++ {
		consenters = append(consenters, ConsenterInfo{
			PartyId:    uint16(i),
			Endpoint:   consenterNodes[i].Address(),
			TLSCACerts: []RawBytes{RawBytes(base64.StdEncoding.EncodeToString(ca.CertBytes()))},
			PublicKey:  consenterNodes[i].pk,
		})
	}
	return batchers, consenters, batcherNodes, consenterNodes
}

func keygen(t *testing.T) (*ecdsa.PrivateKey, []byte) {
	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)

	rawPK, err := x509.MarshalPKIXPublicKey(&sk.PublicKey)
	assert.NoError(t, err)
	return sk, rawPK
}

func createNodes(t *testing.T, ca tlsgen.CA) []*node {
	var result []*node

	var sks []*ecdsa.PrivateKey
	var pks []RawBytes

	for i := 0; i < 4; i++ {
		sk, rawPK := keygen(t)
		sks = append(sks, sk)
		pks = append(pks, RawBytes(base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Bytes: rawPK, Type: "PUBLIC KEY"}))))

	}

	for i := 0; i < 4; i++ {
		kp, err := ca.NewServerCertKeyPair("127.0.0.1")
		assert.NoError(t, err)

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
		assert.NoError(t, err)

		tlsCert := []byte(base64.StdEncoding.EncodeToString(kp.Cert))
		tlsKey := []byte(base64.StdEncoding.EncodeToString(kp.Key))
		result = append(result, &node{GRPCServer: srv, TLSKey: tlsKey, TLSCert: tlsCert, pk: pks[i], sk: sks[i]})
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
