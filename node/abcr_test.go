package node

import (
	arma "arma/pkg"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/v2/pkg/types"
	"github.com/SmartBFT-Go/consensus/v2/smartbftprotos"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
	"github.com/stretchr/testify/assert"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm/tlsgen"
	protos "github.ibm.com/Yacov-Manevich/ARMA/node/protos/comm"
)

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

		proposal := m.consenters[0].AssembleProposal(nil, batch)
		batch = nil
		for _, c := range m.consenters {
			c.Deliver(proposal, nil)
		}
	}
}

func (m *mockBFT) HandleMessage(targetID uint64, _ *smartbftprotos.Message) {

}

func (m *mockBFT) HandleRequest(targetID uint64, request []byte) {

}

func TestABCR(t *testing.T) {
	ca, err := tlsgen.NewCA()
	assert.NoError(t, err)

	batcherInfos, consenterInfos, batcherNodes, consenterNodes := createConsentersAndBatchers(t, ca)

	shards := []ShardInfo{{ShardId: 1, Batchers: batcherInfos}}

	dss, factories := createBatcherDeliverServers(t)

	batchers := createBatchers(t, batcherNodes, shards, consenterInfos, dss, factories)

	routers := createRouters(t, batcherInfos, ca)

	consenterDeliverServes, consenterLedgers := createConsenterLedgers(t)

	_, clean := createConsenters(t, consenterNodes, consenterInfos, consenterLedgers, consenterDeliverServes)
	defer clean()

	for _, b := range batchers {
		go b.b.Run()
	}

	_, armaLedger := createAssembler(t, err, ca, shards, consenterInfos)

	for i := 0; i < 4; i++ {
		routers[i].Submit(context.Background(), &protos.Request{Payload: []byte{1, 2, 3}})
	}

	for armaLedger.Height() == 0 {
		time.Sleep(time.Second)
	}
}

func createConsenterLedgers(t *testing.T) ([]*DeliverService, []Storage) {
	var consenterDeliverServes []*DeliverService
	var consenterLedgers []Storage
	for i := 0; i < 4; i++ {

		dir, err := os.MkdirTemp("", fmt.Sprintf("t.Name()-%s-consenter%d", t.Name(), i))
		assert.NoError(t, err)

		factory, err := fileledger.New(dir, &disabled.Provider{})
		assert.NoError(t, err)

		consensusLedger, err := factory.GetOrCreate("consensus")
		assert.NoError(t, err)

		consenterLedgers = append(consenterLedgers, &ConsensusLedger{ledger: consensusLedger})
		consenterDeliverServes = append(consenterDeliverServes, &DeliverService{
			factory: factory,
		})
	}
	return consenterDeliverServes, consenterLedgers
}

func createBatcherDeliverServers(t *testing.T) ([]*DeliverService, []blockledger.Factory) {
	var factories []blockledger.Factory
	var dss []*DeliverService
	for i := 0; i < 4; i++ {

		dir, err := os.MkdirTemp("", fmt.Sprintf("t.Name()-%s-batcher%d", t.Name(), i))
		assert.NoError(t, err)

		factory, err := fileledger.New(dir, &disabled.Provider{})
		assert.NoError(t, err)

		factories = append(factories, factory)

		ds := &DeliverService{
			factory: factory,
		}

		dss = append(dss, ds)
	}
	return dss, factories
}

func createAssembler(t *testing.T, err error, ca tlsgen.CA, shards []ShardInfo, consenterInfos []ConsenterInfo) (*Assembler, blockledger.ReadWriter) {
	ckp, err := ca.NewClientCertKeyPair()
	assert.NoError(t, err)
	dir, err := os.MkdirTemp("", fmt.Sprintf("t.Name()-%s-assembler", t.Name()))
	assert.NoError(t, err)
	aLogger := createLogger(t, 1)

	factory, err := fileledger.New(dir, &disabled.Provider{})
	assert.NoError(t, err)

	assembler := NewAssembler(aLogger, dir, AssemblerNodeConfig{
		PartyId:            0,
		TLSPrivateKeyFile:  []byte(base64.StdEncoding.EncodeToString(ckp.Key)),
		TLSCertificateFile: []byte(base64.StdEncoding.EncodeToString(ckp.Cert)),
		Shards:             shards,
		Consenter:          consenterInfos[0],
	}, factory)

	ledger, err := factory.GetOrCreate("arma")
	if err != nil {
		aLogger.Panicf("Failed creating arma ledger: %v", err)
	}

	return assembler, ledger
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

func createConsenters(t *testing.T, consenterNodes []*node, consenterInfos []ConsenterInfo, ledgers []Storage, dss []*DeliverService) ([]*Consensus, func()) {
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
		orderer.RegisterAtomicBroadcastServer(consenterNodes[i].Server(), dss[i])
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

func createBatchers(t *testing.T, batcherNodes []*node, shards []ShardInfo, consenterInfos []ConsenterInfo, ds []*DeliverService, factories []blockledger.Factory) []*Batcher {
	var batchers []*Batcher

	for i := 0; i < 4; i++ {
		key, err := x509.MarshalPKCS8PrivateKey(batcherNodes[i].sk)
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
			SigningPrivateKey:  RawBytes(base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Bytes: key}))),
		}

		le, err := factories[i].GetOrCreate("shard1")
		if err != nil {
			l.Panicf("Failed creating ledger: %v", err)
		}

		cert, err := base64.StdEncoding.DecodeString(string(conf.TLSCertificateFile))
		if err != nil {
			l.Panicf("TLS certificate is not a valid base64 encoded string: %v", err)
		}

		tlsKey, err := base64.StdEncoding.DecodeString(string(conf.TLSPrivateKeyFile))
		if err != nil {
			l.Panicf("TLS private key is not a valid base64 encoded string: %v", err)
		}

		ledger := &BatcherLedger{Ledger: le, Logger: l}

		bp := &BatchPuller{
			getHeight: ledger.Height,
			logger:    l,
			config:    conf,
			ledger:    ledger,
			tlsCert:   cert,
			tlsKey:    tlsKey,
		}

		batcher := NewBatcher(l, conf, ledger, bp)
		batchers = append(batchers, batcher)
		protos.RegisterRequestTransmitServer(batcherNodes[i].Server(), batcher)
		protos.RegisterAckServiceServer(batcherNodes[i].Server(), batcher)
		orderer.RegisterAtomicBroadcastServer(batcherNodes[i].Server(), ds[i])
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
