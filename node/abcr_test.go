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
	"math"
	"os"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/v2/pkg/consensus"
	"github.com/SmartBFT-Go/consensus/v2/pkg/types"
	"github.com/SmartBFT-Go/consensus/v2/pkg/wal"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
	"github.com/stretchr/testify/require"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm/tlsgen"
	protos "github.ibm.com/Yacov-Manevich/ARMA/node/protos/comm"
)

func TestABCR(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	batcherInfos, consenterInfos, batcherNodes, consenterNodes := createConsentersAndBatchers(t, ca)

	shards := []ShardInfo{{ShardId: 1, Batchers: batcherInfos}}

	dss, factories := createBatcherDeliverServers(t)

	consenterDeliverServes, consenterLedgers := createConsenterLedgers(t)

	_, clean := createConsenters(t, consenterNodes, consenterInfos, consenterLedgers, consenterDeliverServes, shards)
	defer clean()

	batchers := createBatchers(t, batcherNodes, shards, consenterInfos, dss, factories)

	routers := createRouters(t, batcherInfos, ca)

	for _, b := range batchers {
		go b.b.Run()
	}

	_, armaLedger := createAssembler(t, err, ca, shards, consenterInfos)

	for i := 0; i < 10; i++ {
		txn := make([]byte, 32)
		rand.Read(txn)

		for i := 0; i < 4; i++ {
			routers[i].Submit(context.Background(), &protos.Request{Payload: txn})
		}

		for armaLedger.Height() == uint64(i) {
			time.Sleep(time.Millisecond * 10)
		}
	}

}

func createConsenterLedgers(t *testing.T) ([]*DeliverService, []Storage) {
	var consenterDeliverServes []*DeliverService
	var consenterLedgers []Storage
	for i := 0; i < 4; i++ {

		dir, err := os.MkdirTemp("", fmt.Sprintf("t.Name()-%s-consenter%d", t.Name(), i+1))
		require.NoError(t, err)

		factory, err := fileledger.New(dir, &disabled.Provider{})
		require.NoError(t, err)

		consensusLedger, err := factory.GetOrCreate("consensus")
		require.NoError(t, err)

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

		dir, err := os.MkdirTemp("", fmt.Sprintf("t.Name()-%s-batcher%d", t.Name(), i+1))
		require.NoError(t, err)

		factory, err := fileledger.New(dir, &disabled.Provider{})
		require.NoError(t, err)

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
	require.NoError(t, err)
	dir, err := os.MkdirTemp("", fmt.Sprintf("t.Name()-%s-assembler", t.Name()))
	require.NoError(t, err)
	aLogger := createLogger(t, 1)

	factory, err := fileledger.New(dir, &disabled.Provider{})
	require.NoError(t, err)

	assembler := NewAssembler(aLogger, dir, AssemblerNodeConfig{
		PartyId:            1,
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
		require.NoError(t, err)
		router := NewRouter([]uint16{1}, []string{batcherInfos[i].Endpoint}, [][][]byte{{ca.CertBytes()}}, kp.Cert, kp.Key, l)
		routers = append(routers, router)
	}
	return routers
}

func createConsenters(t *testing.T, consenterNodes []*node, consenterInfos []ConsenterInfo, ledgers []Storage, dss []*DeliverService, shardInfo []ShardInfo) ([]*Consensus, func()) {
	var consensuses []*Consensus

	initialState := (&arma.State{
		ShardCount: 1,
		N:          4,
		Shards:     []arma.ShardTerm{{Shard: 1}},
		Threshold:  2,
		Quorum:     3,
	}).Serialize()

	var cleans []func()

	for i := 0; i < 4; i++ {

		partyID := arma.PartyID(i + 1)

		config := types.DefaultConfig
		config.SelfID = uint64(partyID)
		config.DecisionsPerLeader = 0
		config.LeaderRotation = false

		l := createLogger(t, int(partyID))

		s := fmt.Sprintf("t.Name()-%s", t.Name())
		dir, err := os.MkdirTemp("", s)
		require.NoError(t, err)

		cleans = append(cleans, func() {
			defer os.RemoveAll(dir)
		})

		db, err := NewBatchAttestationDB(dir, l)
		require.NoError(t, err)

		consenterVerifier := buildVerifier(t, consenterInfos, shardInfo)

		wal, err := wal.Create(l, dir, &wal.Options{
			FileSizeBytes:   wal.FileSizeBytesDefault,
			BufferSizeBytes: wal.BufferSizeBytesDefault,
		})
		require.NoError(t, err)

		sk, err := x509.MarshalPKCS8PrivateKey(consenterNodes[i].sk)
		require.NoError(t, err)

		c := &Consensus{
			Config: ConsenterNodeConfig{
				TLSPrivateKeyFile:  consenterNodes[i].TLSKey,
				TLSCertificateFile: consenterNodes[i].TLSCert,
				PartyId:            uint16(partyID),
				SigningPrivateKey:  []byte(base64.StdEncoding.EncodeToString(sk)),
				Consenters:         consenterInfos,
			},
			WAL:           wal,
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
			SigVerifier:  consenterVerifier,
			Signer:       ECDSASigner(*consenterNodes[i].sk),
		}

		bft := &consensus.Consensus{
			Logger:            l,
			Config:            config,
			WAL:               wal,
			RequestInspector:  c,
			Signer:            c,
			Assembler:         c,
			Synchronizer:      c,
			Scheduler:         time.NewTicker(time.Second).C,
			ViewChangerTicker: time.NewTicker(time.Second).C,
			Application:       c,
			Verifier:          c,
		}
		c.BFT = bft

		myIdentity := getOurIdentity(t, consenterInfos, partyID)

		SetupComm(c, consenterNodes[i].Server(), myIdentity)

		consensuses = append(consensuses, c)
		protos.RegisterConsensusServer(consenterNodes[i].Server(), c)
		orderer.RegisterAtomicBroadcastServer(consenterNodes[i].Server(), dss[i])
		go consenterNodes[i].Start()
		err = bft.Start()
		require.NoError(t, err)
		t.Log("Consenter gRPC service listening on", consenterNodes[i].Address())
	}

	return consensuses, func() {
		for _, clean := range cleans {
			clean()
		}
	}
}

func getOurIdentity(t *testing.T, consenterInfos []ConsenterInfo, partyID arma.PartyID) []byte {
	var myIdentity []byte
	for _, ci := range consenterInfos {
		pk := ci.PublicKey
		pk2, err := base64.StdEncoding.DecodeString(string(pk))
		require.NoError(t, err)

		if ci.PartyID == uint16(partyID) {
			myIdentity = pk2
			break
		}
	}
	return myIdentity
}

func buildVerifier(t *testing.T, consenterInfos []ConsenterInfo, shardInfo []ShardInfo) ECDSAVerifier {
	verifier := make(ECDSAVerifier)
	for _, ci := range consenterInfos {
		pk := ci.PublicKey
		pk2, err := base64.StdEncoding.DecodeString(string(pk))
		require.NoError(t, err)

		pk3, _ := pem.Decode(pk2)
		require.NotNil(t, pk3)

		pk4, err := x509.ParsePKIXPublicKey(pk3.Bytes)
		require.NoError(t, err)

		verifier[struct {
			party arma.PartyID
			shard arma.ShardID
		}{shard: math.MaxUint16, party: arma.PartyID(ci.PartyID)}] = *pk4.(*ecdsa.PublicKey)
	}

	for _, shard := range shardInfo {
		for _, bi := range shard.Batchers {
			pk := bi.PublicKey
			pk2, err := base64.StdEncoding.DecodeString(string(pk))
			require.NoError(t, err)

			pk3, _ := pem.Decode(pk2)
			require.NotNil(t, pk3)

			pk4, err := x509.ParsePKIXPublicKey(pk3.Bytes)
			require.NoError(t, err)

			verifier[struct {
				party arma.PartyID
				shard arma.ShardID
			}{shard: arma.ShardID(shard.ShardId), party: arma.PartyID(bi.PartyID)}] = *pk4.(*ecdsa.PublicKey)
		}
	}

	return verifier
}

func createBatchers(t *testing.T, batcherNodes []*node, shards []ShardInfo, consenterInfos []ConsenterInfo, ds []*DeliverService, factories []blockledger.Factory) []*Batcher {
	var batchers []*Batcher

	for i := 0; i < 4; i++ {
		key, err := x509.MarshalPKCS8PrivateKey(batcherNodes[i].sk)
		require.NoError(t, err)

		partyID := arma.PartyID(i) + 1
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
			PartyID:    uint16(i + 1),
			Endpoint:   batcherNodes[i].Address(),
			TLSCert:    batcherNodes[i].TLSCert,
			TLSCACerts: []RawBytes{RawBytes(base64.StdEncoding.EncodeToString(ca.CertBytes()))},
			PublicKey:  batcherNodes[i].pk,
		})
	}

	var consenters []ConsenterInfo
	for i := 0; i < 4; i++ {
		consenters = append(consenters, ConsenterInfo{
			PartyID:    uint16(i + 1),
			Endpoint:   consenterNodes[i].Address(),
			TLSCACerts: []RawBytes{RawBytes(base64.StdEncoding.EncodeToString(ca.CertBytes()))},
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
		pks = append(pks, RawBytes(base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Bytes: rawPK, Type: "PUBLIC KEY"}))))

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
