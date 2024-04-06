package node

import (
	arma "arma/pkg"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"github.com/SmartBFT-Go/consensus/v2/pkg/consensus"
	"github.com/SmartBFT-Go/consensus/v2/pkg/types"
	"github.com/SmartBFT-Go/consensus/v2/pkg/wal"
	"github.com/SmartBFT-Go/consensus/v2/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

func TestConsensus(t *testing.T) {

	v := make(ECDSAVerifier)

	initialState := (&arma.State{
		ShardCount: 2,
		N:          4,
		Shards:     []arma.ShardTerm{{Shard: 1}, {Shard: 2}},
		Threshold:  2,
		Quorum:     3,
	}).Serialize()

	nodeIDs := []uint64{1, 2, 3, 4}

	var cleanups []func()

	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()

	network := make(network)

	for i := uint16(1); i <= 4; i++ {
		c, cleanup := makeConsensusNode(t, i, network, initialState, nodeIDs, v)
		network[uint64(i)] = c
		cleanups = append(cleanups, cleanup)
	}

	for i := uint16(1); i <= 4; i++ {
		err := network[uint64(i)].BFT.Start()
		assert.NoError(t, err)
	}

	baf1, err := createBAF(network[1].Signer, uint16(network[1].BFT.Config.SelfID), 1, []byte{1, 2, 3}, 1, 1)
	assert.NoError(t, err)

	baf2, err := createBAF(network[2].Signer, uint16(network[2].BFT.Config.SelfID), 1, []byte{1, 2, 3}, 1, 1)
	assert.NoError(t, err)

	baf3, err := createBAF(network[1].Signer, uint16(network[1].BFT.Config.SelfID), 2, []byte{1, 2, 3}, 2, 1)
	assert.NoError(t, err)

	baf4, err := createBAF(network[2].Signer, uint16(network[2].BFT.Config.SelfID), 2, []byte{1, 2, 3}, 2, 1)
	assert.NoError(t, err)

	network[uint64(1)].SubmitRequest((&arma.ControlEvent{BAF: baf1}).Bytes())
	network[uint64(1)].SubmitRequest((&arma.ControlEvent{BAF: baf2}).Bytes())

	network[uint64(1)].SubmitRequest((&arma.ControlEvent{BAF: baf3}).Bytes())
	network[uint64(1)].SubmitRequest((&arma.ControlEvent{BAF: baf4}).Bytes())

	var wg sync.WaitGroup
	wg.Add(4)

	for _, node := range network {
		go func(node *Consensus) {
			defer wg.Done()
			rawDecision := <-node.Storage.(mockStorage)
			decision, _, err := bytesToDecision(rawDecision)
			assert.NoError(t, err)

			hdr := &Header{}
			err = hdr.FromBytes(decision.Header)
			assert.NoError(t, err)

			assert.Equal(t, []uint64{0, 1}, hdr.Sequences)
			assert.Equal(t, uint64(1), hdr.Num)
		}(node)
	}

	wg.Wait()

}

func makeConsensusNode(t *testing.T, partyID uint16, network network, initialState []byte, nodes []uint64, verifier ECDSAVerifier) (*Consensus, func()) {
	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)

	signer := ECDSASigner(*sk)
	verifier[partyID] = signer.PublicKey

	dir, err := os.MkdirTemp("", t.Name())
	assert.NoError(t, err)

	l := createLogger(t, int(partyID))

	db, err := NewBatchAttestationDB(dir, l)
	assert.NoError(t, err)

	consenter := &arma.Consenter{
		State:             initialState,
		DB:                db,
		Logger:            l,
		FragmentFromBytes: BatchAttestationFromBytes,
	}

	c := &Consensus{
		Logger:       l,
		Signer:       signer,
		SigVerifier:  verifier,
		State:        initialState,
		CurrentNodes: nodes,
		Storage:      make(mockStorage),
		Arma:         consenter,
	}

	wal, err := wal.Create(l, dir, &wal.Options{
		FileSizeBytes:   wal.FileSizeBytesDefault,
		BufferSizeBytes: wal.BufferSizeBytesDefault,
	})
	assert.NoError(t, err)

	c.BFT = &consensus.Consensus{
		Logger:            l,
		Signer:            c,
		Application:       c,
		RequestInspector:  c,
		Synchronizer:      c,
		Assembler:         c,
		Scheduler:         time.NewTicker(time.Second).C,
		ViewChangerTicker: time.NewTicker(time.Second).C,
		WAL:               wal,
		Config:            types.DefaultConfig,
		Verifier:          c,
		Comm: &mockComm{
			nodes: nodes,
			from:  uint64(partyID),
			net:   network,
		},
	}

	c.BFT.Config.SelfID = uint64(partyID)

	consenter.TotalOrder = c.BFT

	return c, func() {
		os.RemoveAll(dir)
	}
}

type mockComm struct {
	from  uint64
	net   network
	nodes []uint64
}

func (comm *mockComm) SendConsensus(targetID uint64, m *smartbftprotos.Message) {
	comm.net[targetID].BFT.HandleMessage(comm.from, m)
}

func (comm *mockComm) SendTransaction(targetID uint64, request []byte) {
	comm.net[targetID].BFT.HandleRequest(comm.from, request)
}

func (comm *mockComm) Nodes() []uint64 {
	return comm.nodes
}

type network map[uint64]*Consensus

type mockStorage chan []byte

func (m mockStorage) Append(bytes []byte) {
	m <- bytes
}

func TestProposalSerialization(t *testing.T) {
	proposal := types.Proposal{
		Header:   []byte{1, 2, 3},
		Payload:  []byte{4, 5, 6},
		Metadata: []byte{7, 8, 9},
	}
	signatures := []types.Signature{{ID: 10, Value: []byte{11}, Msg: []byte{12}}, {ID: 13, Value: []byte{14}, Msg: []byte{15}}}
	bytes := decisionToBytes(proposal, signatures)
	proposal2, signatures2, err := bytesToDecision(bytes)
	assert.NoError(t, err)
	assert.Equal(t, proposal, proposal2)
	assert.Equal(t, signatures, signatures2)
}
