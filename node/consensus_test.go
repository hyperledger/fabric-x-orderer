package node

import (
	arma "arma/pkg"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/assert"
)

func TestConsensus(t *testing.T) {

	sk1, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)

	sk2, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)

	sk3, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)

	sk4, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)

	sks := []*ecdsa.PrivateKey{sk1, sk2, sk3, sk4}

	baf1, err := createBAF(sk1, uint16(1), 1, []byte{1, 2, 3}, 1, 1)
	assert.NoError(t, err)

	baf2, err := createBAF(sk2, uint16(2), 1, []byte{1, 2, 3}, 1, 1)
	assert.NoError(t, err)

	baf11, err := createBAF(sk3, uint16(3), 1, []byte{1, 2, 3}, 1, 1)
	assert.NoError(t, err)

	baf21, err := createBAF(sk4, uint16(4), 1, []byte{1, 2, 3}, 1, 1)
	assert.NoError(t, err)

	baf3, err := createBAF(sk1, uint16(1), 2, []byte{1, 2, 4}, 2, 1)
	assert.NoError(t, err)

	baf4, err := createBAF(sk2, uint16(2), 2, []byte{1, 2, 4}, 2, 1)
	assert.NoError(t, err)

	baf5, err := createBAF(sk1, uint16(1), 1, []byte{1, 2, 5}, 1, 2)
	assert.NoError(t, err)

	baf6, err := createBAF(sk2, uint16(2), 1, []byte{1, 2, 5}, 1, 2)
	assert.NoError(t, err)

	for _, tst := range []struct {
		name                string
		expectedSequences   [][]uint64
		expectedDecisionNum []uint64
		events              []scheduleEvent
	}{
		{
			name:                "two batches single decision",
			expectedSequences:   [][]uint64{{1, 1}},
			expectedDecisionNum: []uint64{1},
			events: []scheduleEvent{
				{ControlEvent: &arma.ControlEvent{BAF: baf1}},
				{ControlEvent: &arma.ControlEvent{BAF: baf2}},
				{ControlEvent: &arma.ControlEvent{BAF: baf3}},
				{ControlEvent: &arma.ControlEvent{BAF: baf4}},
			},
		},
		{
			name:                "two batches single decision more than needed batch attestation shares",
			expectedSequences:   [][]uint64{{1, 1}, {2}},
			expectedDecisionNum: []uint64{1, 2},
			events: []scheduleEvent{
				{ControlEvent: &arma.ControlEvent{BAF: baf1}},
				{ControlEvent: &arma.ControlEvent{BAF: baf2}},
				{ControlEvent: &arma.ControlEvent{BAF: baf3}},
				{ControlEvent: &arma.ControlEvent{BAF: baf4}},
				{delay: time.Second},
				{ControlEvent: &arma.ControlEvent{BAF: baf11}},
				{ControlEvent: &arma.ControlEvent{BAF: baf21}},
				{ControlEvent: &arma.ControlEvent{BAF: baf5}},
				{ControlEvent: &arma.ControlEvent{BAF: baf6}},
			},
		},
	} {
		tst := tst
		t.Run(tst.name, func(t *testing.T) {
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
				c, cleanup := makeConsensusNode(t, sks[i-1], arma.PartyID(i), network, initialState, nodeIDs, v)
				network[uint64(i)] = c
				cleanups = append(cleanups, cleanup)
			}

			for i := uint16(1); i <= 4; i++ {
				err := network[uint64(i)].BFT.Start()
				assert.NoError(t, err)
			}

			for _, ce := range tst.events {
				if ce.delay > 0 {
					time.Sleep(ce.delay)
					continue
				}

				for _, node := range network {
					node.SubmitRequest(ce.Bytes())
					time.Sleep(time.Millisecond)
				}
			}

			var wg sync.WaitGroup
			wg.Add(4)

			for _, node := range network {
				go func(node *Consensus) {
					defer wg.Done()

					tstExpectedSequences := make([][]uint64, len(tst.expectedSequences))
					tstExpectedDecisionNum := make([]uint64, len(tst.expectedDecisionNum))

					copy(tstExpectedSequences, tst.expectedSequences)
					copy(tstExpectedDecisionNum, tst.expectedDecisionNum)

					for {
						rawDecision := <-node.Storage.(mockStorage)
						decision, _, err := bytesToDecision(rawDecision)
						assert.NoError(t, err)

						hdr := &Header{}
						err = hdr.FromBytes(decision.Header)
						assert.NoError(t, err)

						expectedSequences := tstExpectedSequences[0]
						tstExpectedSequences = tstExpectedSequences[1:]

						expectedDecisionNum := tstExpectedDecisionNum[0]
						tstExpectedDecisionNum = tstExpectedDecisionNum[1:]

						var actualSequences []uint64
						for _, ab := range hdr.AvailableBatches {
							actualSequences = append(actualSequences, ab.seq)
						}
						assert.Equal(t, expectedSequences, actualSequences)
						assert.Equal(t, expectedDecisionNum, hdr.Num)

						if len(tstExpectedSequences) == 0 {
							return
						}
					}
				}(node)
			}
			wg.Wait()

		})
	}
}

type scheduleEvent struct {
	*arma.ControlEvent
	delay time.Duration
}

func makeConsensusNode(t *testing.T, sk *ecdsa.PrivateKey, partyID arma.PartyID, network network, initialState []byte, nodes []uint64, verifier ECDSAVerifier) (*Consensus, func()) {
	signer := ECDSASigner(*sk)

	for _, shard := range []arma.ShardID{1, 2, math.MaxUint16} {
		verifier[struct {
			party arma.PartyID
			shard arma.ShardID
		}{party: partyID, shard: shard}] = signer.PublicKey
	}

	dir, err := os.MkdirTemp("", strings.Replace(t.Name(), "/", "-", -1))
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

	config := types.DefaultConfig
	config.SelfID = uint64(partyID)

	c := &Consensus{
		CurrentConfig: types.DefaultConfig,
		Logger:        l,
		Signer:        signer,
		SigVerifier:   verifier,
		State:         initialState,
		CurrentNodes:  nodes,
		Storage:       make(mockStorage),
		Arma:          consenter,
	}

	c.CurrentConfig.SelfID = uint64(partyID)

	wal, err := wal.Create(l, dir, &wal.Options{
		FileSizeBytes:   wal.FileSizeBytesDefault,
		BufferSizeBytes: wal.BufferSizeBytesDefault,
	})
	assert.NoError(t, err)

	c.BFT = &consensus.Consensus{
		Metadata:          &smartbftprotos.ViewMetadata{},
		Logger:            l,
		Signer:            c,
		Application:       c,
		RequestInspector:  c,
		Assembler:         c,
		Scheduler:         time.NewTicker(time.Second).C,
		ViewChangerTicker: time.NewTicker(time.Second).C,
		WAL:               wal,
		Config:            c.CurrentConfig,
		Verifier:          c,
		Comm: &mockComm{
			nodes: nodes,
			from:  uint64(partyID),
			net:   network,
		},
	}

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

func TestAvailableBatches(t *testing.T) {
	var ab AvailableBatch
	ab.digest = make([]byte, 32)
	ab.primary = 42
	ab.shard = 666
	ab.seq = 100

	var ab2 AvailableBatch
	ab2.Deserialize(ab.Serialize())

	assert.Equal(t, ab, ab2)
}

func TestHeaderBytes(t *testing.T) {
	hdr := Header{
		State: []byte{1, 2, 3},
		Num:   100,
		AvailableBatches: []AvailableBatch{
			{seq: 1, shard: 2, primary: 3, digest: make([]byte, 32)},
			{seq: 4, shard: 5, primary: 6, digest: make([]byte, 32)},
		},
	}

	var hdr2 Header
	hdr2.FromBytes(hdr.Bytes())

	assert.Equal(t, hdr, hdr2)
}
