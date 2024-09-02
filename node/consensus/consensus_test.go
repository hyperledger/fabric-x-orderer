package consensus

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"math"
	"math/big"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	arma_types "arma/common/types"
	"arma/core"
	"arma/node/batcher"
	"arma/node/consensus/state"
	"arma/node/crypto"
	"arma/testutil"

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

	dig := make([]byte, 32-3)

	dig123 := append([]byte{1, 2, 3}, dig...)
	baf123id1p1s1, err := batcher.CreateBAF(sk1, 1, 1, dig123, 1, 1)
	assert.NoError(t, err)
	baf123id2p1s1, err := batcher.CreateBAF(sk2, 2, 1, dig123, 1, 1)
	assert.NoError(t, err)
	baf123id3p1s1, err := batcher.CreateBAF(sk3, 3, 1, dig123, 1, 1)
	assert.NoError(t, err)
	baf123id4p1s1, err := batcher.CreateBAF(sk4, 4, 1, dig123, 1, 1)
	assert.NoError(t, err)

	dig124 := append([]byte{1, 2, 4}, dig...)
	baf124id1p2s1, err := batcher.CreateBAF(sk1, 1, 2, dig124, 2, 1)
	assert.NoError(t, err)
	baf124id2p2s1, err := batcher.CreateBAF(sk2, 2, 2, dig124, 2, 1)
	assert.NoError(t, err)

	dig125 := append([]byte{1, 2, 5}, dig...)
	baf125id1p1s2, err := batcher.CreateBAF(sk1, 1, 1, dig125, 1, 2)
	assert.NoError(t, err)
	baf125id2p1s2, err := batcher.CreateBAF(sk2, 2, 1, dig125, 1, 2)
	assert.NoError(t, err)

	for _, tst := range []struct {
		name                string
		expectedSequences   [][]arma_types.BatchSequence
		expectedDecisionNum []uint64
		events              []scheduleEvent
		commitEvent         *sync.WaitGroup
	}{
		{
			name:                "two batches single decision",
			expectedSequences:   [][]arma_types.BatchSequence{{1, 1}},
			expectedDecisionNum: []uint64{1},
			commitEvent:         new(sync.WaitGroup),
			events: []scheduleEvent{
				{expectCommits: big.NewInt(4)},
				{ControlEvent: &core.ControlEvent{BAF: baf123id1p1s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf123id2p1s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf124id1p2s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf124id2p2s1}},
				{waitForCommit: &struct{}{}},
			},
		},
		{
			name:                "two batches single decision more than needed batch attestation shares",
			expectedSequences:   [][]arma_types.BatchSequence{{1, 1}, {2}},
			expectedDecisionNum: []uint64{1, 2},
			commitEvent:         new(sync.WaitGroup),
			events: []scheduleEvent{
				{expectCommits: big.NewInt(4)},
				{ControlEvent: &core.ControlEvent{BAF: baf123id1p1s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf123id2p1s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf124id1p2s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf124id2p2s1}},
				{waitForCommit: &struct{}{}},
				{expectCommits: big.NewInt(4)},
				{ControlEvent: &core.ControlEvent{BAF: baf123id3p1s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf123id4p1s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf125id1p1s2}},
				{ControlEvent: &core.ControlEvent{BAF: baf125id2p1s2}},
				{waitForCommit: &struct{}{}},
			},
		},
		{
			name:                "two batches from same primary in single decision",
			expectedSequences:   [][]arma_types.BatchSequence{{1, 2}, {1}},
			expectedDecisionNum: []uint64{1, 2},
			commitEvent:         new(sync.WaitGroup),
			events: []scheduleEvent{
				{expectCommits: big.NewInt(4)},
				{ControlEvent: &core.ControlEvent{BAF: baf123id3p1s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf123id4p1s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf125id1p1s2}},
				{ControlEvent: &core.ControlEvent{BAF: baf125id2p1s2}},
				{waitForCommit: &struct{}{}},
				{expectCommits: big.NewInt(4)},
				{ControlEvent: &core.ControlEvent{BAF: baf123id1p1s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf123id2p1s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf124id1p2s1}},
				{ControlEvent: &core.ControlEvent{BAF: baf124id2p2s1}},
				{waitForCommit: &struct{}{}},
			},
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			verifier := make(crypto.ECDSAVerifier)

			initialAppContext := &state.BlockHeader{
				Number:   0,
				PrevHash: nil,
				Digest:   nil,
			}

			initialState := &core.State{
				ShardCount: 2,
				N:          4,
				Shards:     []core.ShardTerm{{Shard: 1}, {Shard: 2}},
				Threshold:  2,
				Quorum:     3,
				AppContext: initialAppContext.Bytes(),
			}

			nodeIDs := []uint64{1, 2, 3, 4}

			var cleanups []func()

			defer func() {
				for _, cleanup := range cleanups {
					cleanup()
				}
			}()

			network := make(network)

			for i := uint16(1); i <= 4; i++ {
				onCommit := func() {
					tst.commitEvent.Done()
				}
				c, cleanup := makeConsensusNode(t, sks[i-1], arma_types.PartyID(i), network, initialState, nodeIDs, verifier, onCommit)
				network[uint64(i)] = c
				cleanups = append(cleanups, cleanup)
			}

			for i := uint16(1); i <= 4; i++ {
				err := network[uint64(i)].Start()
				assert.NoError(t, err)
			}

			for _, ce := range tst.events {
				if ce.waitForCommit != nil {
					tst.commitEvent.Wait()
					continue
				}

				if ce.expectCommits != nil {
					tst.commitEvent.Add(int(ce.expectCommits.Uint64()))
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

					tstExpectedSequences := make([][]arma_types.BatchSequence, len(tst.expectedSequences))
					tstExpectedDecisionNum := make([]uint64, len(tst.expectedDecisionNum))

					copy(tstExpectedSequences, tst.expectedSequences)
					copy(tstExpectedDecisionNum, tst.expectedDecisionNum)

					for {
						rawDecision := <-node.Storage.(*commitInterceptor).Storage.(mockStorage)
						decision, _, err := bytesToDecision(rawDecision)
						assert.NoError(t, err)

						hdr := &state.Header{}
						err = hdr.FromBytes(decision.Header)
						assert.NoError(t, err)

						expectedSequences := tstExpectedSequences[0]
						tstExpectedSequences = tstExpectedSequences[1:]

						expectedDecisionNum := tstExpectedDecisionNum[0]
						tstExpectedDecisionNum = tstExpectedDecisionNum[1:]

						var actualSequences []arma_types.BatchSequence
						for _, ab := range hdr.AvailableBatches {
							actualSequences = append(actualSequences, ab.Seq())
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
	*core.ControlEvent
	expectCommits *big.Int
	waitForCommit *struct{}
}

func makeConsensusNode(t *testing.T, sk *ecdsa.PrivateKey, partyID arma_types.PartyID, network network, initialState *core.State, nodes []uint64, verifier crypto.ECDSAVerifier, onCommit func()) (*Consensus, func()) {
	signer := crypto.ECDSASigner(*sk)

	for _, shard := range []arma_types.ShardID{1, 2, math.MaxUint16} {
		verifier[crypto.ShardPartyKey{Party: partyID, Shard: shard}] = signer.PublicKey
	}

	dir, err := os.MkdirTemp("", strings.Replace(t.Name(), "/", "-", -1))
	assert.NoError(t, err)

	l := testutil.CreateLogger(t, int(partyID))

	db, err := NewBatchAttestationDB(dir, l)
	assert.NoError(t, err)

	consenter := &core.Consenter{ // TODO should this be initialized as part of consensus node start?
		State:           initialState,
		DB:              db,
		Logger:          l,
		BAFDeserializer: &state.BAFDeserializer{},
	}

	c := &Consensus{
		CurrentConfig: types.DefaultConfig,
		Logger:        l,
		Signer:        signer,
		SigVerifier:   verifier,
		State:         initialState,
		CurrentNodes:  nodes,
		Storage:       &commitInterceptor{Storage: make(mockStorage, 100), f: onCommit},
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

type commitInterceptor struct {
	Storage
	f func()
}

func (c *commitInterceptor) Append(bytes []byte) {
	defer c.f()
	c.Storage.Append(bytes)
}
