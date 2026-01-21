/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"math/big"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	ordererRulesMocks "github.com/hyperledger/fabric-x-orderer/config/verify/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	node_consensus "github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/badb"
	configrequest_mocks "github.com/hyperledger/fabric-x-orderer/node/consensus/configrequest/mocks"
	consensus_mocks "github.com/hyperledger/fabric-x-orderer/node/consensus/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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
	baf123id1p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sk1), 1, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)
	baf123id2p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sk2), 2, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)
	baf123id3p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sk3), 3, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)
	baf123id4p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sk4), 4, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)

	dig124 := append([]byte{1, 2, 4}, dig...)
	baf124id1p2s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sk1), 1, 2, dig124, 2, 1, 0)
	assert.NoError(t, err)
	baf124id2p2s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sk2), 2, 2, dig124, 2, 1, 0)
	assert.NoError(t, err)

	dig125 := append([]byte{1, 2, 5}, dig...)
	baf125id1p1s2, err := batcher.CreateBAF(crypto.ECDSASigner(*sk1), 1, 1, dig125, 1, 2, 0)
	assert.NoError(t, err)
	baf125id2p1s2, err := batcher.CreateBAF(crypto.ECDSASigner(*sk2), 2, 1, dig125, 1, 2, 0)
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
				{ControlEvent: &state.ControlEvent{BAF: baf123id1p1s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf123id2p1s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf124id1p2s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf124id2p2s1}},
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
				{ControlEvent: &state.ControlEvent{BAF: baf123id1p1s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf123id2p1s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf124id1p2s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf124id2p2s1}},
				{waitForCommit: &struct{}{}},
				{expectCommits: big.NewInt(4)},
				{ControlEvent: &state.ControlEvent{BAF: baf123id3p1s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf123id4p1s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf125id1p1s2}},
				{ControlEvent: &state.ControlEvent{BAF: baf125id2p1s2}},
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
				{ControlEvent: &state.ControlEvent{BAF: baf123id3p1s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf123id4p1s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf125id1p1s2}},
				{ControlEvent: &state.ControlEvent{BAF: baf125id2p1s2}},
				{waitForCommit: &struct{}{}},
				{expectCommits: big.NewInt(4)},
				{ControlEvent: &state.ControlEvent{BAF: baf123id1p1s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf123id2p1s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf124id1p2s1}},
				{ControlEvent: &state.ControlEvent{BAF: baf124id2p2s1}},
				{waitForCommit: &struct{}{}},
			},
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			verifier := make(crypto.ECDSAVerifier)

			initialState := &state.State{
				ShardCount: 2,
				N:          4,
				Shards:     []state.ShardTerm{{Shard: 1}, {Shard: 2}},
				Threshold:  2,
				Quorum:     3,
				AppContext: protoutil.MarshalOrPanic(&common.BlockHeader{Number: 0}),
			}

			nodeIDs := []uint64{1, 2, 3, 4}

			var cleanups []func()

			defer func() {
				for _, cleanup := range cleanups {
					cleanup()
				}
			}()

			network := make(network)
			listeners := make([]*storageListener, 4)

			for i := uint16(1); i <= 4; i++ {
				onCommit := func() {
					tst.commitEvent.Done()
				}
				dir := t.TempDir()
				c, cleanup := makeConsensusNode(t, sks[i-1], arma_types.PartyID(i), network, initialState, nodeIDs, verifier, dir)

				listeners[i-1] = &storageListener{c: make(chan *common.Block, 100), f: onCommit}
				c.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(listeners[i-1])
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
				go func(node *node_consensus.Consensus) {
					defer wg.Done()

					tstExpectedSequences := make([][]arma_types.BatchSequence, len(tst.expectedSequences))
					tstExpectedDecisionNum := make([]uint64, len(tst.expectedDecisionNum))

					copy(tstExpectedSequences, tst.expectedSequences)
					copy(tstExpectedDecisionNum, tst.expectedDecisionNum)

					for {
						b := <-listeners[node.BFTConfig.SelfID-1].c
						decision, _, err := state.BytesToDecision(b.Data.Data[0])
						assert.NoError(t, err)

						hdr := &state.Header{}
						err = hdr.Deserialize(decision.Header)
						assert.NoError(t, err)

						expectedSequences := tstExpectedSequences[0]
						tstExpectedSequences = tstExpectedSequences[1:]

						expectedDecisionNum := tstExpectedDecisionNum[0]
						tstExpectedDecisionNum = tstExpectedDecisionNum[1:]

						var actualSequences []arma_types.BatchSequence
						for _, acb := range hdr.AvailableCommonBlocks {
							_, _, seq, _, _, _, _, err := ledger.AssemblerBlockMetadataFromBytes(acb.Metadata.Metadata[common.BlockMetadataIndex_ORDERER])
							assert.NoError(t, err)
							actualSequences = append(actualSequences, seq)
						}
						assert.Equal(t, expectedSequences, actualSequences)
						assert.Equal(t, expectedDecisionNum, uint64(hdr.Num))

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
	*state.ControlEvent
	expectCommits *big.Int
	waitForCommit *struct{}
}

func makeConsensusNode(t *testing.T, sk *ecdsa.PrivateKey, partyID arma_types.PartyID, network network, initialState *state.State, nodes []uint64, verifier crypto.ECDSAVerifier, dir string) (*node_consensus.Consensus, func()) {
	signer := crypto.ECDSASigner(*sk)

	for _, shard := range []arma_types.ShardID{1, 2, arma_types.ShardIDConsensus} {
		verifier[crypto.ShardPartyKey{Party: partyID, Shard: shard}] = signer.PublicKey
	}

	l := testutil.CreateLogger(t, int(partyID))

	db, err := badb.NewBatchAttestationDB(dir, l)
	assert.NoError(t, err)

	ledger, err := ledger.NewConsensusLedger(dir)
	assert.NoError(t, err)

	initialState, md := initializeStateAndMetadata(t, initialState, ledger)

	consenter := &node_consensus.Consenter{ // TODO should this be initialized as part of consensus node start?
		DB:              db,
		Logger:          l,
		BAFDeserializer: &state.BAFDeserialize{},
	}

	bundle := &configMocks.FakeConfigResources{}
	configtxValidator := &policyMocks.FakeConfigtxValidator{}
	configtxValidator.ChannelIDReturns("arma")
	configtxValidator.SequenceReturns(0)
	bundle.ConfigtxValidatorReturns(configtxValidator)

	consenterNodeConfig := nodeconfig.ConsenterNodeConfig{Bundle: bundle, PartyId: partyID, MonitoringListenAddress: "127.0.0.1:0", MetricsLogInterval: 3 * time.Second}

	c := &node_consensus.Consensus{
		Config:       &consenterNodeConfig,
		BFTConfig:    smartbft_types.DefaultConfig,
		Logger:       l,
		Signer:       signer,
		SigVerifier:  verifier,
		State:        initialState,
		CurrentNodes: nodes,
		Storage:      ledger,
		Arma:         consenter,
		BADB:         db,
		Net:          &consensus_mocks.FakeNetStopper{},
		Synchronizer: &consensus_mocks.FakeSynchronizerStopper{},
		Metrics:      node_consensus.NewConsensusMetrics(&consenterNodeConfig, ledger.Height(), l),
	}

	c.BFTConfig.SelfID = uint64(partyID)
	c.BFTConfig.RequestBatchMaxInterval = 500 * time.Millisecond // wait for all control events before creating a new batch

	bftWAL, walInitState, err := wal.InitializeAndReadAll(l, dir, wal.DefaultOptions())
	assert.NoError(t, err)

	c.BFT = &consensus.Consensus{
		Metadata:          md,
		Logger:            l,
		Signer:            c,
		Application:       c,
		RequestInspector:  c,
		Assembler:         c,
		Scheduler:         time.NewTicker(time.Second).C,
		ViewChangerTicker: time.NewTicker(time.Second).C,
		WAL:               bftWAL,
		WALInitialContent: walInitState,
		Config:            c.BFTConfig,
		Verifier:          c,
		Comm: &mockComm{
			nodes: nodes,
			from:  uint64(partyID),
			net:   network,
		},
	}

	return c, func() {
		c.Stop()
		os.RemoveAll(dir)
	}
}

func initializeStateAndMetadata(t *testing.T, initState *state.State, ledger *ledger.ConsensusLedger) (*state.State, *smartbftprotos.ViewMetadata) {
	height := ledger.Height()

	if height == 0 {
		genesisCommonBlock := &common.Block{Header: &common.BlockHeader{Number: 0}}
		genesisProposal := smartbft_types.Proposal{
			Header: (&state.Header{
				AvailableCommonBlocks: []*common.Block{genesisCommonBlock},
				State:                 initState,
				Num:                   0,
			}).Serialize(),
			Metadata: nil,
		}
		ledger.Append(state.DecisionToBytes(genesisProposal, nil))
		return initState, &smartbftprotos.ViewMetadata{}
	}

	lastBlock, err := ledger.RetrieveBlockByNumber(height - 1)
	assert.NoError(t, err)

	proposal, _, err := state.BytesToDecision(lastBlock.Data.Data[0])
	assert.NoError(t, err)

	md := &smartbftprotos.ViewMetadata{}
	err = proto.Unmarshal(proposal.Metadata, md)
	assert.NoError(t, err)

	header := &state.Header{}
	err = header.Deserialize(proposal.Header)
	assert.NoError(t, err)

	return header.State, md
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

type network map[uint64]*node_consensus.Consensus

func TestAssembleProposalAndVerify(t *testing.T) {
	logger := testutil.CreateLogger(t, 1)

	dir, err := os.MkdirTemp("", strings.Replace(t.Name(), "/", "-", -1))
	assert.NoError(t, err)

	db, err := badb.NewBatchAttestationDB(dir, logger)
	assert.NoError(t, err)

	verifier := make(crypto.ECDSAVerifier)

	bundle := &configMocks.FakeConfigResources{}
	configtxValidator := &policyMocks.FakeConfigtxValidator{}
	configtxValidator.ChannelIDReturns("arma")
	configEnvelope := &common.ConfigEnvelope{
		Config:     nil,
		LastUpdate: nil,
	}
	configtxValidator.ProposeConfigUpdateReturns(configEnvelope, nil)
	bundle.ConfigtxValidatorReturns(configtxValidator)

	config := &nodeconfig.ConsenterNodeConfig{
		ClientSignatureVerificationRequired: false,
		Bundle:                              bundle,
		RequestMaxBytes:                     1000,
	}
	requestVerifier := node_consensus.CreateConsensusRulesVerifier(config)

	numOfParties := 4

	sks := make([]*ecdsa.PrivateKey, numOfParties)

	for i := 0; i < numOfParties; i++ {
		sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		assert.NoError(t, err)

		sks[i] = sk

		signer := crypto.ECDSASigner(*sk)
		for _, shard := range []arma_types.ShardID{1, 2, arma_types.ShardIDConsensus} {
			verifier[crypto.ShardPartyKey{Party: arma_types.PartyID(i + 1), Shard: shard}] = signer.PublicKey
		}
	}

	signer1 := crypto.ECDSASigner(*sks[0])
	complaint1 := &state.Complaint{ShardTerm: state.ShardTerm{Shard: 1}, Signer: 1}
	sig, err := signer1.Sign(complaint1.ToBeSigned())
	require.NoError(t, err)
	complaint1.Signature = sig

	complaint1ConfigSeq1 := &state.Complaint{ShardTerm: state.ShardTerm{Shard: 1}, Signer: 1, ConfigSeq: 1}
	sig, err = signer1.Sign(complaint1ConfigSeq1.ToBeSigned())
	require.NoError(t, err)
	complaint1ConfigSeq1.Signature = sig

	signer2 := crypto.ECDSASigner(*sks[1])
	complaint2 := &state.Complaint{ShardTerm: state.ShardTerm{Shard: 1}, Signer: 2}
	sig, err = signer2.Sign(complaint2.ToBeSigned())
	require.NoError(t, err)
	complaint2.Signature = sig

	signer3 := crypto.ECDSASigner(*sks[2])
	complaint3 := &state.Complaint{ShardTerm: state.ShardTerm{Shard: 1}, Signer: 3}
	sig, err = signer3.Sign(complaint3.ToBeSigned())
	require.NoError(t, err)
	complaint3.Signature = sig

	complaint3ConfigSeq1 := &state.Complaint{ShardTerm: state.ShardTerm{Shard: 1}, Signer: 3, ConfigSeq: 1}
	sig, err = signer3.Sign(complaint3ConfigSeq1.ToBeSigned())
	require.NoError(t, err)
	complaint3ConfigSeq1.Signature = sig

	dig := make([]byte, 32-3)

	dig123 := append([]byte{1, 2, 3}, dig...)
	baf123id1p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sks[0]), 1, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)
	baf123id2p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sks[1]), 2, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)
	baf123id1p1s1cs1, err := batcher.CreateBAF(crypto.ECDSASigner(*sks[0]), 1, 1, dig123, 1, 1, 1)
	assert.NoError(t, err)
	baf123id2p1s1cs1, err := batcher.CreateBAF(crypto.ECDSASigner(*sks[1]), 2, 1, dig123, 1, 1, 1)
	assert.NoError(t, err)

	dig124 := append([]byte{1, 2, 4}, dig...)
	baf124id3p1s2, err := batcher.CreateBAF(crypto.ECDSASigner(*sks[2]), 3, 1, dig124, 1, 2, 0)
	assert.NoError(t, err)
	baf124id4p1s2, err := batcher.CreateBAF(crypto.ECDSASigner(*sks[3]), 4, 1, dig124, 1, 2, 0)
	assert.NoError(t, err)

	dig125 := append([]byte{1, 2, 5}, dig...)
	baf125id1p1s3, err := batcher.CreateBAF(crypto.ECDSASigner(*sks[0]), 1, 1, dig125, 1, 3, 0)
	assert.NoError(t, err)

	for _, tst := range []struct {
		name                   string
		initialAppContext      *common.BlockHeader
		metadata               *smartbftprotos.ViewMetadata
		ces                    []state.ControlEvent
		bafsOfAvailableBatches []arma_types.BatchAttestationFragment
		numPending             int
		numComplaints          int
		newTermForShard1       uint64
		withConfig             bool
	}{
		{
			name: "single block",
			initialAppContext: &common.BlockHeader{
				Number:       0,
				PreviousHash: nil,
				DataHash:     nil,
			},
			metadata: &smartbftprotos.ViewMetadata{
				LatestSequence: 0,
			},
			ces:                    []state.ControlEvent{{BAF: baf123id1p1s1}, {BAF: baf123id2p1s1}},
			bafsOfAvailableBatches: []arma_types.BatchAttestationFragment{baf123id1p1s1},
			numPending:             0,
		},
		{
			name: "pending",
			initialAppContext: &common.BlockHeader{
				Number:       0,
				PreviousHash: nil,
				DataHash:     nil,
			},
			metadata: &smartbftprotos.ViewMetadata{
				LatestSequence: 0,
			},
			ces:        []state.ControlEvent{{BAF: baf123id1p1s1}},
			numPending: 1,
		},
		{
			name: "single block too many bafs",
			initialAppContext: &common.BlockHeader{
				Number:       0,
				PreviousHash: nil,
				DataHash:     nil,
			},
			metadata: &smartbftprotos.ViewMetadata{
				LatestSequence: 0,
			},
			ces:                    []state.ControlEvent{{BAF: baf123id1p1s1}, {BAF: baf123id2p1s1}, {BAF: baf123id1p1s1}, {BAF: baf123id2p1s1}, {BAF: baf123id2p1s1}},
			bafsOfAvailableBatches: []arma_types.BatchAttestationFragment{baf123id1p1s1},
			numPending:             0,
		},
		{
			name: "two blocks plus pending and one complaint",
			initialAppContext: &common.BlockHeader{
				Number:       0,
				PreviousHash: nil,
				DataHash:     nil,
			},
			metadata: &smartbftprotos.ViewMetadata{
				LatestSequence: 0,
			},
			ces:                    []state.ControlEvent{{BAF: baf123id1p1s1}, {BAF: baf123id2p1s1}, {BAF: baf124id3p1s2}, {BAF: baf124id4p1s2}, {BAF: baf125id1p1s3}, {Complaint: complaint2}},
			bafsOfAvailableBatches: []arma_types.BatchAttestationFragment{baf123id1p1s1, baf124id3p1s2},
			numPending:             1,
			numComplaints:          1,
		},
		{
			name: "block with different context and term change",
			initialAppContext: &common.BlockHeader{
				Number:       10,
				PreviousHash: append(make([]byte, 31), byte(10)),
				DataHash:     append(make([]byte, 31), byte(20)),
			},
			metadata: &smartbftprotos.ViewMetadata{
				LatestSequence: 5,
			},
			ces:                    []state.ControlEvent{{Complaint: complaint1}, {Complaint: complaint2}, {Complaint: complaint3}, {BAF: baf124id4p1s2}, {BAF: baf124id3p1s2}},
			bafsOfAvailableBatches: []arma_types.BatchAttestationFragment{baf124id3p1s2},
			numPending:             0,
			newTermForShard1:       1,
		},
		{
			name: "no blocks with two pending and one complaint",
			initialAppContext: &common.BlockHeader{
				Number:       10,
				PreviousHash: append(make([]byte, 31), byte(1)),
				DataHash:     append(make([]byte, 31), byte(2)),
			},
			metadata: &smartbftprotos.ViewMetadata{
				LatestSequence: 5,
			},
			ces:           []state.ControlEvent{{BAF: baf124id4p1s2}, {BAF: baf123id2p1s1}, {Complaint: complaint1}},
			numPending:    2,
			numComplaints: 1,
		},
		{
			name: "one config block",
			initialAppContext: &common.BlockHeader{
				Number:       10,
				PreviousHash: append(make([]byte, 31), byte(1)),
				DataHash:     append(make([]byte, 31), byte(2)),
			},
			metadata: &smartbftprotos.ViewMetadata{
				LatestSequence: 5,
			},
			ces:        []state.ControlEvent{{ConfigRequest: &state.ConfigRequest{Envelope: tx.CreateStructuredConfigUpdateEnvelope([]byte{1})}}},
			withConfig: true,
		},
		{
			name: "filtering mismatch config seq",
			initialAppContext: &common.BlockHeader{
				Number:       0,
				PreviousHash: nil,
				DataHash:     nil,
			},
			metadata: &smartbftprotos.ViewMetadata{
				LatestSequence: 0,
			},
			ces:           []state.ControlEvent{{BAF: baf123id1p1s1}, {BAF: baf123id2p1s1cs1}, {Complaint: complaint2}, {Complaint: complaint3ConfigSeq1}},
			numPending:    1,
			numComplaints: 1,
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			initialState := &state.State{
				ShardCount: 2,
				N:          4,
				Shards:     []state.ShardTerm{{Shard: 1}, {Shard: 2}},
				Threshold:  2,
				Quorum:     3,
				AppContext: protoutil.MarshalOrPanic(tst.initialAppContext),
				Pending:    []arma_types.BatchAttestationFragment{baf123id1p1s1cs1},
				Complaints: []state.Complaint{*complaint1ConfigSeq1},
			}

			consenter := &node_consensus.Consenter{
				DB:              db,
				Logger:          logger,
				BAFDeserializer: &state.BAFDeserialize{},
			}

			payloadBytes := []byte{1}
			configRequestEnvelope := tx.CreateStructuredConfigEnvelope(payloadBytes)
			configRequest := &protos.Request{
				Payload:   configRequestEnvelope.Payload,
				Signature: configRequestEnvelope.Signature,
			}
			mockConfigUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
			mockConfigUpdateProposer.ProposeConfigUpdateReturns(configRequest, nil)

			mockConfigRequestValidator := &configrequest_mocks.FakeConfigRequestValidator{}
			mockConfigRequestValidator.ValidateConfigRequestReturns(nil)

			mockConfigRulesVerifier := &ordererRulesMocks.FakeOrdererRules{}
			mockConfigRulesVerifier.ValidateNewConfigReturns(nil)

			mockConfigApplier := &consensus_mocks.FakeConfigApplier{}
			mockConfigApplier.ApplyConfigToStateCalls(func(s *state.State, request *state.ConfigRequest) (*state.State, error) {
				return s, nil
			})

			c := &node_consensus.Consensus{
				Arma:                   consenter,
				State:                  initialState,
				Logger:                 logger,
				SigVerifier:            verifier,
				RequestVerifier:        requestVerifier,
				Config:                 config,
				ConfigUpdateProposer:   mockConfigUpdateProposer,
				ConfigApplier:          mockConfigApplier,
				ConfigRequestValidator: mockConfigRequestValidator,
				ConfigRulesVerifier:    mockConfigRulesVerifier,
			}

			reqs := make([][]byte, len(tst.ces))
			for i, ce := range tst.ces {
				reqs[i] = ce.Bytes()
			}

			mBytes, err := proto.Marshal(tst.metadata)
			require.NoError(t, err)

			proposal := c.AssembleProposal(mBytes, reqs)
			require.NotNil(t, proposal)

			brs := arma_types.BatchedRequests(reqs)
			require.Equal(t, brs.Serialize(), proposal.Payload)

			header := &state.Header{}
			require.NoError(t, header.Deserialize(proposal.Header))

			require.Equal(t, tst.metadata.LatestSequence, uint64(header.Num))

			if !tst.withConfig {
				require.Len(t, header.AvailableCommonBlocks, len(tst.bafsOfAvailableBatches))
			} else {
				require.Len(t, header.AvailableCommonBlocks, len(tst.bafsOfAvailableBatches)+1)
			}

			for i, baf := range tst.bafsOfAvailableBatches {
				require.Equal(t, baf.Digest(), header.AvailableCommonBlocks[i].Header.DataHash)
			}

			latestBlockHeader := tst.initialAppContext
			latestBlockNumber := tst.initialAppContext.Number + 1
			latestBlockHash := protoutil.BlockHeaderHash(tst.initialAppContext)

			for i, baf := range tst.bafsOfAvailableBatches {
				latestBlockHeader = &common.BlockHeader{
					Number:       latestBlockNumber,
					PreviousHash: latestBlockHash,
					DataHash:     baf.Digest(),
				}

				require.Equal(t, latestBlockHeader, header.AvailableCommonBlocks[i].Header)

				latestBlockNumber++
				latestBlockHash = protoutil.BlockHeaderHash(latestBlockHeader)
			}

			if tst.withConfig {
				latestBlockHeader = &common.BlockHeader{
					Number:       latestBlockNumber,
					PreviousHash: latestBlockHash,
				}
				configReq, err := protoutil.Marshal(tst.ces[len(tst.ces)-1].ConfigRequest.Envelope)
				require.NoError(t, err)
				batchedConfigReq := arma_types.BatchedRequests([][]byte{configReq})
				latestBlockHeader.DataHash = batchedConfigReq.Digest()
			}

			require.NotNil(t, header.State)
			require.Len(t, header.State.Pending, tst.numPending)
			require.Len(t, header.State.Complaints, tst.numComplaints)
			require.Equal(t, tst.newTermForShard1, header.State.Shards[0].Term)

			require.Equal(t, protoutil.MarshalOrPanic(latestBlockHeader), header.State.AppContext)

			_, err = c.VerifyProposal(proposal)
			require.Nil(t, err)
		})
	}
}

func TestVerifyProposal(t *testing.T) {
	logger := testutil.CreateLogger(t, 1)

	dir, err := os.MkdirTemp("", strings.Replace(t.Name(), "/", "-", -1))
	assert.NoError(t, err)

	db, err := badb.NewBatchAttestationDB(dir, logger)
	assert.NoError(t, err)

	verifier := make(crypto.ECDSAVerifier)

	numOfParties := 4

	sks := make([]*ecdsa.PrivateKey, numOfParties)

	for i := 0; i < numOfParties; i++ {
		sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		assert.NoError(t, err)

		sks[i] = sk

		signer := crypto.ECDSASigner(*sk)
		for _, shard := range []arma_types.ShardID{1, 2, arma_types.ShardIDConsensus} {
			verifier[crypto.ShardPartyKey{Party: arma_types.PartyID(i + 1), Shard: shard}] = signer.PublicKey
		}
	}

	initialAppContext := &common.BlockHeader{
		Number:       10,
		PreviousHash: nil,
		DataHash:     nil,
	}

	initialState := state.State{
		ShardCount: 2,
		N:          4,
		Shards:     []state.ShardTerm{{Shard: 1}, {Shard: 2}},
		Threshold:  2,
		Quorum:     3,
		AppContext: protoutil.MarshalOrPanic(initialAppContext),
	}

	consenter := &node_consensus.Consenter{
		DB:              db,
		Logger:          logger,
		BAFDeserializer: &state.BAFDeserialize{},
	}

	bundle := &configMocks.FakeConfigResources{}
	configtxValidator := &policyMocks.FakeConfigtxValidator{}
	configtxValidator.ChannelIDReturns("arma")
	configtxValidator.SequenceReturns(0)
	bundle.ConfigtxValidatorReturns(configtxValidator)

	c := &node_consensus.Consensus{
		Arma:        consenter,
		State:       &initialState,
		Logger:      logger,
		SigVerifier: verifier,
		Config:      &nodeconfig.ConsenterNodeConfig{Bundle: bundle},
	}

	dig := make([]byte, 32-3)

	dig123 := append([]byte{1, 2, 3}, dig...)
	baf123id1p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sks[0]), 1, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)
	baf123id2p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sks[1]), 2, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)

	ces := []state.ControlEvent{{BAF: baf123id1p1s1}, {BAF: baf123id2p1s1}}
	reqs := make([][]byte, len(ces))
	for i, ce := range ces {
		reqs[i] = ce.Bytes()
	}
	brs := arma_types.BatchedRequests(reqs)

	header := state.Header{}
	header.Num = 0

	latestBlockHeader := &common.BlockHeader{}
	latestBlockHeader.Number = initialAppContext.Number + 1
	latestBlockHeader.DataHash = baf123id1p1s1.Digest()
	latestBlockHeader.PreviousHash = protoutil.BlockHeaderHash(initialAppContext)

	ab := state.NewAvailableBatch(baf123id1p1s1.Primary(), baf123id1p1s1.Shard(), baf123id1p1s1.Seq(), baf123id1p1s1.Digest())

	header.AvailableCommonBlocks = []*common.Block{{Header: latestBlockHeader}}

	protoutil.InitBlockMetadata(header.AvailableCommonBlocks[0])
	blockMetadata, err := ledger.AssemblerBlockMetadataToBytes(ab, &state.OrderingInformation{DecisionNum: 0, BatchCount: 1, BatchIndex: 0}, 0)
	require.Nil(t, err)
	header.AvailableCommonBlocks[0].Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = blockMetadata

	newState := initialState
	newState.AppContext = protoutil.MarshalOrPanic(latestBlockHeader)

	header.State = &newState

	metadata := &smartbftprotos.ViewMetadata{
		LatestSequence: 0,
	}

	mBytes, err := proto.Marshal(metadata)
	require.NoError(t, err)

	// 1. no error
	t.Log("no error")

	proposal := smartbft_types.Proposal{
		Header:   header.Serialize(),
		Payload:  brs.Serialize(),
		Metadata: mBytes,
	}

	infos, err := c.VerifyProposal(proposal)
	require.Nil(t, err)
	require.NotNil(t, infos)
	require.Equal(t, len(brs), len(infos))
	require.Equal(t, c.RequestID(brs[0]), infos[0])
	require.Equal(t, c.RequestID(brs[1]), infos[1])

	// 2. nil header
	t.Log("nil header")
	verifyProposalRequireError(t, c, nil, brs.Serialize(), mBytes)

	// 3. nil metadata
	t.Log("nil metadata")
	verifyProposalRequireError(t, c, header.Serialize(), brs.Serialize(), nil)

	// 4. nil payload
	t.Log("nil payload")
	verifyProposalRequireError(t, c, header.Serialize(), nil, mBytes)

	// 5. mismatch metadata latest sequence and header number
	t.Log("mismatch metadata latest sequence and header number")
	header1 := header
	header1.Num = 1
	verifyProposalRequireError(t, c, header1.Serialize(), brs.Serialize(), mBytes)

	// 6. mismatch state config in header
	t.Log("mismatch state config in header")
	headerState := header
	badState := newState
	headerState.State = &badState
	headerState.State.Quorum = 10
	verifyProposalRequireError(t, c, headerState.Serialize(), brs.Serialize(), mBytes)
	headerState.State.Quorum = header.State.Quorum
	headerState.State.Threshold = 10
	verifyProposalRequireError(t, c, headerState.Serialize(), brs.Serialize(), mBytes)
	headerState.State.Threshold = header.State.Threshold
	headerState.State.N = 10
	verifyProposalRequireError(t, c, headerState.Serialize(), brs.Serialize(), mBytes)

	// 7. mismatch state pending in header
	t.Log("mismatch state pending in header")
	headerPending := header
	badState = newState
	headerPending.State = &badState
	headerPending.State.Pending = []arma_types.BatchAttestationFragment{baf123id1p1s1}
	verifyProposalRequireError(t, c, headerPending.Serialize(), brs.Serialize(), mBytes)

	// 8. mismatch state app context in header
	t.Log("mismatch state app context in header")
	headerAppContext := header
	headerAppContext.State = &newState
	badAppContextNumber := &common.BlockHeader{
		Number:       100,
		DataHash:     baf123id1p1s1.Digest(),
		PreviousHash: protoutil.BlockHeaderHash(initialAppContext),
	}
	headerAppContext.State.AppContext = protoutil.MarshalOrPanic(badAppContextNumber)
	verifyProposalRequireError(t, c, headerAppContext.Serialize(), brs.Serialize(), mBytes)
	badAppContextDataHash := &common.BlockHeader{
		Number:       11,
		DataHash:     []byte{9},
		PreviousHash: protoutil.BlockHeaderHash(initialAppContext),
	}
	headerAppContext.State.AppContext = protoutil.MarshalOrPanic(badAppContextDataHash)
	verifyProposalRequireError(t, c, headerAppContext.Serialize(), brs.Serialize(), mBytes)
	badAppContextPrevHash := &common.BlockHeader{
		Number:       11,
		DataHash:     baf123id1p1s1.Digest(),
		PreviousHash: []byte{9},
	}
	headerAppContext.State.AppContext = protoutil.MarshalOrPanic(badAppContextPrevHash)
	verifyProposalRequireError(t, c, headerAppContext.Serialize(), brs.Serialize(), mBytes)

	// 9. mismatch available batch in header
	t.Log("mismatch available batch in header")
	headerAB := header
	headerAB.AvailableCommonBlocks = []*common.Block{{Header: latestBlockHeader}}
	verifyProposalRequireError(t, c, headerAB.Serialize(), brs.Serialize(), mBytes)

	// 10. mismatch block header in header
	t.Log("mismatch block header in header")
	headerBH := header
	badBH := latestBlockHeader
	badBH.PreviousHash[0]++
	headerBH.AvailableCommonBlocks = []*common.Block{{Header: badBH}}
	verifyProposalRequireError(t, c, headerBH.Serialize(), brs.Serialize(), mBytes)

	// 11. mismatch verification sequence
	t.Log("mismatch verification sequence")
	proposalV := proposal
	proposalV.VerificationSequence = 1
	_, err = c.VerifyProposal(proposalV)
	require.Error(t, err)
	require.ErrorContains(t, err, "expected verification sequence")
}

func verifyProposalRequireError(t *testing.T, c *node_consensus.Consensus, header, payload, metadata []byte) {
	proposal := smartbft_types.Proposal{
		Header:   header,
		Payload:  payload,
		Metadata: metadata,
	}

	infos, err := c.VerifyProposal(proposal)
	require.NotNil(t, err)
	require.Nil(t, infos)
	t.Logf("err: %v", err)
}

func TestSignProposal(t *testing.T) {
	logger := testutil.CreateLogger(t, 1)

	dir, err := os.MkdirTemp("", strings.Replace(t.Name(), "/", "-", -1))
	assert.NoError(t, err)

	db, err := badb.NewBatchAttestationDB(dir, logger)
	assert.NoError(t, err)

	verifier := make(crypto.ECDSAVerifier)

	numOfParties := 4

	sks := make([]*ecdsa.PrivateKey, numOfParties)

	for i := 0; i < numOfParties; i++ {
		sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		assert.NoError(t, err)

		sks[i] = sk

		signer := crypto.ECDSASigner(*sk)
		for _, shard := range []arma_types.ShardID{1, 2, arma_types.ShardIDConsensus} {
			verifier[crypto.ShardPartyKey{Party: arma_types.PartyID(i + 1), Shard: shard}] = signer.PublicKey
		}
	}

	initialAppContext := &common.BlockHeader{
		Number:       10,
		PreviousHash: nil,
		DataHash:     nil,
	}

	initialState := state.State{
		ShardCount: 2,
		N:          4,
		Shards:     []state.ShardTerm{{Shard: 1}, {Shard: 2}},
		Threshold:  2,
		Quorum:     3,
		AppContext: protoutil.MarshalOrPanic(initialAppContext),
	}

	consenter := &node_consensus.Consenter{
		DB:              db,
		Logger:          logger,
		BAFDeserializer: &state.BAFDeserialize{},
	}

	c := &node_consensus.Consensus{
		BFTConfig:   smartbft_types.Configuration{SelfID: 1},
		Arma:        consenter,
		State:       &initialState,
		Logger:      logger,
		SigVerifier: verifier,
		Signer:      crypto.ECDSASigner(*sks[0]),
	}

	proposal := smartbft_types.Proposal{}

	require.Panics(t, func() {
		c.SignProposal(proposal, nil)
	})

	dig := make([]byte, 32-3)

	dig123 := append([]byte{1, 2, 3}, dig...)
	baf123id1p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sks[0]), 1, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)
	baf123id2p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sks[1]), 2, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)

	ces := []state.ControlEvent{{BAF: baf123id1p1s1}, {BAF: baf123id2p1s1}}
	reqs := make([][]byte, len(ces))
	for i, ce := range ces {
		reqs[i] = ce.Bytes()
	}
	brs := arma_types.BatchedRequests(reqs)

	proposal.Payload = brs.Serialize()

	require.Panics(t, func() {
		c.SignProposal(proposal, nil)
	})

	header := state.Header{}
	header.Num = 0

	latestBlockHeader := initialAppContext
	latestBlockHeader.Number += 1
	latestBlockHeader.DataHash = baf123id1p1s1.Digest()
	latestBlockHeader.PreviousHash = protoutil.BlockHeaderHash(initialAppContext)

	newState := initialState
	newState.AppContext = protoutil.MarshalOrPanic(latestBlockHeader)

	header.State = &newState

	proposal.Header = header.Serialize()

	sig := c.SignProposal(proposal, nil)

	require.NotNil(t, sig)

	_, err = c.VerifyConsenterSig(*sig, proposal)
	require.NoError(t, err)
}

func TestConsensusStartStop(t *testing.T) {
	dir := t.TempDir()

	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)

	verifier := make(crypto.ECDSAVerifier)

	initialAppContext := &common.BlockHeader{
		Number:       1,
		PreviousHash: nil,
		DataHash:     nil,
	}

	initialState := &state.State{
		ShardCount: 2,
		N:          1,
		Shards:     []state.ShardTerm{{Shard: 1}, {Shard: 2}},
		Threshold:  1,
		Quorum:     1,
		AppContext: protoutil.MarshalOrPanic(initialAppContext),
	}

	nodeIDs := []uint64{1}

	commitEvent := new(sync.WaitGroup)
	onCommit := func() {
		commitEvent.Done()
	}

	network := make(map[uint64]*node_consensus.Consensus)

	c, cleanup := makeConsensusNode(t, sk, arma_types.PartyID(1), network, initialState, nodeIDs, verifier, dir)
	defer cleanup()

	listener := &storageListener{c: make(chan *common.Block, 100), f: onCommit}
	c.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(listener)

	err = c.Start()
	assert.NoError(t, err)

	// 1. Valid request
	commitEvent.Add(1)
	dig := make([]byte, 32-3)
	dig123 := append([]byte{1, 2, 3}, dig...)
	baf123id1p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sk), 1, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)

	ce1 := &state.ControlEvent{BAF: baf123id1p1s1}
	err = c.SubmitRequest(ce1.Bytes())
	assert.NoError(t, err)
	commitEvent.Wait()

	b := <-listener.c
	assert.Equal(t, uint64(1), b.Header.Number)

	decision, _, err := state.BytesToDecision(b.Data.Data[0])
	assert.NoError(t, err)

	hdr := &state.Header{}
	err = hdr.Deserialize(decision.Header)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(hdr.AvailableCommonBlocks))
	c.Stop()

	c, cleanup = makeConsensusNode(t, sk, arma_types.PartyID(1), network, initialState, nodeIDs, verifier, dir)
	defer cleanup()

	c.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(listener)

	err = c.Start()
	assert.NoError(t, err)

	// 2. Verify handling duplicates after recovery node
	commitEvent.Add(1)
	err = c.SubmitRequest(ce1.Bytes())
	assert.NoError(t, err)
	commitEvent.Wait()

	b = <-listener.c
	assert.Equal(t, uint64(2), b.Header.Number)

	decision, _, err = state.BytesToDecision(b.Data.Data[0])
	assert.NoError(t, err)

	err = hdr.Deserialize(decision.Header)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(hdr.AvailableCommonBlocks))

	digests, _ := c.BADB.List()
	assert.Equal(t, 1, len(digests))
	assert.Contains(t, digests, dig123)

	c.Stop()

	c, cleanup = makeConsensusNode(t, sk, arma_types.PartyID(1), network, c.State, nodeIDs, verifier, dir)
	defer cleanup()

	c.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(listener)

	err = c.Start()
	assert.NoError(t, err)

	// 3. Valid request after recovery node
	dig124 := append([]byte{1, 2, 4}, dig...)
	baf124id1p1s2, err := batcher.CreateBAF(crypto.ECDSASigner(*sk), 1, 1, dig124, 1, 2, 0)
	assert.NoError(t, err)
	ce2 := &state.ControlEvent{BAF: baf124id1p1s2}
	commitEvent.Add(1)
	err = c.SubmitRequest(ce2.Bytes())
	assert.NoError(t, err)
	commitEvent.Wait()

	b = <-listener.c
	assert.Equal(t, uint64(3), b.Header.Number)

	decision, _, err = state.BytesToDecision(b.Data.Data[0])
	assert.NoError(t, err)

	err = hdr.Deserialize(decision.Header)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(hdr.AvailableCommonBlocks))

	// 4. Verify duplicate dig123 is handled correctly by BADB
	commitEvent.Add(1)
	err = c.SubmitRequest(ce1.Bytes())
	assert.NoError(t, err)
	commitEvent.Wait()

	digests, _ = c.BADB.List()
	assert.Equal(t, 2, len(digests))
	assert.Contains(t, digests, dig123)
	assert.Contains(t, digests, dig124)

	b = <-listener.c
	assert.Equal(t, uint64(4), b.Header.Number)

	decision, _, err = state.BytesToDecision(b.Data.Data[0])
	assert.NoError(t, err)

	err = hdr.Deserialize(decision.Header)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(hdr.AvailableCommonBlocks))
}

func TestCreateAndVerifyDataCommonBlock(t *testing.T) {
	for _, tst := range []struct {
		name            string
		err             string
		blockNum        uint64
		prevHash        []byte
		primary         arma_types.PartyID
		shard           arma_types.ShardID
		seq             arma_types.BatchSequence
		digest          []byte
		decisionNum     arma_types.DecisionNum
		batchCount      int
		batchIndex      int
		lastConfigBlock uint64
	}{
		{
			name: "no error",
		},
		{
			name:     "wrong block number",
			err:      "proposed block header number",
			blockNum: 1,
		},
		{
			name:     "wrong block number",
			err:      "proposed block header prev hash",
			prevHash: []byte{1},
		},
		{
			name:    "wrong primary",
			err:     "proposed block metadata",
			primary: 1,
		},
		{
			name:  "wrong shard",
			err:   "proposed block metadata",
			shard: 1,
		},
		{
			name: "wrong seq",
			err:  "proposed block metadata",
			seq:  1,
		},
		{
			name:   "wrong digest",
			err:    "proposed block data hash",
			digest: []byte{1},
		},
		{
			name:        "wrong decision number",
			err:         "proposed block metadata",
			decisionNum: 1,
		},
		{
			name:       "wrong batch count",
			err:        "proposed block metadata",
			batchCount: 1,
		},
		{
			name:       "wrong batch index",
			err:        "proposed block metadata",
			batchIndex: 1,
		},
		{
			name:            "wrong last config block",
			err:             "last config in block",
			lastConfigBlock: 1,
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			block, err := node_consensus.CreateDataCommonBlock(0, nil, state.NewAvailableBatch(0, 0, 0, nil), 0, 0, 0, 0)
			require.NoError(t, err)
			require.NotNil(t, block)
			err = node_consensus.VerifyDataCommonBlock(block, tst.blockNum, tst.prevHash, state.NewAvailableBatch(tst.primary, tst.shard, tst.seq, tst.digest), tst.decisionNum, tst.batchCount, tst.batchIndex, tst.lastConfigBlock)
			if tst.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tst.err)
			}

			block, err = node_consensus.CreateDataCommonBlock(tst.blockNum, tst.prevHash, state.NewAvailableBatch(tst.primary, tst.shard, tst.seq, tst.digest), tst.decisionNum, tst.batchCount, tst.batchIndex, tst.lastConfigBlock)
			require.NoError(t, err)
			require.NotNil(t, block)
			err = node_consensus.VerifyDataCommonBlock(block, 0, nil, state.NewAvailableBatch(0, 0, 0, nil), 0, 0, 0, 0)
			if tst.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tst.err)
			}
		})
	}
}

func TestCreateAndVerifyConfigCommonBlock(t *testing.T) {
	for _, tst := range []struct {
		name        string
		err         string
		blockNum    uint64
		prevHash    []byte
		dataHash    []byte
		decisionNum arma_types.DecisionNum
		batchCount  int
		batchIndex  int
	}{
		{
			name: "no error",
		},
		{
			name:     "wrong block number",
			err:      "proposed config block header number",
			blockNum: 1,
		},
		{
			name:     "wrong block number",
			err:      "proposed config block header prev hash",
			prevHash: []byte{1},
		},
		{
			name:     "wrong data hash",
			err:      "proposed config block data hash",
			dataHash: []byte{1},
		},
		{
			name:        "wrong decision number",
			err:         "proposed config block metadata",
			decisionNum: 1,
		},
		{
			name:       "wrong batch count",
			err:        "proposed config block metadata",
			batchCount: 1,
		},
		{
			name:       "wrong batch index",
			err:        "proposed config block metadata",
			batchIndex: 1,
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			configBlock, err := node_consensus.CreateConfigCommonBlock(0, nil, 0, 0, 0, nil)
			require.NoError(t, err)
			require.NotNil(t, configBlock)

			nilConfigReq := arma_types.BatchedRequests([][]byte{nil})
			dataHash := nilConfigReq.Digest()
			if tst.dataHash != nil {
				dataHash = tst.dataHash
			}

			err = node_consensus.VerifyConfigCommonBlock(configBlock, tst.blockNum, tst.prevHash, dataHash, tst.decisionNum, tst.batchCount, tst.batchIndex)
			if tst.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tst.err)
			}

			var configReq []byte
			if tst.dataHash != nil {
				configReq = tst.dataHash
			}

			configBlock, err = node_consensus.CreateConfigCommonBlock(tst.blockNum, tst.prevHash, tst.decisionNum, tst.batchCount, tst.batchIndex, configReq)
			require.NoError(t, err)
			require.NotNil(t, configBlock)

			err = node_consensus.VerifyConfigCommonBlock(configBlock, 0, nil, nilConfigReq.Digest(), 0, 0, 0)
			if tst.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tst.err)
			}
		})
	}
}

func TestConsensusSoftStop(t *testing.T) {
	dir := t.TempDir()

	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)

	verifier := make(crypto.ECDSAVerifier)

	initialAppContext := &common.BlockHeader{
		Number:       1,
		PreviousHash: nil,
		DataHash:     nil,
	}

	initialState := &state.State{
		ShardCount: 1,
		N:          1,
		Shards:     []state.ShardTerm{{Shard: 1}},
		Threshold:  1,
		Quorum:     1,
		AppContext: protoutil.MarshalOrPanic(initialAppContext),
	}

	nodeIDs := []uint64{1}

	commitEvent := new(sync.WaitGroup)
	onCommit := func() {
		commitEvent.Done()
	}

	network := make(map[uint64]*node_consensus.Consensus)

	c, cleanup := makeConsensusNode(t, sk, arma_types.PartyID(1), network, initialState, nodeIDs, verifier, dir)
	defer cleanup()

	listener := &storageListener{c: make(chan *common.Block, 100), f: onCommit}
	c.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(listener)

	err = c.Start()
	assert.NoError(t, err)

	// submit request and verify block is committed
	commitEvent.Add(1)
	dig := make([]byte, 32-3)
	dig123 := append([]byte{1, 2, 3}, dig...)
	baf123id1p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sk), 1, 1, dig123, 1, 1, 0)
	assert.NoError(t, err)

	ce1 := &state.ControlEvent{BAF: baf123id1p1s1}
	err = c.SubmitRequest(ce1.Bytes())
	assert.NoError(t, err)
	commitEvent.Wait()

	b := <-listener.c
	assert.Equal(t, uint64(1), b.Header.Number)

	decision, _, err := state.BytesToDecision(b.Data.Data[0])
	assert.NoError(t, err)

	hdr := &state.Header{}
	err = hdr.Deserialize(decision.Header)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(hdr.AvailableCommonBlocks))

	// perform soft stop
	c.SoftStop()

	// submitting new requests should now fail
	dig124 := append([]byte{1, 2, 4}, dig...)
	baf124id1p1s1, err := batcher.CreateBAF(crypto.ECDSASigner(*sk), 1, 1, dig124, 1, 2, 0)
	assert.NoError(t, err)

	ce2 := &state.ControlEvent{BAF: baf124id1p1s1}
	err = c.SubmitRequest(ce2.Bytes())
	assert.Error(t, err)
}
