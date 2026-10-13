/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	node_consensus "github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// fakeEventStream is a NotifyEvent server stream that delivers the given events and then io.EOF.
type fakeEventStream struct {
	grpc.ServerStream
	ctx    context.Context
	events [][]byte
}

func (s *fakeEventStream) Context() context.Context {
	return s.ctx
}

func (s *fakeEventStream) Recv() (*protos.Event, error) {
	if len(s.events) == 0 {
		return nil, io.EOF
	}
	payload := s.events[0]
	s.events = s.events[1:]
	return &protos.Event{Payload: payload}, nil
}

func (s *fakeEventStream) Send(*protos.EventResponse) error {
	return nil
}

// TestNotifyEventValidatesSender checks that NotifyEvent accepts an event only over the TLS connection of
// the node that created it, and closes the stream otherwise. The consenter has no keys registered, so an
// accepted event fails signature verification in SubmitRequest, which only logs it; NotifyEvent then reads
// io.EOF and returns nil. A rejected event makes NotifyEvent return an error.
func TestNotifyEventValidatesSender(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	newCert := func(t *testing.T) []byte {
		kp, err := ca.NewClientCertKeyPair()
		require.NoError(t, err)
		return kp.Cert
	}
	batcher1Shard1 := newCert(t)
	batcher2Shard1 := newCert(t)
	batcher1Shard2 := newCert(t)
	assembler1 := newCert(t)
	assembler2 := newCert(t)

	consenterConfig := func(batcher1Shard1Cert []byte) *nodeconfig.ConsenterNodeConfig {
		bundle := &configMocks.FakeConfigResources{}
		bundle.ConfigtxValidatorReturns(&policyMocks.FakeConfigtxValidator{})
		return &nodeconfig.ConsenterNodeConfig{
			Bundle: bundle,
			Shards: []nodeconfig.ShardInfo{
				{ShardId: 1, Batchers: []nodeconfig.BatcherInfo{
					{PartyID: 1, TLSCert: batcher1Shard1Cert},
					{PartyID: 2, TLSCert: batcher2Shard1},
				}},
				{ShardId: 2, Batchers: []nodeconfig.BatcherInfo{
					{PartyID: 1, TLSCert: batcher1Shard2},
				}},
			},
			Assemblers: []nodeconfig.AssemblerInfo{
				{PartyID: 1, TLSCert: assembler1},
				{PartyID: 2, TLSCert: assembler2},
			},
		}
	}

	c := &node_consensus.Consensus{
		Logger:      testutil.CreateLogger(t, 1),
		SigVerifier: make(crypto.ECDSAVerifier),
		Config:      consenterConfig(batcher1Shard1),
	}

	ctxOf := func(t *testing.T, pemCert []byte) context.Context {
		cert, err := utils.Parsex509Cert(pemCert)
		require.NoError(t, err)
		ctx, err := createContextForSubmitConfig(cert)
		require.NoError(t, err)
		return ctx
	}

	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	bafOf := func(t *testing.T, signer arma_types.PartyID, primary arma_types.PartyID, shard arma_types.ShardID) []byte {
		baf, err := batcher.CreateBAF(crypto.ECDSASigner(*sk), signer, shard, digest123, primary, 1, 0, 1, []byte{1})
		require.NoError(t, err)
		return (&state.ControlEvent{BAF: baf}).Bytes()
	}
	complaintOf := func(signer arma_types.PartyID, shard arma_types.ShardID) []byte {
		complaint := &state.Complaint{ShardTerm: state.ShardTerm{Shard: shard, Term: 1}, Signer: signer, Signature: []byte{1}}
		return (&state.ControlEvent{Complaint: complaint}).Bytes()
	}
	reportOf := func(party arma_types.PartyID) []byte {
		report := &state.AssemblerDecisionReport{Party: party, DecisionNum: 1, Signature: []byte{1}}
		return (&state.ControlEvent{AssemblerReport: report}).Bytes()
	}
	configRequest := (&state.ControlEvent{ConfigRequest: &state.ConfigRequest{Envelope: &common.Envelope{Payload: []byte{1}}}}).Bytes()

	const notTheCreator = "the event does not come from the node that created it"

	for _, tst := range []struct {
		name         string
		ctx          context.Context
		events       [][]byte
		expectErr    string
		expectUnread int // events left unread when the stream is closed
	}{
		{
			name:   "accepts a BAF from the primary that signed it",
			ctx:    ctxOf(t, batcher1Shard1),
			events: [][]byte{bafOf(t, 1, 1, 1)},
		},
		{
			name:   "accepts a BAF from the secondary that signed it",
			ctx:    ctxOf(t, batcher2Shard1),
			events: [][]byte{bafOf(t, 2, 1, 1)},
		},
		{
			name:   "accepts a complaint from the batcher that signed it",
			ctx:    ctxOf(t, batcher2Shard1),
			events: [][]byte{complaintOf(2, 1)},
		},
		{
			name:   "accepts a report from the assembler of its party",
			ctx:    ctxOf(t, assembler2),
			events: [][]byte{reportOf(2)},
		},
		{
			name:      "rejects a BAF from the batcher of another party",
			ctx:       ctxOf(t, batcher2Shard1),
			events:    [][]byte{bafOf(t, 1, 1, 1)},
			expectErr: notTheCreator,
		},
		{
			name:      "rejects a BAF from the signer's batcher of another shard",
			ctx:       ctxOf(t, batcher1Shard2),
			events:    [][]byte{bafOf(t, 1, 1, 1)},
			expectErr: notTheCreator,
		},
		{
			name:      "rejects a BAF from the signer's assembler",
			ctx:       ctxOf(t, assembler1),
			events:    [][]byte{bafOf(t, 1, 1, 1)},
			expectErr: notTheCreator,
		},
		{
			name:      "rejects a BAF of a party with no batcher in the shard",
			ctx:       ctxOf(t, batcher1Shard1),
			events:    [][]byte{bafOf(t, 3, 3, 1)},
			expectErr: "no batcher of party 3 in shard 1",
		},
		{
			name:      "rejects a complaint from the batcher of another party",
			ctx:       ctxOf(t, batcher1Shard1),
			events:    [][]byte{complaintOf(2, 1)},
			expectErr: notTheCreator,
		},
		{
			name:      "rejects a report from the assembler of another party",
			ctx:       ctxOf(t, assembler1),
			events:    [][]byte{reportOf(2)},
			expectErr: notTheCreator,
		},
		{
			name:      "rejects a report from a batcher of its party",
			ctx:       ctxOf(t, batcher1Shard1),
			events:    [][]byte{reportOf(1)},
			expectErr: notTheCreator,
		},
		{
			name:      "rejects a report of a party with no assembler",
			ctx:       ctxOf(t, assembler1),
			events:    [][]byte{reportOf(3)},
			expectErr: "no assembler of party 3",
		},
		{
			name:      "rejects a config request",
			ctx:       ctxOf(t, batcher1Shard1),
			events:    [][]byte{configRequest},
			expectErr: "only BAFs, complaints and assembler decision reports can be submitted as events",
		},
		{
			name:      "rejects a malformed event",
			ctx:       ctxOf(t, batcher1Shard1),
			events:    [][]byte{{0xff, 0xff}},
			expectErr: "malformed event",
		},
		{
			name:      "rejects an event over a connection with no client certificate",
			ctx:       context.Background(),
			events:    [][]byte{bafOf(t, 1, 1, 1)},
			expectErr: "could not extract the client certificate",
		},
		{
			name:         "closes the stream at the first rejected event",
			ctx:          ctxOf(t, batcher1Shard1),
			events:       [][]byte{bafOf(t, 1, 1, 1), complaintOf(2, 1), bafOf(t, 1, 1, 1)},
			expectErr:    notTheCreator,
			expectUnread: 1,
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			stream := &fakeEventStream{ctx: tst.ctx, events: tst.events}
			err := c.NotifyEvent(stream)
			if tst.expectErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tst.expectErr)
			require.Len(t, stream.events, tst.expectUnread)
		})
	}

	t.Run("checks the sender against the current config", func(t *testing.T) {
		rotatedBatcher1Shard1 := newCert(t)
		c.Config = consenterConfig(rotatedBatcher1Shard1)
		defer func() { c.Config = consenterConfig(batcher1Shard1) }()

		err := c.NotifyEvent(&fakeEventStream{ctx: ctxOf(t, batcher1Shard1), events: [][]byte{bafOf(t, 1, 1, 1)}})
		require.ErrorContains(t, err, notTheCreator)

		err = c.NotifyEvent(&fakeEventStream{ctx: ctxOf(t, rotatedBatcher1Shard1), events: [][]byte{bafOf(t, 1, 1, 1)}})
		require.NoError(t, err)
	})
}

// TestNotifyEventOrdersBAFFromItsBatcher checks that a BAF sent by a batcher over its own NotifyEvent stream
// passes the sender check and is ordered.
func TestNotifyEventOrdersBAFFromItsBatcher(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	setup := setupConsensusTest(t, ca, 1, utils.EmptyGenesisBlock("arma"))
	defer setup.consensusNodes[0].Stop()

	batcherNode := setup.batcherNodes[0]
	consenterInfo := setup.consentersInfo[0]
	sender := batcher.CreateConsenterControlEventStream(batcherNode.TLSKey, batcherNode.TLSCert, consenterInfo)

	baf, err := batcher.CreateBAF(crypto.ECDSASigner(*batcherNode.sk), 1, 1, digest123, 1, 1, 0, 1, nil)
	require.NoError(t, err)
	require.NoError(t, sender.SendControlEvent(state.ControlEvent{BAF: baf}))

	select {
	case b := <-setup.listeners[0].c:
		require.Equal(t, uint64(1), b.Header.Number)
	case <-time.After(30 * time.Second):
		t.Fatal("the BAF was not ordered")
	}
}
