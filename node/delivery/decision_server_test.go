/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package delivery_test

import (
	"testing"

	smartbft_types "github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

// decisionServer serves the consensus deliver service over TLS, over a decision ledger, on an endpoint
// that stays the same across a restart.
type decisionServer struct {
	endpoint       string
	ledger         *node_ledger.ConsensusLedger
	deliverService orderer.AtomicBroadcastServer
	serverKeyPair  *tlsgen.CertKeyPair
	server         *comm.GRPCServer
}

func serveDecisionsOverTLS(t *testing.T, channelID string, bundle *mocks.FakeConfigResources, ca tlsgen.CA) *decisionServer {
	consensusLedger, err := node_ledger.NewConsensusLedger(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(consensusLedger.Close)

	deliverService, err := delivery.NewDeliverService(channelID, consensusLedger, bundle)
	require.NoError(t, err)

	serverKeyPair, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	ds := &decisionServer{
		endpoint:       testutil.AllocateLocalhostAddress(t),
		ledger:         consensusLedger,
		deliverService: deliverService,
		serverKeyPair:  serverKeyPair,
	}
	ds.start(t)
	t.Cleanup(func() { ds.server.Stop() })

	return ds
}

// appendDecisions appends count decisions to the ledger, block i holding decision i. Block 0 is filler:
// a header of all zero values serializes to no bytes, which a replicator cannot read back, so the
// replicators in these tests start from decision 1.
func (ds *decisionServer) appendDecisions(t *testing.T, count int) {
	for range count {
		var prevHash []byte
		height := ds.ledger.Height()
		if height > 0 {
			prev, err := ds.ledger.RetrieveBlockByNumber(height - 1)
			require.NoError(t, err)
			prevHash = protoutil.BlockHeaderHash(prev.Header)
		}
		header := &state.Header{Num: types.DecisionNum(height)}
		ds.ledger.Append(state.CreateBlockToAppendFromDecision(height, smartbft_types.Proposal{Header: header.Serialize()}, nil, prevHash, 0))
	}
}

func (ds *decisionServer) start(t *testing.T) {
	server, err := comm.NewGRPCServer(ds.endpoint, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:      true,
			Certificate: ds.serverKeyPair.Cert,
			Key:         ds.serverKeyPair.Key,
		},
	})
	require.NoError(t, err)

	orderer.RegisterAtomicBroadcastServer(server.Server(), ds.deliverService)
	ds.server = server

	go func() {
		_ = server.Start()
	}()
}

func (ds *decisionServer) restart(t *testing.T) {
	ds.server.Stop()
	ds.start(t)
}
