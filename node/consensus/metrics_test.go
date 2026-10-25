/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"testing"

	smartbft_types "github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	"github.com/hyperledger/fabric-x-orderer/common/operations"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// Scenario:
// 1. create a consensus ledger holding a genesis decision
// 2. deliver one more decision through Deliver
// 3. read back the block the ledger stored for that decision
// 4. check the decisions size counter grew by exactly the size of that block
func TestDeliverCountsDecisionSize(t *testing.T) {
	logger := testutil.CreateLogger(t, 1)

	consensusLedger, err := ledger.NewConsensusLedger(t.TempDir())
	require.NoError(t, err)
	defer consensusLedger.Close()

	genesisProposal := smartbft_types.Proposal{
		Header: (&state.Header{
			AvailableCommonBlocks: []*common.Block{{Header: &common.BlockHeader{Number: 0}}},
			State:                 &state.State{N: 1},
			Num:                   0,
		}).Serialize(),
	}
	genesisBlock := state.CreateBlockToAppendFromDecision(0, genesisProposal, nil, nil, 0)
	consensusLedger.Append(genesisBlock)

	consenterNodeConfig := &nodeconfig.ConsenterNodeConfig{
		Metrics: &operations.Metrics{Provider: "prometheus"},
	}
	c := &Consensus{
		Config:   consenterNodeConfig,
		Logger:   logger,
		Storage:  consensusLedger,
		Arma:     &Consenter{Logger: logger},
		PrevHash: protoutil.BlockHeaderHash(genesisBlock.Header),
		Metrics:  NewConsensusMetrics(consenterNodeConfig, consensusLedger.Height(), 0, logger),
	}

	decisionsSize := func() uint64 {
		return uint64(monitoring.GetMetricValue(c.Metrics.decisionsBytes.(prometheus.Metric), logger))
	}
	require.Equal(t, uint64(0), decisionsSize())

	proposal := smartbft_types.Proposal{
		Header: (&state.Header{
			State:                        &state.State{N: 1},
			Num:                          1,
			DecisionNumOfLastConfigBlock: 0,
		}).Serialize(),
		Payload: []byte("decision payload"),
	}
	c.Deliver(proposal, nil)

	block, err := consensusLedger.RetrieveBlockByNumber(1)
	require.NoError(t, err)
	require.Equal(t, uint64(proto.Size(block)), decisionsSize())
}
