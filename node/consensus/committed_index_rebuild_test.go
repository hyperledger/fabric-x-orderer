/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"testing"

	smartbft_types "github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-x-common/protoutil"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/committedindex"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestRebuildCommittedIndex commits decisions through a live index while the prune points advance, writes
// them to a consenter ledger, and rebuilds a fresh index from the last state: the rebuilt index must hold
// the same batches and answer the same as the live one.
func TestRebuildCommittedIndex(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	l, err := ledger.NewConsensusLedger(t.TempDir())
	require.NoError(t, err)
	defer l.Close()

	live := &Consenter{Logger: logger}
	var prevHash []byte
	var last *state.State
	const decisions = 50
	for n := arma_types.DecisionNum(0); n < decisions; n++ {
		st := &state.State{}
		if n > 10 {
			st.SetPrunePoint(1, 1, arma_types.BatchSequence(n-10))
		}
		if n > 20 {
			st.SetPrunePoint(1, 2, arma_types.BatchSequence(n-20))
		}
		st.ReplayFrom = live.Committed.ReplayFrom()
		hdr := &state.Header{Num: n, DecisionNumOfLastConfigBlock: 0, State: st}
		if n > 0 && n%7 != 0 { // decision 0 and every 7th decision commit no batch
			for i, primary := range []arma_types.PartyID{1, 2} {
				block, err := CreateDataCommonBlock(uint64(n)*2+uint64(i), nil, state.NewAvailableBatch(primary, 1, arma_types.BatchSequence(n), []byte{byte(n), byte(primary)}), 1, n, 2, i, 0)
				require.NoError(t, err)
				hdr.AvailableCommonBlocks = append(hdr.AvailableCommonBlocks, block)
			}
		}
		live.IndexCommitted(hdr)

		md, err := proto.Marshal(&smartbftprotos.ViewMetadata{LatestSequence: uint64(n)})
		require.NoError(t, err)
		proposal := smartbft_types.Proposal{Header: hdr.Serialize(), Payload: []byte{1}, Metadata: md}
		block := state.CreateBlockToAppendFromDecision(uint64(n), proposal, nil, prevHash, 0)
		prevHash = protoutil.BlockHeaderHash(block.Header)
		l.Append(block)
		last = st
	}
	require.Positive(t, last.ReplayFrom)

	rebuilt := committedindex.New()
	rebuildCommittedIndex(logger, rebuilt, l, last)

	require.Equal(t, live.Committed.Size(), rebuilt.Size())
	require.Equal(t, live.Committed.Ledgers(), rebuilt.Ledgers())
	require.Equal(t, live.Committed.ReplayFrom(), rebuilt.ReplayFrom())
	for _, key := range live.Committed.Ledgers() {
		from := last.PrunePoint(key.Shard, key.Primary)
		require.Equal(t,
			live.Committed.DurableFrontier(key.Shard, key.Primary, from, decisions-1),
			rebuilt.DurableFrontier(key.Shard, key.Primary, from, decisions-1),
			"pair %v", key)
	}
}
