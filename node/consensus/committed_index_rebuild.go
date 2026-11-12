/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/committedindex"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
)

// rebuildCommittedIndex rebuilds the index of committed batches of a starting consenter from its ledger,
// whose last decision has state st. It replays the decisions from st.ReplayFrom, which committed every
// batch at or above the prune points of st, and then drops the batches below those prune points. It panics
// if a decision cannot be read.
func rebuildCommittedIndex(logger *flogging.FabricLogger, idx *committedindex.Index, l *ledger.ConsensusLedger, st *state.State) {
	height := l.Height()
	from := uint64(st.ReplayFrom)
	if from >= height {
		return
	}

	start := time.Now()
	it, _ := l.Iterator(&ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: from}}})
	defer it.Close()

	for num := from; num < height; num++ {
		block, status := it.Next()
		if status != common.Status_SUCCESS {
			logger.Panicf("Failed reading decision %d to rebuild the committed index: %s", num, status)
		}
		proposal, err := state.ConsenterBlockToProposal(block)
		if err != nil {
			logger.Panicf("Failed reading decision %d to rebuild the committed index: %v", num, err)
		}
		hdr := &state.Header{}
		if err := hdr.Deserialize(proposal.Header); err != nil {
			logger.Panicf("Failed reading decision %d to rebuild the committed index: %v", num, err)
		}
		ids, err := committedBatchIDs(hdr)
		if err != nil {
			logger.Panicf("Failed reading decision %d to rebuild the committed index: %v", num, err)
		}
		idx.Add(hdr.Num, ids)
	}

	for _, p := range st.PrunePoints {
		idx.DropBelow(p.Shard, p.Primary, p.Seq)
	}

	logger.Infof("Rebuilt the committed index from decisions %d to %d in %s; it holds %d batches",
		from, height-1, time.Since(start).Round(time.Millisecond), idx.Size())
}
