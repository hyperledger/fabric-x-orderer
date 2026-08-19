/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/stretchr/testify/require"
)

// baf builds a BAF for the given batch identity <shard, primary, seq, digest> and signer. Only the
// identity fields and signer matter to aggregateFragments; the rest are fixed.
func baf(shard types.ShardID, primary types.PartyID, seq types.BatchSequence, digest []byte, signer types.PartyID) types.BatchAttestationFragment {
	return types.NewSimpleBatchAttestationFragment(shard, primary, seq, digest, signer, 0, 0, nil)
}

// requireSingleBatch asserts every fragment in the group carries the same
// <shard, primary, seq, digest>, i.e. the group really is a single batch.
func requireSingleBatch(t *testing.T, group []types.BatchAttestationFragment) {
	t.Helper()
	require.NotEmpty(t, group)
	head := group[0]
	for _, f := range group {
		require.Equal(t, head.Shard(), f.Shard())
		require.Equal(t, head.Primary(), f.Primary())
		require.Equal(t, head.Seq(), f.Seq())
		require.Equal(t, head.Digest(), f.Digest())
	}
}

func TestAggregateFragments(t *testing.T) {
	digestA := []byte{0xAA}
	digestB := []byte{0xBB}

	t.Run("empty input yields no groups", func(t *testing.T) {
		require.Empty(t, aggregateFragments(nil))
		require.Empty(t, aggregateFragments([]types.BatchAttestationFragment{}))
	})

	t.Run("fragments of one batch collapse into a single group", func(t *testing.T) {
		in := []types.BatchAttestationFragment{
			baf(1, 1, 1, digestA, 2),
			baf(1, 1, 1, digestA, 3),
			baf(1, 1, 1, digestA, 4),
		}

		groups := aggregateFragments(in)

		require.Len(t, groups, 1)
		require.Len(t, groups[0], 3)
		requireSingleBatch(t, groups[0])
		require.Equal(t, digestA, groups[0][0].Digest())
	})

	t.Run("grouping is by identity not adjacency and preserves first-seen order", func(t *testing.T) {
		// Batch A (shard 1) is seen first, then batch B (shard 2); the two interleave in the input.
		in := []types.BatchAttestationFragment{
			baf(1, 1, 1, digestA, 2),
			baf(2, 3, 1, digestB, 4),
			baf(1, 1, 1, digestA, 5),
			baf(2, 3, 1, digestB, 6),
		}

		groups := aggregateFragments(in)

		require.Len(t, groups, 2)
		// First-seen order: batch A (shard 1) before batch B (shard 2).
		require.Equal(t, types.ShardID(1), groups[0][0].Shard())
		require.Equal(t, types.ShardID(2), groups[1][0].Shard())
		for _, g := range groups {
			require.Len(t, g, 2)
			requireSingleBatch(t, g)
		}
	})

	t.Run("each identity dimension splits groups", func(t *testing.T) {
		// Five fragments differing from a baseline batch in exactly one identity dimension each,
		// plus the baseline itself: shard, primary, seq, and digest must every one be part of the
		// key, so all five are distinct batches -> five singleton groups.
		in := []types.BatchAttestationFragment{
			baf(1, 1, 1, digestA, 2), // baseline
			baf(9, 1, 1, digestA, 2), // differs by shard
			baf(1, 9, 1, digestA, 2), // differs by primary
			baf(1, 1, 9, digestA, 2), // differs by seq
			baf(1, 1, 1, digestB, 2), // differs by digest
		}

		groups := aggregateFragments(in)

		require.Len(t, groups, 5)
		for _, g := range groups {
			require.Len(t, g, 1)
		}
	})

	t.Run("same digest under different primaries are distinct batches", func(t *testing.T) {
		// A legitimate post-rotation case: two primaries batch the same requests -> identical
		// digest for the same shard/seq. Different Primary => different BatchID => separate groups.
		digest := []byte{0x07}
		in := []types.BatchAttestationFragment{
			baf(1, 1, 1, digest, 2),
			baf(1, 6, 1, digest, 4),
			baf(1, 1, 1, digest, 3),
			baf(1, 6, 1, digest, 5),
		}

		groups := aggregateFragments(in)

		require.Len(t, groups, 2)
		seenPrimaries := map[types.PartyID]int{}
		for _, g := range groups {
			require.Len(t, g, 2)
			requireSingleBatch(t, g)
			require.Equal(t, digest, g[0].Digest())
			seenPrimaries[g[0].Primary()]++
		}
		require.Equal(t, 1, seenPrimaries[types.PartyID(1)])
		require.Equal(t, 1, seenPrimaries[types.PartyID(6)])
	})
}
