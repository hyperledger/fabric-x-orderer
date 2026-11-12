/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package committedindex

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/stretchr/testify/require"
)

// commit is one batch of <shard, primary> with the given seq and digest, committed in decision.
type commit struct {
	shard    types.ShardID
	primary  types.PartyID
	seq      types.BatchSequence
	digest   byte
	decision types.DecisionNum
}

// indexOf adds the commits to a new index, one decision at a time and in decision order, as the
// consenter does.
func indexOf(commits ...commit) *Index {
	x := New()
	byDecision := make(map[types.DecisionNum][]types.BatchID)
	var last types.DecisionNum
	for _, c := range commits {
		byDecision[c.decision] = append(byDecision[c.decision], state.NewAvailableBatch(c.primary, c.shard, c.seq, []byte{c.digest}))
		last = max(last, c.decision)
	}
	for d := types.DecisionNum(0); d <= last; d++ {
		x.Add(d, byDecision[d])
	}
	return x
}

func TestDurableFrontier(t *testing.T) {
	const durable = 10

	for _, tst := range []struct {
		name    string
		commits []commit
		from    types.BatchSequence
		want    types.BatchSequence
	}{
		{
			name:    "durable run",
			commits: []commit{{1, 1, 0, 0, 1}, {1, 1, 1, 0, 2}, {1, 1, 2, 0, 3}, {1, 1, 3, 0, 4}, {1, 1, 4, 0, 5}},
			want:    5,
		},
		{
			name:    "gap",
			commits: []commit{{1, 1, 0, 0, 1}, {1, 1, 1, 0, 2}, {1, 1, 2, 0, 3}, {1, 1, 4, 0, 5}},
			want:    3,
		},
		{
			name:    "not durable yet",
			commits: []commit{{1, 1, 0, 0, 1}, {1, 1, 1, 0, 2}, {1, 1, 2, 0, 11}, {1, 1, 3, 0, 4}, {1, 1, 4, 0, 5}},
			want:    2,
		},
		{
			name:    "second digest not durable",
			commits: []commit{{1, 1, 0, 0, 1}, {1, 1, 1, 1, 9}, {1, 1, 1, 2, 11}, {1, 1, 2, 0, 5}},
			want:    1,
		},
		{
			name:    "second digest durable",
			commits: []commit{{1, 1, 0, 0, 1}, {1, 1, 1, 1, 2}, {1, 1, 1, 2, 3}, {1, 1, 2, 0, 4}},
			want:    3,
		},
		{
			name: "from the middle",
			commits: []commit{
				{1, 1, 0, 0, 1},
				{1, 1, 1, 0, 2},
				{1, 1, 2, 0, 3},
				{1, 1, 3, 0, 4},
				{1, 1, 4, 0, 5},
				{1, 1, 5, 0, 6},
			},
			from: 3,
			want: 6,
		},
		{
			name:    "unknown pair",
			commits: []commit{{2, 2, 0, 0, 1}},
			from:    7,
			want:    7,
		},
		{
			name:    "commits out of seq order",
			commits: []commit{{1, 1, 1, 0, 3}, {1, 1, 0, 0, 4}},
			want:    2,
		},
		{
			name:    "other pairs do not count",
			commits: []commit{{1, 1, 0, 0, 1}, {1, 2, 1, 0, 2}, {2, 1, 1, 0, 3}},
			want:    1,
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			x := indexOf(tst.commits...)
			require.Equal(t, tst.want, x.DurableFrontier(1, 1, tst.from, durable))
		})
	}
}

// TestReplayFromP1P2 covers a primary that served a few early decisions and a primary that has served for
// a long time since: once the early primary's batches are dropped, the replay start follows the recent
// primary only.
func TestReplayFromP1P2(t *testing.T) {
	var commits []commit
	for seq := types.BatchSequence(0); seq < 10; seq++ {
		commits = append(commits, commit{1, 1, seq, 0, types.DecisionNum(1 + seq/2)})
	}
	for k := types.BatchSequence(0); k < 1000; k++ {
		commits = append(commits, commit{1, 2, k, 0, types.DecisionNum(6 + k)})
	}
	x := indexOf(commits...)
	require.Equal(t, types.DecisionNum(1), x.ReplayFrom())

	x.DropBelow(1, 1, 10)
	x.DropBelow(1, 2, 990)

	require.Equal(t, types.DecisionNum(996), x.ReplayFrom())
	require.Equal(t, 10, x.Size())
	require.Equal(t, []LedgerKey{{Shard: 1, Primary: 2}}, x.Ledgers())
}

// TestReplayFromEmpty verifies that an index that holds nothing replays from the next decision to index.
func TestReplayFromEmpty(t *testing.T) {
	x := New()
	require.Equal(t, types.DecisionNum(0), x.ReplayFrom())

	x.Add(0, nil)
	x.Add(1, nil)
	require.Equal(t, types.DecisionNum(2), x.ReplayFrom())

	x = indexOf(commit{1, 1, 0, 0, 3}, commit{1, 1, 1, 0, 7})
	x.DropBelow(1, 1, 2)
	require.Equal(t, 0, x.Size())
	require.Empty(t, x.Ledgers())
	require.Equal(t, types.DecisionNum(8), x.ReplayFrom())
}

// TestDropBelowKeepsTheRest verifies that dropping part of a pair keeps the frontier and the replay start
// of what remains.
func TestDropBelowKeepsTheRest(t *testing.T) {
	x := indexOf(commit{1, 1, 0, 0, 1}, commit{1, 1, 1, 0, 2}, commit{1, 1, 2, 0, 3}, commit{1, 1, 3, 0, 4})
	x.DropBelow(1, 1, 2)

	require.Equal(t, 2, x.Size())
	require.Equal(t, types.DecisionNum(3), x.ReplayFrom())
	require.Equal(t, types.BatchSequence(4), x.DurableFrontier(1, 1, 2, 10))

	// Dropping below a seq that is already gone changes nothing.
	x.DropBelow(1, 1, 1)
	require.Equal(t, 2, x.Size())
}

// TestAddIdempotent verifies that adding a decision again, as the sync path may, changes nothing.
func TestAddIdempotent(t *testing.T) {
	x := indexOf(commit{1, 1, 0, 0, 1}, commit{1, 1, 1, 0, 2}, commit{1, 1, 2, 0, 3})
	size, replayFrom := x.Size(), x.ReplayFrom()

	x.Add(3, []types.BatchID{state.NewAvailableBatch(1, 1, 2, []byte{0})})
	x.Add(2, []types.BatchID{state.NewAvailableBatch(1, 1, 1, []byte{0})})

	require.Equal(t, size, x.Size())
	require.Equal(t, replayFrom, x.ReplayFrom())
}

func TestLedgersSorted(t *testing.T) {
	x := indexOf(commit{1, 2, 0, 0, 1}, commit{0, 3, 0, 0, 2}, commit{1, 1, 0, 0, 3})
	require.Equal(t, []LedgerKey{{Shard: 0, Primary: 3}, {Shard: 1, Primary: 1}, {Shard: 1, Primary: 2}}, x.Ledgers())
}

// TestFarBeyondTheWindow verifies that a seq far above the held batches drops all of them and yields itself
// as the frontier, rather than overflowing the window offset.
func TestFarBeyondTheWindow(t *testing.T) {
	const far = types.BatchSequence(1 << 63)

	x := indexOf(commit{1, 1, 0, 0, 1}, commit{1, 1, 1, 0, 2})
	require.Equal(t, far, x.DurableFrontier(1, 1, far, 10))

	x.DropBelow(1, 1, far)
	require.Zero(t, x.Size())
	require.Equal(t, types.DecisionNum(3), x.ReplayFrom())
}
