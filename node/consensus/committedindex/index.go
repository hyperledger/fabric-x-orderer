/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package committedindex keeps, in memory, the decision that committed each batch of every
// <shard, primary> that is not pruned yet, so that consensus can compute prune points without reading its
// ledger.
package committedindex

import (
	"cmp"
	"slices"
	"sort"
	"sync"

	"github.com/hyperledger/fabric-x-orderer/common/types"
)

// LedgerKey identifies the batch ledger of a primary in a shard.
type LedgerKey struct {
	Shard   types.ShardID
	Primary types.PartyID
}

// Index records the decisions that committed the batches of each <shard, primary>, from the lowest seq not
// dropped yet upwards. Batches are added in decision order, as decisions commit, and dropped below the prune
// points. The zero value is an empty index. It is safe for concurrent use, and must not be copied.
type Index struct {
	lock    sync.RWMutex
	ledgers map[LedgerKey]*ledgerWindow
	// queue holds, in decision order, the decisions that committed batches still in the index, with the
	// number of such batches. Its front is the lowest decision that committed a batch the index holds.
	queue []decisionCount
	// next is the decision Add expects next.
	next types.DecisionNum
	size int
}

// ledgerWindow holds the batches of one <shard, primary>. decisions[i] lists the decisions that committed
// seq base+i: usually one, more for an equivocating primary, and none for a seq that is not committed.
type ledgerWindow struct {
	base      types.BatchSequence
	decisions [][]types.DecisionNum
	entries   int
}

type decisionCount struct {
	decision types.DecisionNum
	count    int
}

// New returns an empty index.
func New() *Index {
	return &Index{}
}

// Add records the batches committed by decision num. Decisions must be added in order; a decision below
// the next one expected was already added and is ignored, so adding a decision again changes nothing.
func (x *Index) Add(num types.DecisionNum, ids []types.BatchID) {
	x.lock.Lock()
	defer x.lock.Unlock()

	if num < x.next {
		return
	}
	x.next = num + 1
	if len(ids) == 0 {
		return
	}

	if x.ledgers == nil {
		x.ledgers = make(map[LedgerKey]*ledgerWindow)
	}
	x.queue = append(x.queue, decisionCount{decision: num, count: len(ids)})
	x.size += len(ids)
	for _, id := range ids {
		key := LedgerKey{Shard: id.Shard(), Primary: id.Primary()}
		w, exists := x.ledgers[key]
		if !exists {
			w = &ledgerWindow{base: id.Seq()}
			x.ledgers[key] = w
		}
		w.add(id.Seq(), num)
	}
}

// DropBelow removes the batches of <shard, primary> whose seq is below seq.
func (x *Index) DropBelow(shard types.ShardID, primary types.PartyID, seq types.BatchSequence) {
	x.lock.Lock()
	defer x.lock.Unlock()

	key := LedgerKey{Shard: shard, Primary: primary}
	w, exists := x.ledgers[key]
	if !exists || seq <= w.base {
		return
	}

	dropped := len(w.decisions)
	if offset := seq - w.base; offset < types.BatchSequence(dropped) {
		dropped = int(offset)
	}
	for _, decisions := range w.decisions[:dropped] {
		for _, decision := range decisions {
			x.release(decision)
		}
		w.entries -= len(decisions)
		x.size -= len(decisions)
	}
	w.decisions = w.decisions[dropped:]
	w.base = seq
	if w.entries == 0 {
		delete(x.ledgers, key)
	}

	for len(x.queue) > 0 && x.queue[0].count == 0 {
		x.queue = x.queue[1:]
	}
}

// release decrements the number of batches the index holds from decision.
func (x *Index) release(decision types.DecisionNum) {
	i := sort.Search(len(x.queue), func(i int) bool { return x.queue[i].decision >= decision })
	x.queue[i].count--
}

// DurableFrontier returns the lowest seq at or above from that <shard, primary> did not commit in a decision
// at or below durable: a seq with no committed batch, or with a batch committed in a later decision.
func (x *Index) DurableFrontier(shard types.ShardID, primary types.PartyID, from types.BatchSequence, durable types.DecisionNum) types.BatchSequence {
	x.lock.RLock()
	defer x.lock.RUnlock()

	w, exists := x.ledgers[LedgerKey{Shard: shard, Primary: primary}]
	if !exists || from < w.base || from-w.base >= types.BatchSequence(len(w.decisions)) {
		return from
	}

	seq := from
	for i := int(from - w.base); i < len(w.decisions); i++ {
		decisions := w.decisions[i]
		if len(decisions) == 0 || slices.Max(decisions) > durable {
			break
		}
		seq++
	}
	return seq
}

// Ledgers returns the <shard, primary> pairs the index holds batches of, sorted by shard and then primary.
func (x *Index) Ledgers() []LedgerKey {
	x.lock.RLock()
	defer x.lock.RUnlock()

	keys := make([]LedgerKey, 0, len(x.ledgers))
	for key := range x.ledgers {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b LedgerKey) int {
		if c := cmp.Compare(a.Shard, b.Shard); c != 0 {
			return c
		}
		return cmp.Compare(a.Primary, b.Primary)
	})
	return keys
}

// ReplayFrom returns the lowest decision that committed a batch the index holds, or, when it holds none, the
// next decision to add. Replaying the decisions from it rebuilds every batch the index holds.
func (x *Index) ReplayFrom() types.DecisionNum {
	x.lock.RLock()
	defer x.lock.RUnlock()

	if len(x.queue) > 0 {
		return x.queue[0].decision
	}
	return x.next
}

// Size returns the number of batches the index holds.
func (x *Index) Size() int {
	x.lock.RLock()
	defer x.lock.RUnlock()

	return x.size
}

// add records that decision committed seq. A seq below base, committed out of order, extends the window
// downwards.
func (w *ledgerWindow) add(seq types.BatchSequence, decision types.DecisionNum) {
	if seq < w.base {
		w.decisions = append(make([][]types.DecisionNum, w.base-seq), w.decisions...)
		w.base = seq
	}
	i := int(seq - w.base)
	if i >= len(w.decisions) {
		w.decisions = append(w.decisions, make([][]types.DecisionNum, i+1-len(w.decisions))...)
	}
	w.decisions[i] = append(w.decisions[i], decision)
	w.entries++
}
