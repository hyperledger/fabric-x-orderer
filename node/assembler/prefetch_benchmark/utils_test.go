/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package prefetch_benchmark_test

import (
	"math/rand"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
)

const PREFETCH_STRESS_TEST_MEMORY_FACTOR = 100

func choosePrimary(parties []types.PartyID, currentLeader types.PartyID) types.PartyID {
	if len(parties) == 1 {
		return parties[0]
	}
	for {
		newLeader := parties[rand.Intn(len(parties))]
		if newLeader != currentLeader {
			return newLeader
		}
	}
}

func tossCoin(p float64) bool {
	return rand.Float64() < p
}

func selectRandomBatches(batches []types.BatchID, k int) ([]types.BatchID, []types.BatchID) {
	if k <= 0 {
		return []types.BatchID{}, batches
	}

	if k >= len(batches) {
		return batches, []types.BatchID{}
	}

	pos := 0
	for pos < k {
		idx := pos + rand.Intn(len(batches)-pos)
		batches[pos], batches[idx] = batches[idx], batches[pos]
		pos++
	}

	return batches[:k], batches[k:]
}
