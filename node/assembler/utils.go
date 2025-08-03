/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"sort"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/config"
)

func partiesFromAssemblerConfig(config *config.AssemblerNodeConfig) []types.PartyID {
	var parties []types.PartyID
	for _, b := range config.Shards[0].Batchers {
		parties = append(parties, types.PartyID(b.PartyID))
	}

	sort.Slice(parties, func(i, j int) bool {
		return int(parties[i]) < int(parties[j])
	})

	return parties
}

func shardsFromAssemblerConfig(config *config.AssemblerNodeConfig) []types.ShardID {
	shardIds := make([]types.ShardID, len(config.Shards))
	for i, shard := range config.Shards {
		shardIds[i] = shard.ShardId
	}

	sort.Slice(shardIds, func(i, j int) bool {
		return int(shardIds[i]) < int(shardIds[j])
	})

	return shardIds
}

// TODO: use stringer/formatter/gostringer for more general solution
func BatchToString(batchID types.BatchID) string {
	return types.BatchIDToString(batchID)
}

func batchSizeBytes(batch types.Batch) int {
	size := 0
	for _, req := range batch.Requests() {
		size += len(req)
	}
	return size
}
