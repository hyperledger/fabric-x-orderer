/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"fmt"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
)

func partiesFromAssemblerConfig(config *config.AssemblerNodeConfig) []types.PartyID {
	var parties []types.PartyID
	for _, b := range config.Shards[0].Batchers {
		parties = append(parties, types.PartyID(b.PartyID))
	}
	return parties
}

func shardsFromAssemblerConfig(config *config.AssemblerNodeConfig) []types.ShardID {
	shardIds := make([]types.ShardID, len(config.Shards))
	for i, shard := range config.Shards {
		shardIds[i] = shard.ShardId
	}
	return shardIds
}

// TODO: use stringer/formatter/gostringer for more general solution
func BatchToString(batch types.BatchID) string {
	if batch == nil {
		return "<NIL BATCH>"
	}
	return fmt.Sprintf("Batch<shard: %d, primary: %d, seq: %d, digest: %s>", batch.Shard(), batch.Primary(), batch.Seq(), core.ShortDigestString(batch.Digest()))
}

func batchSizeBytes(batch core.Batch) int {
	size := 0
	for _, req := range batch.Requests() {
		size += len(req)
	}
	return size
}
