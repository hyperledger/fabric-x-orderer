package assembler

import (
	"fmt"

	"arma/common/types"
	"arma/core"
	"arma/node/config"
)

func partiesFromAssemblerConfig(config config.AssemblerNodeConfig) []types.PartyID {
	var parties []types.PartyID
	for _, b := range config.Shards[0].Batchers {
		parties = append(parties, types.PartyID(b.PartyID))
	}
	return parties
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
