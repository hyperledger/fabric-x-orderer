/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"sort"

	"github.com/hyperledger/fabric-x-orderer/common/types"
)

func (c *BatcherNodeConfig) GetShardsIDs() []types.ShardID {
	var ids []types.ShardID
	for _, shard := range c.Shards {
		ids = append(ids, shard.ShardId)
	}
	sort.Slice(ids, func(i, j int) bool {
		return int(ids[i]) < int(ids[j])
	})
	return ids
}

func (rc *RouterNodeConfig) GetMaxSizeBytes() (uint64, error) {
	return rc.RequestMaxBytes, nil
}
