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

func NewRouterFilterConfig(requestMaxBytes uint64, clientSignatureVerificationRequired bool, channelID string) RouterFilterConfig {
	return RouterFilterConfig{requestMaxBytes: requestMaxBytes, clientSignatureVerificationRequired: clientSignatureVerificationRequired, channelID: channelID}
}

func (rfc RouterFilterConfig) RequestMaxBytes() uint64 {
	return rfc.requestMaxBytes
}

func (rfc RouterFilterConfig) ClientSignatureVerificationRequired() bool {
	return rfc.clientSignatureVerificationRequired
}

func (rfc RouterFilterConfig) ChannelID() string {
	return rfc.channelID
}
