/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"sort"

	"github.com/hyperledger/fabric-x-common/common/policies"
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

func (c *BatcherNodeConfig) GetRequestMaxBytes() uint64 {
	return c.RequestMaxBytes
}

func (c *BatcherNodeConfig) GetClientSignatureVerificationRequired() bool {
	return c.ClientSignatureVerificationRequired
}

func (c *BatcherNodeConfig) GetChannelID() string {
	return c.Bundle.ConfigtxValidator().ChannelID()
}

func (c *BatcherNodeConfig) GetPolicyManager() policies.Manager {
	return c.Bundle.PolicyManager()
}

func (rfc *RouterNodeConfig) GetRequestMaxBytes() uint64 {
	return rfc.RequestMaxBytes
}

func (rfc *RouterNodeConfig) GetClientSignatureVerificationRequired() bool {
	return rfc.ClientSignatureVerificationRequired
}

func (rfc *RouterNodeConfig) GetChannelID() string {
	return rfc.Bundle.ConfigtxValidator().ChannelID()
}

func (rfc *RouterNodeConfig) GetPolicyManager() policies.Manager {
	return rfc.Bundle.PolicyManager()
}
