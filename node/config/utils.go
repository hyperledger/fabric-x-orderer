/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"sort"

	"github.com/hyperledger/fabric-x-common/common/configtx"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
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

func NewRouterFilterConfig(requestMaxBytes uint64, clientSignatureVerificationRequired bool, channelID string, policyManager policies.Manager, configTxValidator configtx.Validator, signer protoutil.Signer) RouterFilterConfig {
	return RouterFilterConfig{requestMaxBytes: requestMaxBytes, clientSignatureVerificationRequired: clientSignatureVerificationRequired, channelID: channelID, policyManager: policyManager, configTxValidator: configTxValidator, signer: signer}
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

func (rfc RouterFilterConfig) PolicyManager() policies.Manager {
	return rfc.policyManager
}

func (rfc RouterFilterConfig) ConfigTxValidator() configtx.Validator {
	return rfc.configTxValidator
}

func (rfc RouterFilterConfig) Signer() protoutil.Signer {
	return rfc.signer
}
