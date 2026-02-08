/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"github.com/hyperledger/fabric-x-orderer/common/types"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
)

//go:generate counterfeiter -o mocks/consensus_decision_replicator_creator.go . ConsensusDecisionReplicatorCreator
type ConsensusDecisionReplicatorCreator interface {
	CreateDecisionConsensusReplicator(conf *node_config.BatcherNodeConfig, logger types.Logger, lastKnownDecisionNum types.DecisionNum) DecisionReplicator
}

type ConsensusDecisionReplicatorFactory struct{}

func (c *ConsensusDecisionReplicatorFactory) CreateDecisionConsensusReplicator(config *node_config.BatcherNodeConfig, logger types.Logger, lastKnownDecisionNum types.DecisionNum) DecisionReplicator {
	var endpoint string
	var tlsCAs []node_config.RawBytes
	for i := 0; i < len(config.Consenters); i++ {
		consenter := config.Consenters[i]
		if consenter.PartyID == config.PartyId {
			endpoint = consenter.Endpoint
			tlsCAs = consenter.TLSCACerts
		}
	}

	if endpoint == "" || len(tlsCAs) == 0 {
		logger.Panicf("Failed finding endpoint and TLS CAs for party %d", config.PartyId)
	}
	return delivery.NewConsensusDecisionReplicator(tlsCAs, config.TLSPrivateKeyFile, config.TLSCertificateFile, endpoint, logger, delivery.NextSeekInfo(uint64(lastKnownDecisionNum)))
}
