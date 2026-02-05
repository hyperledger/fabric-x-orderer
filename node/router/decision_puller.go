/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
)

type DecisionPuller interface {
	ReplicateState() <-chan *state.Header
	Stop()
}

func CreateConsensusDecisionReplicator(config *node_config.RouterNodeConfig, seekInfo *orderer.SeekInfo, logger types.Logger) DecisionPuller {
	return delivery.NewConsensusDecisionReplicator(config.Consenter.TLSCACerts, config.TLSPrivateKeyFile, config.TLSCertificateFile, config.Consenter.Endpoint, logger, seekInfo)
}
