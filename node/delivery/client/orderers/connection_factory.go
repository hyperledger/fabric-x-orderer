/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package orderers

import (
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
)

type ConnectionSourcer interface {
	PartyEndpoint(party types.PartyID) (*Endpoint, error)
	RandomEndpoint() (*Endpoint, error)
	ShuffledEndpoints() []*Endpoint
	Update(partyToEndpoints map[types.PartyID]*SourceEndpoint)
}

type ConnectionSourceCreator interface {
	// CreateConnectionSource creates a ConnectionSourcer implementation.
	// In a peer, selfEndpoint == "";
	// In an orderer selfEndpoint carries the (replication service) endpoint of the orderer.
	CreateConnectionSource(logger *flogging.FabricLogger, selfParty types.PartyID, targetShard types.ShardID, partOfTarget bool) ConnectionSourcer
}

type ConnectionSourceFactory struct{}

func (f *ConnectionSourceFactory) CreateConnectionSource(logger *flogging.FabricLogger, selfParty types.PartyID, targetShard types.ShardID, partOfTarget bool) ConnectionSourcer {
	return NewConnectionSource(logger, selfParty, targetShard, partOfTarget)
}
