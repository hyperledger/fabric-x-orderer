/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver

import (
	"fmt"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/pkg/errors"
)

// ConnectingNodes names the nodes that connect to a service: every node of a role, or the node of a
// single party, and for a batcher every shard or a single one. Build one through the constructors
// below, so that every party and every shard are named explicitly rather than by a zero ID.
type ConnectingNodes struct {
	role       types.NodeRole
	partyID    types.PartyID
	shardID    types.ShardID
	everyParty bool
	everyShard bool
}

// EveryRouter names the router of every party.
func EveryRouter() ConnectingNodes {
	return ConnectingNodes{role: types.RoleRouter, everyParty: true}
}

// RouterOfParty names the router of a single party.
func RouterOfParty(partyID types.PartyID) ConnectingNodes {
	return ConnectingNodes{role: types.RoleRouter, partyID: partyID}
}

// EveryBatcher names the batcher of every shard in every party.
func EveryBatcher() ConnectingNodes {
	return ConnectingNodes{role: types.RoleBatcher, everyParty: true, everyShard: true}
}

// BatchersOfShard names the batcher of a single shard in every party.
func BatchersOfShard(shardID types.ShardID) ConnectingNodes {
	return ConnectingNodes{role: types.RoleBatcher, shardID: shardID, everyParty: true}
}

// BatcherOfParty names the batcher of a single shard in a single party.
func BatcherOfParty(partyID types.PartyID, shardID types.ShardID) ConnectingNodes {
	return ConnectingNodes{role: types.RoleBatcher, partyID: partyID, shardID: shardID}
}

// EveryConsenter names the consenter of every party.
func EveryConsenter() ConnectingNodes {
	return ConnectingNodes{role: types.RoleConsenter, everyParty: true}
}

// ConsenterOfParty names the consenter of a single party.
func ConsenterOfParty(partyID types.PartyID) ConnectingNodes {
	return ConnectingNodes{role: types.RoleConsenter, partyID: partyID}
}

// EveryAssembler names the assembler of every party.
func EveryAssembler() ConnectingNodes {
	return ConnectingNodes{role: types.RoleAssembler, everyParty: true}
}

// AssemblerOfParty names the assembler of a single party.
func AssemblerOfParty(partyID types.PartyID) ConnectingNodes {
	return ConnectingNodes{role: types.RoleAssembler, partyID: partyID}
}

func (c ConnectingNodes) String() string {
	switch {
	case c.role == types.RoleBatcher && c.everyParty && c.everyShard:
		return "every batcher"
	case c.role == types.RoleBatcher && c.everyParty:
		return fmt.Sprintf("every batcher of shard %d", c.shardID)
	case c.role == types.RoleBatcher:
		return fmt.Sprintf("the batcher of party %d in shard %d", c.partyID, c.shardID)
	case c.everyParty:
		return fmt.Sprintf("every %s", c.role)
	default:
		return fmt.Sprintf("the %s of party %d", c.role, c.partyID)
	}
}

// validate refuses a role that does not exist, and a party or a shard of ID 0, which no node has.
func (c ConnectingNodes) validate() error {
	switch c.role {
	case types.RoleRouter, types.RoleBatcher, types.RoleConsenter, types.RoleAssembler:
	default:
		return errors.Errorf("role %d is not a node role", uint8(c.role))
	}

	if !c.everyParty && c.partyID == 0 {
		return errors.Errorf("%s: parties are numbered from 1", c)
	}

	if c.role == types.RoleBatcher && !c.everyShard && c.shardID == 0 {
		return errors.Errorf("%s: shards are numbered from 1", c)
	}

	return nil
}

// covers reports whether the node is one of the connecting nodes.
func (c ConnectingNodes) covers(node types.NodeIdentity) bool {
	if c.role != node.Role {
		return false
	}

	if !c.everyParty && c.partyID != node.PartyID {
		return false
	}

	return c.role != types.RoleBatcher || c.everyShard || c.shardID == node.ShardID
}
