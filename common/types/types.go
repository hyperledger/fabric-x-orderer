/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

import (
	"fmt"
	"math"
)

// ShardID identifies a shard, should be >0.
// Value math.MaxUint16 is reserved.
type ShardID uint16

// ShardIDConsensus is used to encode a config TX / batch emitted by consensus.
const ShardIDConsensus ShardID = math.MaxUint16

// PartyID identifies a party, must be >0.
type PartyID uint16

// BatchSequence is the number a primary batcher assigns to the batches it produces.
type BatchSequence uint64

// DecisionNum is the number the consensus nodes assign to each decision they produce.
type DecisionNum uint64

// ConfigSequence numbers configuration changes, as delivered by a config TX and the corresponding config block.
// It starts from 0 (on the genesis block) and increases by 1 with every config change.
type ConfigSequence uint64

// BatchID is the tuple that identifies a batch.
type BatchID interface {
	// Shard the shard from which this batch was produced.
	Shard() ShardID
	// Primary is the Party ID of the primary batcher which produces this batch.
	Primary() PartyID
	// Seq is the sequence number of this batch.
	Seq() BatchSequence
	// Digest is the digest of the requests in this batch.
	Digest() []byte
}

type BatchAttestation interface {
	Fragments() []BatchAttestationFragment
	Digest() []byte
	Seq() BatchSequence
	Primary() PartyID
	Shard() ShardID
	Serialize() []byte
	Deserialize([]byte) error
}

type BatchAttestationFragment interface {
	Seq() BatchSequence
	Primary() PartyID
	Shard() ShardID
	Signer() PartyID
	Signature() []byte        // Signature over the BAF by the creator of this BAF
	PrimarySignature() []byte // Signature over the BAF by the primary who created the batch (empty when signer of BAF is the primary)
	Digest() []byte
	Serialize() []byte
	Deserialize([]byte) error
	TXCount() uint64
	ConfigSequence() ConfigSequence
	String() string
}

type Batch interface {
	BatchID
	Requests() BatchedRequests
	ConfigSequence() ConfigSequence
	PrimarySignature() []byte
}

type AssemblerConsensusPosition struct {
	DecisionNum DecisionNum
	BatchIndex  int
}

// NodeRole is the service a node runs.
type NodeRole uint8

const (
	RoleUnknown NodeRole = iota
	RoleRouter
	RoleBatcher
	RoleConsenter
	RoleAssembler
)

func (r NodeRole) String() string {
	switch r {
	case RoleRouter:
		return "router"
	case RoleBatcher:
		return "batcher"
	case RoleConsenter:
		return "consenter"
	case RoleAssembler:
		return "assembler"
	default:
		return fmt.Sprintf("unknown role (%d)", uint8(r))
	}
}

// NodeIdentity consists of the role, the party ID, and the shard ID (for batchers).
//
// Construct one through the role-specific constructors below rather than a struct literal: only a
// batcher belongs to a shard, so ShardID is always 0 for every other role, and the constructors are
// the single blessed way to enforce that.
type NodeIdentity struct {
	PartyID PartyID
	Role    NodeRole
	// ShardID identifies a batcher's shard. It is 0 for every other role, which belongs to no
	// shard; in particular it is never ShardIDConsensus, which encodes config TXs/batches emitted
	// by consensus and is not a node identity.
	ShardID ShardID
}

// NewBatcherIdentity returns the identity of the batcher of the given party in the given shard.
func NewBatcherIdentity(partyID PartyID, shardID ShardID) NodeIdentity {
	return NodeIdentity{Role: RoleBatcher, PartyID: partyID, ShardID: shardID}
}

// NewRouterIdentity returns the identity of the router of the given party. A router belongs to no
// shard, so its ShardID is 0.
func NewRouterIdentity(partyID PartyID) NodeIdentity {
	return NodeIdentity{Role: RoleRouter, PartyID: partyID}
}

// NewConsenterIdentity returns the identity of the consenter of the given party. A consenter
// belongs to no shard, so its ShardID is 0.
func NewConsenterIdentity(partyID PartyID) NodeIdentity {
	return NodeIdentity{Role: RoleConsenter, PartyID: partyID}
}

// NewAssemblerIdentity returns the identity of the assembler of the given party. An assembler
// belongs to no shard, so its ShardID is 0.
func NewAssemblerIdentity(partyID PartyID) NodeIdentity {
	return NodeIdentity{Role: RoleAssembler, PartyID: partyID}
}

func (n NodeIdentity) String() string {
	if n.Role == RoleBatcher {
		return fmt.Sprintf("%s of party %d in shard %d", n.Role, n.PartyID, n.ShardID)
	}
	return fmt.Sprintf("%s of party %d", n.Role, n.PartyID)
}
