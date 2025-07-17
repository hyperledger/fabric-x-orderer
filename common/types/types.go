/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

import (
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

// BatchID is the tuple that identifies a batch.
//
//go:generate counterfeiter -o mocks/batch_id.go . BatchID
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

//go:generate counterfeiter -o mocks/batch_attestation.go . BatchAttestation
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
	Signature() []byte
	Digest() []byte
	Serialize() []byte
	Deserialize([]byte) error
	GarbageCollect() [][]byte
	Epoch() int64
	String() string
}

//go:generate counterfeiter -o mocks/batch.go . Batch
type Batch interface {
	BatchID
	Requests() BatchedRequests
}
