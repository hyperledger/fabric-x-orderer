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
	Signature() []byte
	Digest() []byte
	Serialize() []byte
	Deserialize([]byte) error
	ConfigSequence() ConfigSequence
	String() string
}

type Batch interface {
	BatchID
	Requests() BatchedRequests
}

// OrderingInfo is an opaque object that provides extra information on the order of the batch attestation and
// metadata to be used in the construction of the block.
type OrderingInfo interface {
	fmt.Stringer
}

// OrderedBatchAttestation carries the BatchAttestation and information on the actual order of the batch from the
// consensus cluster. This information is used when appending to the ledger, and helps the assembler to recover
// following a shutdown or a failure.
type OrderedBatchAttestation interface {
	BatchAttestation() BatchAttestation
	// OrderingInfo is an opaque object that provides extra information on the order of the batch attestation and
	// metadata to be used in the construction of the block.
	OrderingInfo() OrderingInfo
}

type AssemblerConsensusPosition struct {
	DecisionNum DecisionNum
	BatchIndex  int
}
