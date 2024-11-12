package types

// ShardID identifies a shard, should be >0.
// Value math.MaxUint16 is reserved.
type ShardID uint16

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
