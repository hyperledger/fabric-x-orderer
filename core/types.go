package core

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Panicf(template string, args ...interface{})
}

// ShardID identifies a shard, should be >0.
// Value math.MaxUint16 is reserved.
type ShardID uint16

// PartyID identifies a party, must be >0.
type PartyID uint16

// BatchSequence is the number a primary batcher assigns to the batches it produces.
type BatchSequence uint64 // TODO use all over

// BatchID is the tuple that identifies a batch.
type BatchID interface {
	// Shard the shard from which this batch was produced.
	Shard() ShardID
	// Primary is the PartyID of the primary batcher which produces this batch.
	Primary() PartyID
	// Seq is the sequence number of this batch.
	Seq() uint64 // TODO use BatchSequence
	// Digest is the digest of the requests in this batch.
	Digest() []byte
}
