package types

type SimpleBatch struct {
	seq      BatchSequence
	shard    ShardID
	primary  PartyID
	requests BatchedRequests
	digest   []byte
}

func NewSimpleBatch(seq BatchSequence, shard ShardID, primary PartyID, requests BatchedRequests, digest []byte) *SimpleBatch {
	return &SimpleBatch{seq: seq, shard: shard, primary: primary, requests: requests, digest: digest}
}

func (sb *SimpleBatch) Digest() []byte            { return sb.digest }
func (sb *SimpleBatch) Requests() BatchedRequests { return sb.requests }
func (sb *SimpleBatch) Primary() PartyID          { return sb.primary }
func (sb *SimpleBatch) Shard() ShardID            { return sb.shard }
func (sb *SimpleBatch) Seq() BatchSequence        { return sb.seq }
