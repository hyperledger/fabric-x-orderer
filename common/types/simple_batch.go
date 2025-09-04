/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

type SimpleBatch struct {
	shard          ShardID
	primary        PartyID
	seq            BatchSequence
	digest         []byte
	requests       BatchedRequests
	configSequence ConfigSequence
}

func NewSimpleBatch(shard ShardID, primary PartyID, seq BatchSequence, requests BatchedRequests, configSequence ConfigSequence) *SimpleBatch {
	return &SimpleBatch{
		seq:            seq,
		shard:          shard,
		primary:        primary,
		requests:       requests,
		digest:         requests.Digest(),
		configSequence: configSequence,
	}
}

func (sb *SimpleBatch) Digest() []byte                 { return sb.digest }
func (sb *SimpleBatch) Requests() BatchedRequests      { return sb.requests }
func (sb *SimpleBatch) Primary() PartyID               { return sb.primary }
func (sb *SimpleBatch) Shard() ShardID                 { return sb.shard }
func (sb *SimpleBatch) Seq() BatchSequence             { return sb.seq }
func (sb *SimpleBatch) ConfigSequence() ConfigSequence { return sb.configSequence }
