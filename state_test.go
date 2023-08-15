package arma

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStateSerializeDeserialize(t *testing.T) {
	s := State{
		N:          4,
		Threshold:  2,
		Quorum:     3,
		Shards:     []ShardTerm{{Shard: 1, Term: 1}},
		ShardCount: 1,
	}

	bytes := s.Serialize()

	s2 := State{}

	s2.DeSerialize(bytes, nil)

	assert.Equal(t, s, s2)
}
