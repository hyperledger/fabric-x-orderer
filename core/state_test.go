package core_test

import (
	"testing"

	arma "arma/core"

	"github.com/stretchr/testify/assert"
)

func TestStateSerializeDeserialize(t *testing.T) {
	s := arma.State{
		N:          4,
		Threshold:  2,
		Quorum:     3,
		Shards:     []arma.ShardTerm{{Shard: 1, Term: 1}},
		ShardCount: 1,
		AppContext: make([]byte, 64),
	}

	bytes := s.Serialize()

	s2 := arma.State{}

	s2.Deserialize(bytes, nil)

	assert.Equal(t, s, s2)
}

func TestComplaintSerialization(t *testing.T) {
	c := arma.Complaint{
		ShardTerm: arma.ShardTerm{
			Shard: 1,
			Term:  2,
		},
		Signer:    3,
		Signature: []byte{4},
	}

	var c2 arma.Complaint

	err := c2.FromBytes(c.Bytes())
	assert.NoError(t, err)

	assert.Equal(t, c, c2)
}
