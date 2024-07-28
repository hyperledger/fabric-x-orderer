package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAvailableBatches(t *testing.T) {
	var ab AvailableBatch
	ab.digest = make([]byte, 32)
	ab.primary = 42
	ab.shard = 666
	ab.seq = 100

	var ab2 AvailableBatch
	ab2.Deserialize(ab.Serialize())

	assert.Equal(t, ab, ab2)
}
