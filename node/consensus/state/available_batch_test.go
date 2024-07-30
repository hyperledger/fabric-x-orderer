package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAvailableBatches(t *testing.T) {
	var ab AvailableBatch
	ab.digest = make([]byte, 32)
	ab.primary = 42
	ab.shard = 666
	ab.seq = 100

	var ab2 AvailableBatch
	ab2.Deserialize(ab.Serialize())

	require.Equal(t, ab, ab2)
}
