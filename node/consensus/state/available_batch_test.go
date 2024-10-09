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
	require.Equal(t, "Pri 42, Sha 666, Seq 100, Dig 0000000000000000", ab.String())

	var ab2 AvailableBatch
	require.NoError(t, ab2.Deserialize(ab.Serialize()))
	require.Equal(t, ab, ab2)
	require.True(t, ab.Equal(&ab2))
	require.True(t, ab2.Equal(&ab))
}
