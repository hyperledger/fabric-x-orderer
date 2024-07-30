package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBAHeader(t *testing.T) {
	var bah BAHeader
	bah.Sequence = 0
	bah.PrevHash = make([]byte, 32)
	bah.Digest = make([]byte, 32)

	var bah2 BAHeader
	bah2.Deserialize(bah.Serialize())

	require.Equal(t, bah, bah2)

	require.Equal(t, bah.Hash(), bah2.Hash())
}
