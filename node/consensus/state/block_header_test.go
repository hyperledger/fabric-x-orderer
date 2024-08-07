package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBAHeader(t *testing.T) {
	var bh BlockHeader
	bh.Number = 0
	bh.PrevHash = make([]byte, 32)
	bh.Digest = make([]byte, 32)

	var bh2 BlockHeader
	bh2.deserialize(bh.serialize())

	require.Equal(t, bh, bh2)

	require.Equal(t, bh.Hash(), bh2.Hash())

	var bh3 BlockHeader
	bh3.FromBytes(bh.Bytes())

	require.Equal(t, bh, bh3)

	require.Equal(t, bh.Hash(), bh3.Hash())
}
