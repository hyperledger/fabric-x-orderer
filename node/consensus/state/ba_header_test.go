package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBAHeader(t *testing.T) {
	var babh BABlockHeader
	babh.Number = 0
	babh.PrevHash = make([]byte, 32)
	babh.Digest = make([]byte, 32)

	var babh2 BABlockHeader
	babh2.Deserialize(babh.Serialize())

	require.Equal(t, babh, babh2)

	require.Equal(t, babh.Hash(), babh2.Hash())
}
