package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleBAF(t *testing.T) {
	baf := &SimpleBatchAttestationFragment{
		epoch:          1,
		seq:            2,
		primary:        3,
		signer:         4,
		shard:          5,
		digest:         []byte{1, 2, 3, 4},
		signature:      []byte{1, 2, 3, 4, 5, 6, 7, 8},
		garbageCollect: [][]byte{{1, 2, 3, 4}, {5, 6, 7, 8}},
	}

	var bafBytes []byte
	require.NotPanics(t, func() {
		bafBytes = baf.Serialize()
	})

	baf2 := &SimpleBatchAttestationFragment{}
	err := baf2.Deserialize(bafBytes)
	require.NoError(t, err)
	require.Equal(t, baf, baf2)
}
