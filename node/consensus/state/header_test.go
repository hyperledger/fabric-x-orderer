package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeaderBytes(t *testing.T) {
	hdr := Header{
		State: []byte{1, 2, 3},
		Num:   100,
		AvailableBatches: []AvailableBatch{
			NewAvailableBatch(3, 2, 1, make([]byte, 32)),
			NewAvailableBatch(6, 5, 4, make([]byte, 32)),
		},
		BlockHeaders: []BlockHeader{
			{10, make([]byte, 32), make([]byte, 32)},
			{11, make([]byte, 32), make([]byte, 32)},
		},
	}

	var hdr2 Header
	hdr2.FromBytes(hdr.Bytes())

	require.Equal(t, hdr, hdr2)
}
