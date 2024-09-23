package state

import (
	"encoding/binary"
	"testing"

	"arma/core"

	"github.com/stretchr/testify/require"
)

func TestHeaderBytes(t *testing.T) {
	hdr := Header{
		State: &core.State{AppContext: []byte{}},
		Num:   100,
		AvailableBlocks: []AvailableBlock{
			{Header: BlockHeader{10, make([]byte, 32), make([]byte, 32)}, Batch: NewAvailableBatch(3, 2, 1, make([]byte, 32))},
			{Header: BlockHeader{11, make([]byte, 32), make([]byte, 32)}, Batch: NewAvailableBatch(6, 5, 4, make([]byte, 32))},
		},
	}

	var hdr2 Header
	require.NoError(t, hdr2.Deserialize(hdr.Serialize()))

	require.Equal(t, hdr, hdr2)

	hdr.State = nil
	require.NoError(t, hdr2.Deserialize(hdr.Serialize()))

	require.Equal(t, hdr, hdr2)

	err := hdr.Deserialize([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7})
	require.Error(t, err)
	t.Log(err)

	// len of bytes is just 8 which is not enough
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, 100)
	err = hdr.Deserialize(bytes)
	require.Error(t, err)
	t.Log(err)

	// len of bytes is just 12 which is not enough to read the available blocks
	bytes = make([]byte, 12)
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint32(bytes[8:], 1)
	err = hdr.Deserialize(bytes)
	require.Error(t, err)
	t.Log(err)

	// no error (nil state)
	bytes = make([]byte, 8+4+availableBlockSerializedSize)
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint32(bytes[8:], 1)
	ab := AvailableBlock{Header: BlockHeader{10, make([]byte, 32), make([]byte, 32)}, Batch: NewAvailableBatch(3, 2, 1, make([]byte, 32))}
	copy(bytes[12:], ab.Serialize())
	err = hdr.Deserialize(bytes)
	require.NoError(t, err)
	require.Nil(t, hdr.State)
	t.Log(err)
}
