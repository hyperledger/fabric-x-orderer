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
	require.NoError(t, hdr2.FromBytes(hdr.Bytes()))

	require.Equal(t, hdr, hdr2)

	hdr.State = nil
	require.NoError(t, hdr2.FromBytes(hdr.Bytes()))

	require.Equal(t, hdr, hdr2)

	err := hdr.FromBytes([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7})
	require.Error(t, err)
	t.Log(err)

	hdrPanic := Header{
		State: &core.State{AppContext: []byte{}},
		Num:   100,
		AvailableBatches: []AvailableBatch{
			NewAvailableBatch(3, 2, 1, make([]byte, 32)),
		},
		BlockHeaders: []BlockHeader{
			{10, make([]byte, 32), make([]byte, 32)},
			{11, make([]byte, 32), make([]byte, 32)},
		},
	}

	require.Panics(t, func() { hdrPanic.Bytes() })

	// len of rawHeader is just 8 which is not enough
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, 100)
	err = hdr.FromBytes(bytes)
	require.Error(t, err)
	t.Log(err)

	// len of rawHeader is just 12 which is not enough
	bytes = make([]byte, 12)
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint32(bytes[8:], 1)
	err = hdr.FromBytes(bytes)
	require.Error(t, err)
	t.Log(err)

	// len of rawHeader is just 16 which is not enough to read the available batches
	bytes = make([]byte, 16)
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint32(bytes[8:], 1)
	err = hdr.FromBytes(bytes)
	require.Error(t, err)
	t.Log(err)

	// len of rawHeader is just 56 which is not enough to read the length of block headers
	bytes = make([]byte, 8+4+availableBatchSerializedSize)
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint32(bytes[8:], 1)
	ab := NewAvailableBatch(3, 2, 1, make([]byte, 32))
	copy(bytes[12:], ab.Serialize())
	err = hdr.FromBytes(bytes)
	require.Error(t, err)
	t.Log(err)

	// availableBatchCount (1) != blockHeaderCount (2)
	bytes = make([]byte, 8+4+availableBatchSerializedSize+4)
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint32(bytes[8:], 1)
	copy(bytes[12:], ab.Serialize())
	binary.BigEndian.PutUint32(bytes[12+availableBatchSerializedSize:], 2)
	err = hdr.FromBytes(bytes)
	require.Error(t, err)
	t.Log(err)

	// len of rawHeader is just 60 which is not enough to read the block headers
	bytes = make([]byte, 8+4+availableBatchSerializedSize+4)
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint32(bytes[8:], 1)
	copy(bytes[12:], ab.Serialize())
	binary.BigEndian.PutUint32(bytes[12+availableBatchSerializedSize:], 1)
	err = hdr.FromBytes(bytes)
	require.Error(t, err)
	t.Log(err)

	// no error (nil state)
	bytes = make([]byte, 8+4+availableBatchSerializedSize+4+blockHeaderBytesSize)
	binary.BigEndian.PutUint64(bytes, 100)
	binary.BigEndian.PutUint32(bytes[8:], 1)
	copy(bytes[12:], ab.Serialize())
	binary.BigEndian.PutUint32(bytes[12+availableBatchSerializedSize:], 1)
	bh := BlockHeader{10, make([]byte, 32), make([]byte, 32)}
	copy(bytes[12+availableBatchSerializedSize+4:], bh.Bytes())
	err = hdr.FromBytes(bytes)
	require.NoError(t, err)
	require.Nil(t, hdr.State)
}
