package ledger

import (
	"encoding/binary"

	"arma/common/types"
	"arma/node/consensus/state"

	"github.com/pkg/errors"
)

// uint16 + uint16 + uint64 + uint64 + uint32 + uint32
const assemblerBlockMetadataSerializedSize = 2 + 2 + 8 + 8 + 4 + 4

func AssemblerBlockMetadataToBytes(batchID types.BatchID, orderingInfo *state.OrderingInformation) ([]byte, error) {
	if batchID == nil {
		return nil, errors.Errorf("nil batchID")
	}
	if orderingInfo == nil {
		return nil, errors.Errorf("nil orderingInfo")
	}

	buff := make([]byte, assemblerBlockMetadataSerializedSize)
	var pos int
	binary.BigEndian.PutUint16(buff[pos:], uint16(batchID.Primary()))
	pos += 2
	binary.BigEndian.PutUint16(buff[pos:], uint16(batchID.Shard()))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], uint64(batchID.Seq()))
	pos += 8

	binary.BigEndian.PutUint64(buff[pos:], uint64(orderingInfo.DecisionNum))
	pos += 8
	binary.BigEndian.PutUint32(buff[pos:], uint32(orderingInfo.BatchIndex))
	pos += 4
	binary.BigEndian.PutUint32(buff[pos:], uint32(orderingInfo.BatchCount))

	return buff, nil
}

func AssemblerBlockMetadataFromBytes(metadata []byte) (primary types.PartyID, shard types.ShardID, seq types.BatchSequence, num types.DecisionNum, batchIndex, batchCount uint32, err error) {
	if metadata == nil {
		return 0, 0, 0, 0, 0, 0, errors.Errorf("nil bytes")
	}
	if len(metadata) < assemblerBlockMetadataSerializedSize {
		return 0, 0, 0, 0, 0, 0, errors.Errorf("len of metadata %d smaller than expected size %d", len(metadata), assemblerBlockMetadataSerializedSize)
	}

	primary = types.PartyID(binary.BigEndian.Uint16(metadata[0:2]))
	shard = types.ShardID(binary.BigEndian.Uint16(metadata[2:4]))
	seq = types.BatchSequence(binary.BigEndian.Uint64(metadata[4:12]))

	num = types.DecisionNum(binary.BigEndian.Uint64(metadata[12:20]))
	batchIndex = (binary.BigEndian.Uint32(metadata[20:24]))
	batchCount = (binary.BigEndian.Uint32(metadata[24:28]))

	return
}
