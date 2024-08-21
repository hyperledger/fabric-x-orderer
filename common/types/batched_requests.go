package types

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

type BatchedRequests [][]byte

func (br *BatchedRequests) Serialize() []byte {
	if len(*br) == 0 {
		return nil
	}

	var reqsSize int
	for _, req := range *br {
		reqsSize += len(req)
	}

	reqsSize += 4 * len(*br)

	buff := make([]byte, reqsSize)

	var pos int
	for _, req := range *br {
		sizeBuff := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBuff, uint32(len(req)))
		copy(buff[pos:], sizeBuff)
		pos += 4
		copy(buff[pos:], req)
		pos += len(req)
	}

	return buff
}

func (br *BatchedRequests) Deserialize(bytes []byte) error {
	*br = BatchedRequests{}
	if len(bytes) == 0 {
		return errors.Errorf("nil bytes")
	}
	for len(bytes) > 0 {
		if len(bytes) < 4 {
			return errors.Errorf("size of req is not encoded correctly")
		}
		size := binary.BigEndian.Uint32(bytes[0:4])
		bytes = bytes[4:]
		if len(bytes) < int(size) {
			return errors.Errorf("there is no req with a size of %d", size)
		}
		req := bytes[0:size]
		*br = append(*br, req)
		bytes = bytes[size:]
	}

	return nil
}
