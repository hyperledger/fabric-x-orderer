package types

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/pkg/errors"
)

// BatchedRequests are used to represent a batch of requests.
//
// The requests can be:
// 1. Client TXs - in the batchers and assemblers
// 2. Consensus requests, .i.e. control events submitted from batchers to consensus.
type BatchedRequests [][]byte

func (br *BatchedRequests) Serialize() []byte {
	if br == nil || len(*br) == 0 {
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
	if br == nil {
		return errors.New("acceptor is nil")
	}

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

// Digest calculates a sha256 digest on a safe representation of the BatchedRequests.
// This is equivalent to BatchRequestsDataHashWithSerialize, yet faster, and consumes less extra memory.
func (br *BatchedRequests) Digest() []byte {
	if br == nil {
		return sha256.New().Sum(nil)
	}

	sizeBuff := make([]byte, 4)
	h := sha256.New()
	for _, r := range *br {
		binary.BigEndian.PutUint32(sizeBuff, uint32(len(r)))
		h.Write(sizeBuff)
		h.Write(r)
	}

	return h.Sum(nil)
}

// BatchRequestsDataHashWithSerialize is a reference implementation for how Digest should work, if computed with
// serialization. It is used only in tests, as it is slower that the cumulative version implemented in Digest(), above.
func BatchRequestsDataHashWithSerialize(br BatchedRequests) []byte {
	digest := sha256.Sum256(br.Serialize())
	return digest[:]
}
