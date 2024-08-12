package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/binary"
	"math/big"

	"github.com/pkg/errors"
)

// BlockHeader is compatible with a Fabric block header
// See: https://github.com/hyperledger/fabric/blob/main/protoutil/blockutils.go
type BlockHeader struct {
	Number   uint64
	PrevHash []byte
	Digest   []byte
}

type asn1BlockHeader struct {
	Number   *big.Int
	PrevHash []byte
	Digest   []byte
}

func (bh *BlockHeader) serialize() []byte {
	asn1BlockHeader := asn1BlockHeader{
		Number:   new(big.Int).SetUint64(bh.Number),
		PrevHash: bh.PrevHash,
		Digest:   bh.Digest,
	}
	result, err := asn1.Marshal(asn1BlockHeader)
	if err != nil {
		// Errors should only arise for types which cannot be encoded, since the
		// BlockHeader type is known a-priori to contain only encodable types, an
		// error here is fatal and should not be propagated
		panic(err)
	}
	return result
}

func (bh *BlockHeader) deserialize(bytes []byte) error {
	var asn1BlockHeader asn1BlockHeader
	if _, err := asn1.Unmarshal(bytes, &asn1BlockHeader); err != nil {
		return err
	}
	if !asn1BlockHeader.Number.IsUint64() {
		return errors.Errorf("sequence number cannot be represented as uint64")
	}
	bh.Number = asn1BlockHeader.Number.Uint64()
	bh.PrevHash = asn1BlockHeader.PrevHash
	bh.Digest = asn1BlockHeader.Digest

	return nil
}

func (bh *BlockHeader) Hash() []byte {
	sum := sha256.Sum256(bh.serialize())
	return sum[:]
}

var BlockHeaderBytesSize = 8 + 32 + 32 // uint64 + hash + digest

func (bh *BlockHeader) Bytes() []byte {
	buff := make([]byte, BlockHeaderBytesSize)
	binary.BigEndian.PutUint64(buff, bh.Number)
	copy(buff[8:], bh.PrevHash)
	copy(buff[40:], bh.Digest)
	return buff
}

func (bh *BlockHeader) FromBytes(bytes []byte) error {
	if bytes == nil {
		return errors.Errorf("nil bytes")
	}
	if len(bytes) != BlockHeaderBytesSize {
		return errors.Errorf("len of bytes %d does not equal the block header size %d", len(bytes), BlockHeaderBytesSize)
	}
	bh.Number = binary.BigEndian.Uint64(bytes[0:8])
	bh.PrevHash = bytes[8:40]
	bh.Digest = bytes[40:]
	return nil
}

func (bh *BlockHeader) Equal(bh2 *BlockHeader) bool {
	if bh.Number != bh2.Number {
		return false
	}
	if !bytes.Equal(bh.PrevHash, bh2.PrevHash) {
		return false
	}
	return bytes.Equal(bh.Digest, bh2.Digest)
}
