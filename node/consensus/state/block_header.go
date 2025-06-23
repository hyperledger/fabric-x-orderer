/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
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

const (
	flagNil  = byte(0)
	flagFull = byte(32)
)

const blockHeaderBytesSize = 8 + 1 + 32 + 1 + 32 // uint64 + flag + hash + flag + digest

func (bh *BlockHeader) Bytes() []byte {
	buff := make([]byte, blockHeaderBytesSize)
	binary.BigEndian.PutUint64(buff, bh.Number)
	encodeHashBytes(buff, 8, bh.PrevHash)
	encodeHashBytes(buff, 41, bh.Digest)

	return buff
}

func (bh *BlockHeader) FromBytes(bytes []byte) error {
	if bytes == nil {
		return errors.Errorf("nil bytes")
	}
	if len(bytes) != blockHeaderBytesSize {
		return errors.Errorf("len of bytes %d does not equal the block header size %d", len(bytes), blockHeaderBytesSize)
	}
	bh.Number = binary.BigEndian.Uint64(bytes[0:8])
	bh.PrevHash = decodeHashBytes(bytes[8:41])
	bh.Digest = decodeHashBytes(bytes[41:])
	return nil
}

// encodeHashBytes encodes a nil or 32 byte hash in a constant size.
// We must be able to encode nil because the fabric ledger expects the first block to have PrevHash=nil.
func encodeHashBytes(buff []byte, pos int, h []byte) {
	if h == nil {
		z := make([]byte, 32)
		buff[pos] = flagNil
		pos++
		copy(buff[pos:], z)
	} else if len(h) == 32 {
		buff[pos] = flagFull
		pos++
		copy(buff[pos:], h)
	} else {
		panic("unsupported hash size")
	}
}

func decodeHashBytes(encoded []byte) []byte {
	if len(encoded) != 33 {
		panic("unexpected encoded hash size")
	}
	if encoded[0] == flagNil {
		return nil
	}

	return encoded[1:]
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

func (bh *BlockHeader) String() string {
	if bh == nil {
		return "<nil>"
	}

	return fmt.Sprintf("Number: %d, PrevHash: %s, Digest: %s", bh.Number, hex.EncodeToString(bh.PrevHash), hex.EncodeToString(bh.Digest))
}
