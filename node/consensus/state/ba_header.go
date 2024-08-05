package state

import (
	"crypto/sha256"
	"encoding/asn1"
	"math/big"

	"github.com/pkg/errors"
)

// BABlockHeader is compatible with a Fabric block header
// See: https://github.com/hyperledger/fabric/blob/main/protoutil/blockutils.go
type BABlockHeader struct {
	Number   uint64
	PrevHash []byte
	Digest   []byte
}

type asn1BABlockHeader struct {
	Number   *big.Int
	PrevHash []byte
	Digest   []byte
}

func (babh *BABlockHeader) Serialize() []byte {
	asn1BABlockHeader := asn1BABlockHeader{
		Number:   new(big.Int).SetUint64(babh.Number),
		PrevHash: babh.PrevHash,
		Digest:   babh.Digest,
	}
	result, err := asn1.Marshal(asn1BABlockHeader)
	if err != nil {
		// Errors should only arise for types which cannot be encoded, since the
		// BABlockHeader type is known a-priori to contain only encodable types, an
		// error here is fatal and should not be propagated
		panic(err)
	}
	return result
}

func (babh *BABlockHeader) Deserialize(bytes []byte) error {
	var asn1BABlockHeader asn1BABlockHeader
	if _, err := asn1.Unmarshal(bytes, &asn1BABlockHeader); err != nil {
		return err
	}
	if !asn1BABlockHeader.Number.IsUint64() {
		return errors.Errorf("sequence number cannot be represented as uint64")
	}
	babh.Number = asn1BABlockHeader.Number.Uint64()
	babh.PrevHash = asn1BABlockHeader.PrevHash
	babh.Digest = asn1BABlockHeader.Digest

	return nil
}

func (babh *BABlockHeader) Hash() []byte {
	sum := sha256.Sum256(babh.Serialize())
	return sum[:]
}
