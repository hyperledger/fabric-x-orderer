package state

import (
	"crypto/sha256"
	"encoding/asn1"
	"math/big"

	"github.com/pkg/errors"
)

type BAHeader struct {
	Sequence uint64
	PrevHash []byte
	Digest   []byte
}

type asn1BAHeader struct {
	Sequence *big.Int
	PrevHash []byte
	Digest   []byte
}

func (bah *BAHeader) Serialize() []byte {
	asn1BAHeader := asn1BAHeader{
		Sequence: new(big.Int).SetUint64(bah.Sequence),
		PrevHash: bah.PrevHash,
		Digest:   bah.Digest,
	}
	result, err := asn1.Marshal(asn1BAHeader)
	if err != nil {
		// Errors should only arise for types which cannot be encoded, since the
		// BAHeader type is known a-priori to contain only encodable types, an
		// error here is fatal and should not be propagated
		panic(err)
	}
	return result
}

func (bah *BAHeader) Deserialize(bytes []byte) error {
	var asn1BAHeader asn1BAHeader
	if _, err := asn1.Unmarshal(bytes, &asn1BAHeader); err != nil {
		return err
	}
	if !asn1BAHeader.Sequence.IsUint64() {
		return errors.Errorf("sequence number cannot be represented as uint64")
	}
	bah.Sequence = asn1BAHeader.Sequence.Uint64()
	bah.PrevHash = asn1BAHeader.PrevHash
	bah.Digest = asn1BAHeader.Digest

	return nil
}

func (bah *BAHeader) Hash() []byte {
	sum := sha256.Sum256(bah.Serialize())
	return sum[:]
}
