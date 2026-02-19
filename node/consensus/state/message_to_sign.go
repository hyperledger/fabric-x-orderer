/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"encoding/asn1"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/crypto"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
)

type MessageToSign struct {
	IdentifierHeader     []byte
	BlockHeader          []byte
	OrdererBlockMetadata []byte
}

func (m *MessageToSign) AsBytes() []byte {
	msg2Sign := concatenateBytes(m.OrdererBlockMetadata, m.IdentifierHeader, m.BlockHeader)
	return msg2Sign
}

func (m *MessageToSign) Unmarshal(bytes []byte) error {
	_, err := asn1.Unmarshal(bytes, m)
	return err
}

func (m *MessageToSign) Marshal() []byte {
	bytes, err := asn1.Marshal(*m)
	if err != nil {
		panic(err)
	}
	return bytes
}

// concatenateBytes is useful for combining multiple arrays of bytes, especially for
// signatures or digests over multiple fields
// This way is more efficient in speed
func concatenateBytes(data ...[]byte) []byte {
	finalLength := 0
	for _, slice := range data {
		finalLength += len(slice)
	}
	result := make([]byte, finalLength)
	last := 0
	for _, slice := range data {
		last += copy(result[last:], slice)
	}
	return result
}

// NewIdentifierHeaderOrPanic creates an IdentifierHeader with the signer's identifier and a valid nonce
func NewIdentifierHeaderOrPanic(id arma_types.PartyID) *common.IdentifierHeader {
	nonce := randomNonceOrPanic()
	return &common.IdentifierHeader{
		Identifier: uint32(id),
		Nonce:      nonce,
	}
}

func randomNonceOrPanic() []byte {
	nonce, err := crypto.GetRandomNonce()
	if err != nil {
		panic(err)
	}
	return nonce
}
