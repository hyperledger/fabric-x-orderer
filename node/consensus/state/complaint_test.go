/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state_test

import (
	"math"
	"testing"

	consensus_state "github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/stretchr/testify/assert"
)

func TestComplaintBytes(t *testing.T) {
	c := consensus_state.Complaint{
		ShardTerm: consensus_state.ShardTerm{
			Shard: 1,
			Term:  2,
		},
		Signer:    3,
		Signature: []byte{4},
		Reason:    "abc",
		ConfigSeq: 20,
	}

	var c2 consensus_state.Complaint

	err := c2.FromBytes(c.Bytes())
	assert.NoError(t, err)

	assert.Equal(t, c, c2)

	// check with no reason
	c.Reason = ""
	err = c2.FromBytes(c.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, c, c2)

	// check with long reason
	longReason := make([]byte, 2*math.MaxUint16)
	c.Reason = string(longReason)
	err = c2.FromBytes(c.Bytes())
	assert.NoError(t, err)

	shorterReason := make([]byte, math.MaxUint16)
	assert.Equal(t, string(shorterReason), c2.Reason)

	assert.Equal(t, c.Signer, c2.Signer)
	assert.Equal(t, c.ShardTerm, c2.ShardTerm)
	assert.Equal(t, c.Signature, c2.Signature)
}

func TestComplaintFromBytesTooSmall(t *testing.T) {
	var c consensus_state.Complaint

	// Anything up to and including the 24-byte fixed header (without the two
	// length-prefixed fields) is not a valid encoding.
	for _, size := range []int{0, 1, 23, 24} {
		err := c.FromBytes(make([]byte, size))
		assert.Errorf(t, err, "expected error for input of size %d", size)
	}
}

func TestComplaintToBeSigned(t *testing.T) {
	c := consensus_state.Complaint{
		ShardTerm: consensus_state.ShardTerm{
			Shard: 1,
			Term:  2,
		},
		Signer:    3,
		Signature: []byte{4, 5, 6},
		Reason:    "abc",
		ConfigSeq: 20,
	}

	// ToBeSigned must exclude the signature: it equals the encoding of the same
	// complaint with a nil signature.
	unsigned := c
	unsigned.Signature = nil
	assert.Equal(t, unsigned.Bytes(), c.ToBeSigned())

	// The result must be independent of the signature so all signers over the
	// same complaint sign identical bytes.
	c2 := c
	c2.Signature = []byte{9, 9, 9, 9}
	assert.Equal(t, c.ToBeSigned(), c2.ToBeSigned())

	// A change to any signed field must change the bytes to be signed.
	c3 := c
	c3.Term = 3
	assert.NotEqual(t, c.ToBeSigned(), c3.ToBeSigned())
}

func TestComplaintString(t *testing.T) {
	c := consensus_state.Complaint{
		ShardTerm: consensus_state.ShardTerm{
			Shard: 7,
			Term:  2,
		},
		Signer:    3,
		Signature: []byte{4},
		Reason:    "primary timeout",
		ConfigSeq: 20,
	}

	str := c.String()
	assert.Contains(t, str, "Signer: 3")
	assert.Contains(t, str, "Shard: 7")
	assert.Contains(t, str, "Term 2")
	assert.Contains(t, str, "Config Seq: 20")
	assert.Contains(t, str, "primary timeout")
}
