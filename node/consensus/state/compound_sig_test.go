/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state_test

import (
	"encoding/asn1"
	"fmt"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestCompoundSig_UnPack(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var cSig state.CompoundSig
		sigs, err := cSig.UnPack()
		assert.EqualError(t, err, "empty signature value")
		assert.Nil(t, sigs)
	})

	t.Run("zero sigs", func(t *testing.T) {
		cValue, err := asn1.Marshal([][]byte{})
		assert.NoError(t, err)
		cSig := state.CompoundSig{
			ID:    1,
			Value: cValue,
			Msg:   []byte("some message"),
		}

		sigs, err := cSig.UnPack()
		assert.EqualError(t, err, "zero signature values")
		assert.Nil(t, sigs)
	})

	t.Run("rubbish value", func(t *testing.T) {
		cValue := []byte("rubbish")
		cSig := state.CompoundSig{
			ID:    1,
			Value: cValue,
			Msg:   []byte("some message"),
		}

		sigs, err := cSig.UnPack()
		assert.ErrorContains(t, err, "failed to unmarshal values: asn1: structure error:")
		assert.Nil(t, sigs)
	})

	t.Run("rubbish msg", func(t *testing.T) {
		cValue, err := asn1.Marshal([][]byte{{1, 2, 3, 4}})
		assert.NoError(t, err)
		cMsg := []byte("rubbish")
		cSig := state.CompoundSig{
			ID:    1,
			Value: cValue,
			Msg:   cMsg,
		}

		sigs, err := cSig.UnPack()
		assert.ErrorContains(t, err, "failed to unmarshal msgs: asn1: structure error:")
		assert.Nil(t, sigs)
	})

	t.Run("mismatched msgs and values count", func(t *testing.T) {
		cValue, err := asn1.Marshal([][]byte{{1, 2, 3, 4}, {5, 6, 7, 8}})
		assert.NoError(t, err)
		cMsg, err := asn1.Marshal([][]byte{[]byte("some message")})
		assert.NoError(t, err)
		cSig := state.CompoundSig{
			ID:    1,
			Value: cValue,
			Msg:   cMsg,
		}

		sigs, err := cSig.UnPack()
		assert.EqualError(t, err, "number of msgs (1) does not match number of values (2)")
		assert.Nil(t, sigs)
	})

	t.Run("only on proposal", func(t *testing.T) {
		cValue, err := asn1.Marshal([][]byte{{1, 2, 3, 4}})
		assert.NoError(t, err)
		cMsg, err := asn1.Marshal([][]byte{[]byte("some message")})
		assert.NoError(t, err)
		cSig := state.CompoundSig{
			ID:    1,
			Value: cValue,
			Msg:   cMsg,
		}

		sigs, err := cSig.UnPack()
		assert.NoError(t, err)
		assert.Len(t, sigs, 1)
		assert.Equal(t, []byte{1, 2, 3, 4}, sigs[0].Value)
		assert.Equal(t, uint64(1), sigs[0].ID)
		assert.Equal(t, []byte("some message"), sigs[0].Msg)
	})

	t.Run("on proposal and blocks", func(t *testing.T) {
		cValue, err := asn1.Marshal([][]byte{{1, 1}, {2, 2}, {3, 3}})
		assert.NoError(t, err)
		cMsg, err := asn1.Marshal([][]byte{[]byte("1"), []byte("2"), []byte("3")})
		assert.NoError(t, err)
		cSig := state.CompoundSig{
			ID:    1,
			Value: cValue,
			Msg:   cMsg,
		}

		sigs, err := cSig.UnPack()
		assert.NoError(t, err)
		assert.Len(t, sigs, 3)

		for i := uint8(0); i < 3; i++ {
			assert.Equal(t, []byte{i + 1, i + 1}, sigs[i].Value)
			assert.Equal(t, uint64(1), sigs[i].ID)
			msg := fmt.Sprintf("%d", i+1)
			assert.Equal(t, []byte(msg), sigs[i].Msg)
		}
	})
}

func TestCompoundSig_UnpackBlockHeaderSigs(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		var cSigs []smartbft_types.Signature
		for p := uint8(0); p < 4; p++ { // 4 parties
			cValue, err := asn1.Marshal([][]byte{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}}) // 7 blocks headers
			assert.NoError(t, err)
			cMsg, err := asn1.Marshal([][]byte{[]byte("0"), []byte("1"), []byte("2"), []byte("3"), []byte("4"), []byte("5"), []byte("6"), []byte("7")})
			assert.NoError(t, err)
			cSig := state.CompoundSig{
				ID:    uint64(p) + 1,
				Value: cValue,
				Msg:   cMsg,
			}
			cSigs = append(cSigs, smartbft_types.Signature(cSig))
		}

		sigsBnP, err := state.UnpackBlockHeaderSigs(cSigs, 7)
		assert.NoError(t, err)
		assert.Len(t, sigsBnP, 7)
		for b := 0; b < 7; b++ {
			assert.Len(t, sigsBnP[b], 4)
			for p := 0; p < 4; p++ {
				assert.Equal(t, uint64(p)+1, sigsBnP[b][p].ID)
			}
		}
	})

	t.Run("zero blocks", func(t *testing.T) {
		var cSigs []smartbft_types.Signature
		for p := uint8(0); p < 5; p++ {
			cValue, err := asn1.Marshal([][]byte{{1, 1}})
			assert.NoError(t, err)
			cSig := state.CompoundSig{
				ID:    uint64(p) + 1,
				Value: cValue,
				Msg:   []byte("some message"),
			}
			cSigs = append(cSigs, smartbft_types.Signature(cSig))
		}

		sigsBnP, err := state.UnpackBlockHeaderSigs(cSigs, 0)
		assert.NoError(t, err)
		assert.Len(t, sigsBnP, 0)
	})

	t.Run("block num mismatch", func(t *testing.T) {
		var cSigs []smartbft_types.Signature
		for p := uint8(0); p < 4; p++ {
			cValue, err := asn1.Marshal([][]byte{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}})
			assert.NoError(t, err)
			cMsg, err := asn1.Marshal([][]byte{[]byte("0"), []byte("1"), []byte("2"), []byte("3"), []byte("4")})
			assert.NoError(t, err)
			cSig := state.CompoundSig{
				ID:    uint64(p) + 1,
				Value: cValue,
				Msg:   cMsg,
			}
			cSigs = append(cSigs, smartbft_types.Signature(cSig))
		}

		sigsBnP, err := state.UnpackBlockHeaderSigs(cSigs, 7)
		assert.EqualError(t, err, "extracted 5 signatures but expected 8, compound sig: {ID:1 Value:[48 20 4 2 0 0 4 2 1 1 4 2 2 2 4 2 3 3 4 2 4 4] Msg:[48 15 4 1 48 4 1 49 4 1 50 4 1 51 4 1 52]}")
		assert.Nil(t, sigsBnP)
	})
}
