package state_test

import (
	"encoding/asn1"
	"testing"

	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"

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

	t.Run("rubbish", func(t *testing.T) {
		cValue := []byte("rubbish")
		cSig := state.CompoundSig{
			ID:    1,
			Value: cValue,
			Msg:   []byte("some message"),
		}

		sigs, err := cSig.UnPack()
		assert.EqualError(t, err, "failed to unmarshal values: asn1: structure error: tags don't match (16 vs {class:1 tag:18 length:117 isCompound:true}) {optional:false explicit:false application:false private:false defaultValue:<nil> tag:<nil> stringType:0 timeType:0 set:false omitEmpty:false}  @2")
		assert.Nil(t, sigs)
	})

	t.Run("only on proposal", func(t *testing.T) {
		cValue, err := asn1.Marshal([][]byte{{1, 2, 3, 4}})
		assert.NoError(t, err)
		cSig := state.CompoundSig{
			ID:    1,
			Value: cValue,
			Msg:   []byte("some message"),
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
		cSig := state.CompoundSig{
			ID:    1,
			Value: cValue,
			Msg:   []byte("some message"),
		}

		sigs, err := cSig.UnPack()
		assert.NoError(t, err)
		assert.Len(t, sigs, 3)

		for i := uint8(0); i < 3; i++ {
			assert.Equal(t, []byte{i + 1, i + 1}, sigs[i].Value)
			assert.Equal(t, uint64(1), sigs[i].ID)
			if i == 0 {
				assert.Equal(t, []byte("some message"), sigs[i].Msg)
				continue
			}
			assert.True(t, len(sigs[i].Msg) == 0)
		}
	})
}

func TestCompoundSig_UnpackBlockHeaderSigs(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		var cSigs []smartbft_types.Signature
		for p := uint8(0); p < 4; p++ { // 4 parties
			cValue, err := asn1.Marshal([][]byte{{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}}) // 7 blocks headers
			assert.NoError(t, err)
			cSig := state.CompoundSig{
				ID:    uint64(p) + 1,
				Value: cValue,
				Msg:   []byte("some message"),
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
			cSig := state.CompoundSig{
				ID:    uint64(p) + 1,
				Value: cValue,
				Msg:   []byte("some message"),
			}
			cSigs = append(cSigs, smartbft_types.Signature(cSig))
		}

		sigsBnP, err := state.UnpackBlockHeaderSigs(cSigs, 7)
		assert.EqualError(t, err, "extracted 5 signatures but expected 8, compound sig: {ID:1 Value:[48 20 4 2 0 0 4 2 1 1 4 2 2 2 4 2 3 3 4 2 4 4] Msg:[115 111 109 101 32 109 101 115 115 97 103 101]}")
		assert.Nil(t, sigsBnP)
	})
}
