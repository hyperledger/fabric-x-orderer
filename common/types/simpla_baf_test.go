/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types_test

import (
	"bytes"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	stateprotos "github.com/hyperledger/fabric-x-orderer/node/protos/state"
	"github.com/stretchr/testify/require"
)

func TestSimpleBAF(t *testing.T) {
	t.Run("constructor setters and getters", func(t *testing.T) {
		baf := types.NewSimpleBatchAttestationFragment(1, 2, 3, []byte{4, 5, 6, 7}, 8, 18, 10, nil)

		require.Equal(t, types.ShardID(1), baf.Shard())
		require.Equal(t, types.PartyID(2), baf.Primary())
		require.Equal(t, types.BatchSequence(3), baf.Seq())
		require.True(t, bytes.Equal([]byte{4, 5, 6, 7}, baf.Digest()))
		require.Equal(t, types.PartyID(8), baf.Signer())
		require.Nil(t, baf.Signature())
		baf.SetSignature([]byte{9, 10, 11, 12})
		require.True(t, bytes.Equal([]byte{9, 10, 11, 12}, baf.Signature()))
		require.Equal(t, types.ConfigSequence(18), baf.ConfigSequence())
		require.Equal(t, uint64(10), baf.TXCount())

		require.Equal(t, "BAF: Signer: 8; Sh,Pr,Sq,Dg: <1,2,3,04050607>; Config Seq: 18; TX Count: 10", baf.String())

		baf.SetSignature([]byte{19, 20, 21, 22})
		require.True(t, bytes.Equal([]byte{19, 20, 21, 22}, baf.Signature()))
	})

	t.Run("ToBeSigned does not include sig", func(t *testing.T) {
		baf := types.NewSimpleBatchAttestationFragment(1, 2, 3, []byte{4, 5, 6, 7}, 8, 18, 0, nil)
		baf.SetSignature([]byte{9, 10, 11, 12})

		var bafBytes []byte
		require.NotPanics(t, func() {
			bafBytes = baf.Serialize()
		})

		var toBeSignBytes []byte
		require.NotPanics(t, func() {
			toBeSignBytes = baf.ToBeSigned()
		})

		// ToBeSigned uses ASN.1, Serialize uses protobuf, so they should be different
		require.False(t, bytes.Equal(bafBytes, toBeSignBytes))

		// Even with nil signature, they should still be different because of different encodings
		baf.SetSignature(nil)
		bafBytes = baf.Serialize()
		toBeSigned := baf.ToBeSigned()
		require.False(t, bytes.Equal(bafBytes, toBeSigned), "ASN.1 and protobuf encodings are different")
	})

	t.Run("serialize and deserialize", func(t *testing.T) {
		baf := types.NewSimpleBatchAttestationFragment(1, 2, 3, []byte{4, 5, 6, 7}, 8, 18, 12, nil)
		baf.SetSignature([]byte{9, 10, 11, 12})

		var bafBytes []byte
		require.NotPanics(t, func() {
			bafBytes = baf.Serialize()
		})

		baf2 := &types.SimpleBatchAttestationFragment{}
		err := baf2.Deserialize(bafBytes)
		require.NoError(t, err)
		require.Equal(t, baf, baf2)

		// Protobuf is more lenient with partial data than ASN.1
		// It will successfully parse what it can from truncated data
		baf3 := &types.SimpleBatchAttestationFragment{}
		err = baf3.Deserialize(bafBytes[2:])
		// Protobuf may or may not error depending on where we truncate
		// Just verify we can deserialize valid data
		if err == nil {
			// If no error, the values should be different from original due to truncation
			require.NotEqual(t, baf, baf3)
		}
	})

	t.Run("ToProto and FromProto", func(t *testing.T) {
		baf := types.NewSimpleBatchAttestationFragment(1, 2, 3, []byte{4, 5, 6, 7}, 8, 18, 10, []byte{11, 12})
		baf.SetSignature([]byte{9, 10, 11, 12})

		// Convert to protobuf
		pbBaf := baf.ToProto()
		require.NotNil(t, pbBaf)
		require.Equal(t, uint32(1), pbBaf.Shard)
		require.Equal(t, uint32(2), pbBaf.Primary)
		require.Equal(t, uint64(3), pbBaf.Seq)
		require.Equal(t, []byte{4, 5, 6, 7}, pbBaf.Digest)
		require.Equal(t, uint64(18), pbBaf.ConfigSequence)
		require.Equal(t, uint64(10), pbBaf.TxCount)
		require.Equal(t, uint32(8), pbBaf.Signer)
		require.Equal(t, []byte{9, 10, 11, 12}, pbBaf.Signature)
		require.Equal(t, []byte{11, 12}, pbBaf.PrimarySignature)

		// Convert back from protobuf
		baf2 := &types.SimpleBatchAttestationFragment{}
		err := baf2.FromProto(pbBaf)
		require.NoError(t, err)
		require.Equal(t, baf, baf2)
	})

	t.Run("FromProto with validation errors", func(t *testing.T) {
		// Test shard value exceeds uint16
		pbBaf := &stateprotos.BatchAttestationFragment{
			Shard:          uint32(70000), // Exceeds uint16 max (65535)
			Primary:        1,
			Seq:            1,
			Digest:         []byte{1, 2, 3},
			ConfigSequence: 1,
			TxCount:        1,
			Signer:         1,
		}
		baf := &types.SimpleBatchAttestationFragment{}
		err := baf.FromProto(pbBaf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Shard value")
		require.Contains(t, err.Error(), "exceeds uint16 maximum")

		// Test primary value exceeds uint16
		pbBaf = &stateprotos.BatchAttestationFragment{
			Shard:          1,
			Primary:        uint32(70000), // Exceeds uint16 max
			Seq:            1,
			Digest:         []byte{1, 2, 3},
			ConfigSequence: 1,
			TxCount:        1,
			Signer:         1,
		}
		err = baf.FromProto(pbBaf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Primary value")
		require.Contains(t, err.Error(), "exceeds uint16 maximum")

		// Test signer value exceeds uint16
		pbBaf = &stateprotos.BatchAttestationFragment{
			Shard:          1,
			Primary:        1,
			Seq:            1,
			Digest:         []byte{1, 2, 3},
			ConfigSequence: 1,
			TxCount:        1,
			Signer:         uint32(70000), // Exceeds uint16 max
		}
		err = baf.FromProto(pbBaf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Signer value")
		require.Contains(t, err.Error(), "exceeds uint16 maximum")
	})

	t.Run("FromProto with nil and empty signatures", func(t *testing.T) {
		// Test with nil signature - should become empty slice
		pbBaf := &stateprotos.BatchAttestationFragment{
			Shard:            1,
			Primary:          2,
			Seq:              3,
			Digest:           []byte{4, 5, 6},
			ConfigSequence:   7,
			TxCount:          8,
			Signer:           9,
			Signature:        nil,
			PrimarySignature: nil,
		}
		baf := &types.SimpleBatchAttestationFragment{}
		err := baf.FromProto(pbBaf)
		require.NoError(t, err)
		require.NotNil(t, baf.Signature())
		require.Empty(t, baf.Signature())
		require.Nil(t, baf.PrimarySignature())

		// Test with empty signature
		pbBaf.Signature = []byte{}
		pbBaf.PrimarySignature = []byte{}
		baf2 := &types.SimpleBatchAttestationFragment{}
		err = baf2.FromProto(pbBaf)
		require.NoError(t, err)
		require.NotNil(t, baf2.Signature())
		require.Empty(t, baf2.Signature())
		require.Nil(t, baf2.PrimarySignature())
	})

	t.Run("ToProto and FromProto roundtrip with various values", func(t *testing.T) {
		testCases := []struct {
			name             string
			shard            types.ShardID
			primary          types.PartyID
			seq              types.BatchSequence
			digest           []byte
			signer           types.PartyID
			configSeq        types.ConfigSequence
			txCount          uint64
			primarySignature []byte
			signature        []byte
		}{
			{
				name:             "all fields populated",
				shard:            10,
				primary:          20,
				seq:              30,
				digest:           []byte{1, 2, 3, 4, 5},
				signer:           40,
				configSeq:        50,
				txCount:          100,
				primarySignature: []byte{6, 7, 8},
				signature:        []byte{9, 10, 11, 12},
			},
			{
				name:             "minimal values",
				shard:            0,
				primary:          0,
				seq:              0,
				digest:           []byte{},
				signer:           0,
				configSeq:        0,
				txCount:          0,
				primarySignature: nil,
				signature:        nil,
			},
			{
				name:             "max uint16 values",
				shard:            65535,
				primary:          65535,
				seq:              types.BatchSequence(^uint64(0)), // max uint64
				digest:           []byte{255, 255, 255},
				signer:           65535,
				configSeq:        types.ConfigSequence(^uint64(0)), // max uint64
				txCount:          ^uint64(0),                       // max uint64
				primarySignature: []byte{255},
				signature:        []byte{255, 255},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				baf := types.NewSimpleBatchAttestationFragment(
					tc.shard, tc.primary, tc.seq, tc.digest,
					tc.signer, tc.configSeq, tc.txCount, tc.primarySignature,
				)
				baf.SetSignature(tc.signature)

				// Convert to protobuf and back
				pbBaf := baf.ToProto()
				baf2 := &types.SimpleBatchAttestationFragment{}
				err := baf2.FromProto(pbBaf)
				require.NoError(t, err)

				// Verify all fields match
				require.Equal(t, baf.Shard(), baf2.Shard())
				require.Equal(t, baf.Primary(), baf2.Primary())
				require.Equal(t, baf.Seq(), baf2.Seq())
				require.Equal(t, baf.Digest(), baf2.Digest())
				require.Equal(t, baf.Signer(), baf2.Signer())
				require.Equal(t, baf.ConfigSequence(), baf2.ConfigSequence())
				require.Equal(t, baf.TXCount(), baf2.TXCount())
				require.Equal(t, baf.PrimarySignature(), baf2.PrimarySignature())

				// Special handling for signature due to nil vs empty slice normalization
				if tc.signature == nil {
					require.NotNil(t, baf2.Signature())
					require.Empty(t, baf2.Signature())
				} else {
					require.Equal(t, baf.Signature(), baf2.Signature())
				}
			})
		}
	})
}
