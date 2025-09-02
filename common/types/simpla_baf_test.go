/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types_test

import (
	"bytes"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/stretchr/testify/require"
)

func TestSimpleBAF(t *testing.T) {
	t.Run("constructor setters and getters", func(t *testing.T) {
		baf := types.NewSimpleBatchAttestationFragment(1, 2, 3, []byte{4, 5, 6, 7}, 8, nil, 13, [][]byte{{14, 15}, {16, 17}}, 18)

		require.Equal(t, types.ShardID(1), baf.Shard())
		require.Equal(t, types.PartyID(2), baf.Primary())
		require.Equal(t, types.BatchSequence(3), baf.Seq())
		require.True(t, bytes.Equal([]byte{4, 5, 6, 7}, baf.Digest()))
		require.Equal(t, types.PartyID(8), baf.Signer())
		require.Nil(t, baf.Signature())
		baf.SetSignature([]byte{9, 10, 11, 12})
		require.True(t, bytes.Equal([]byte{9, 10, 11, 12}, baf.Signature()))

		require.Equal(t, int64(13), baf.Epoch())
		require.Len(t, baf.GarbageCollect(), 2)
		require.True(t, bytes.Equal([]byte{14, 15}, baf.GarbageCollect()[0]))
		require.True(t, bytes.Equal([]byte{16, 17}, baf.GarbageCollect()[1]))

		require.Equal(t, types.ConfigSequence(18), baf.ConfigSequence())

		require.Equal(t, "BAF: Signer: 8; Sh,Pr,Sq,Dg: <1,2,3,04050607>", baf.String())

		baf.SetSignature([]byte{19, 20, 21, 22})
		require.True(t, bytes.Equal([]byte{19, 20, 21, 22}, baf.Signature()))
	})

	t.Run("ToBeSigned does not include sig", func(t *testing.T) {
		baf := types.NewSimpleBatchAttestationFragment(1, 2, 3, []byte{4, 5, 6, 7}, 8, nil, 13, [][]byte{{14, 15}, {16, 17}}, 18)
		baf.SetSignature([]byte{9, 10, 11, 12})

		var bafBytes []byte
		require.NotPanics(t, func() {
			bafBytes = baf.Serialize()
		})

		var toBeSignBytes []byte
		require.NotPanics(t, func() {
			toBeSignBytes = baf.ToBeSigned()
		})

		require.False(t, bytes.Equal(bafBytes, toBeSignBytes))

		baf.SetSignature(nil)
		bafBytes = baf.Serialize()
		toBeSigned := baf.ToBeSigned()
		require.True(t, bytes.Equal(bafBytes, toBeSigned), "with nil sig it should be equal")
	})

	t.Run("serialize and deserialize", func(t *testing.T) {
		baf := types.NewSimpleBatchAttestationFragment(1, 2, 3, []byte{4, 5, 6, 7}, 8, nil, 13, [][]byte{{14, 15}, {16, 17}}, 18)
		baf.SetSignature([]byte{9, 10, 11, 12})

		var bafBytes []byte
		require.NotPanics(t, func() {
			bafBytes = baf.Serialize()
		})

		baf2 := &types.SimpleBatchAttestationFragment{}
		err := baf2.Deserialize(bafBytes)
		require.NoError(t, err)
		require.Equal(t, baf, baf2)

		baf3 := &types.SimpleBatchAttestationFragment{}
		err = baf3.Deserialize(bafBytes[2:])
		require.Error(t, err)
		require.Contains(t, err.Error(), "asn1: structure error:")
	})
}
