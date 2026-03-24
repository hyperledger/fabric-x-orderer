/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/stretchr/testify/require"
)

func TestBatchID_ToString(t *testing.T) {
	baf := types.NewSimpleBatchAttestationFragment(1, 2, 3, []byte{4, 5, 6, 7}, 5, 0, 0)
	str := types.BatchIDToString(baf)
	require.Equal(t, str, "Sh,Pr,Sq,Dg: <1,2,3,04050607>")
}

func TestBatchID_Equals(t *testing.T) {
	baf1 := types.NewSimpleBatchAttestationFragment(1, 2, 3, []byte{4, 5, 6, 7}, 8, 0, 0)

	t.Run("equal", func(t *testing.T) {
		baf2 := types.NewSimpleBatchAttestationFragment(1, 2, 3, []byte{4, 5, 6, 7}, 9, 13, 0)
		require.True(t, types.BatchIDEqual(baf1, baf2))
	})

	t.Run("not equal", func(t *testing.T) {
		baf2 := types.NewSimpleBatchAttestationFragment(111, 2, 3, []byte{4, 5, 6, 7}, 9, 13, 0)
		require.False(t, types.BatchIDEqual(baf1, baf2))
		baf2 = types.NewSimpleBatchAttestationFragment(1, 111, 3, []byte{4, 5, 6, 7}, 9, 13, 0)
		require.False(t, types.BatchIDEqual(baf1, baf2))
		baf2 = types.NewSimpleBatchAttestationFragment(1, 2, 111, []byte{4, 5, 6, 7}, 9, 13, 0)
		require.False(t, types.BatchIDEqual(baf1, baf2))
		baf2 = types.NewSimpleBatchAttestationFragment(1, 2, 3, []byte{111, 5, 6, 7}, 9, 13, 0)
		require.False(t, types.BatchIDEqual(baf1, baf2))
	})
}
