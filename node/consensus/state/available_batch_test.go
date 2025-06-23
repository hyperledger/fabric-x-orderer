/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAvailableBatches(t *testing.T) {
	var ab AvailableBatch
	ab.digest = make([]byte, 32)
	ab.primary = 42
	ab.shard = 666
	ab.seq = 100
	require.Equal(t, "Sh,Pr,Sq,Dg: <666,42,100,0000000000000000000000000000000000000000000000000000000000000000>", ab.String())

	var ab2 AvailableBatch
	require.NoError(t, ab2.Deserialize(ab.Serialize()))
	require.Equal(t, ab, ab2)
	require.True(t, ab.Equal(&ab2))
	require.True(t, ab2.Equal(&ab))
}
