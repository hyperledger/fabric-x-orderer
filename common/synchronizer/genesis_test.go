/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer_test

import (
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/synchronizer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func newGenesisBlock(number uint64, dataHash []byte) *common.Block {
	return &common.Block{
		Header: &common.BlockHeader{Number: number, PreviousHash: []byte{0}, DataHash: dataHash},
	}
}

func TestSelectGenesisBlock(t *testing.T) {
	logger := flogging.MustGetLogger("synchronizer.genesis.test")

	t.Run("returns block agreed by a Byzantine quorum (F+1)", func(t *testing.T) {
		// cluster size 4 => F=1, required matches = F+1 = 2.
		agreed := newGenesisBlock(0, []byte{1, 2, 3})
		odd := newGenesisBlock(0, []byte{9, 9, 9})
		genesisByEndpoint := map[string]*common.Block{
			"ep1": newGenesisBlock(0, []byte{1, 2, 3}),
			"ep2": agreed,
			"ep3": odd,
		}

		block, err := synchronizer.SelectGenesisBlock(logger, genesisByEndpoint, 4)
		require.NoError(t, err)
		require.NotNil(t, block)
		require.True(t, proto.Equal(agreed, block))
	})

	t.Run("ignores nil blocks and still finds a quorum", func(t *testing.T) {
		// cluster size 4 => required matches = 2.
		genesisByEndpoint := map[string]*common.Block{
			"ep1": newGenesisBlock(0, []byte{1, 2, 3}),
			"ep2": newGenesisBlock(0, []byte{1, 2, 3}),
			"ep3": nil,
			"ep4": nil,
		}

		block, err := synchronizer.SelectGenesisBlock(logger, genesisByEndpoint, 4)
		require.NoError(t, err)
		require.NotNil(t, block)
		require.True(t, proto.Equal(newGenesisBlock(0, []byte{1, 2, 3}), block))
	})

	t.Run("errors when no block reaches the required matches", func(t *testing.T) {
		// cluster size 4 => required matches = 2, but every block is distinct.
		genesisByEndpoint := map[string]*common.Block{
			"ep1": newGenesisBlock(0, []byte{1}),
			"ep2": newGenesisBlock(0, []byte{2}),
			"ep3": newGenesisBlock(0, []byte{3}),
		}

		block, err := synchronizer.SelectGenesisBlock(logger, genesisByEndpoint, 4)
		require.Error(t, err)
		require.Nil(t, block)
		require.Contains(t, err.Error(), "could not find genesis block")
	})

	t.Run("errors on empty endpoint map", func(t *testing.T) {
		block, err := synchronizer.SelectGenesisBlock(logger, map[string]*common.Block{}, 4)
		require.Error(t, err)
		require.Nil(t, block)
	})

	t.Run("errors when all blocks are nil", func(t *testing.T) {
		genesisByEndpoint := map[string]*common.Block{
			"ep1": nil,
			"ep2": nil,
		}

		block, err := synchronizer.SelectGenesisBlock(logger, genesisByEndpoint, 4)
		require.Error(t, err)
		require.Nil(t, block)
	})

	t.Run("errors on non-positive cluster size instead of underflowing", func(t *testing.T) {
		genesisByEndpoint := map[string]*common.Block{
			"ep1": newGenesisBlock(0, []byte{1, 2, 3}),
			"ep2": newGenesisBlock(0, []byte{1, 2, 3}),
		}

		for _, clusterSize := range []int{0, -1} {
			block, err := synchronizer.SelectGenesisBlock(logger, genesisByEndpoint, clusterSize)
			require.Error(t, err)
			require.Nil(t, block)
			require.Contains(t, err.Error(), "invalid cluster size")
		}
	})
}
