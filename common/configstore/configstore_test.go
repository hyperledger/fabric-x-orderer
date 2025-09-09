/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configstore_test

import (
	"bytes"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStore(t *testing.T) {
	dir := t.TempDir()
	t.Run("fresh", func(t *testing.T) {
		s, err := configstore.NewStore(dir)
		assert.NoError(t, err)
		assert.NotNil(t, s)
		nums, err := s.ListBlockNumbers()
		assert.NoError(t, err)
		assert.Len(t, nums, 0)
	})

	t.Run("existing", func(t *testing.T) {
		s, err := configstore.NewStore(dir)
		assert.NoError(t, err)
		assert.NotNil(t, s)
		nums, err := s.ListBlockNumbers()
		assert.NoError(t, err)
		assert.Len(t, nums, 0)
	})
}

func TestStore_Add(t *testing.T) {
	s, err := configstore.NewStore(t.TempDir())
	assert.NoError(t, err)
	assert.NotNil(t, s)

	block0 := &common.Block{Header: &common.BlockHeader{Number: 0}}

	t.Run("first block", func(t *testing.T) {
		err = s.Add(block0)
		assert.NoError(t, err)

		b0, err := s.GetByNumber(0)
		assert.NoError(t, err)
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(b0), protoutil.MarshalOrPanic(block0)))

		nums, err := s.ListBlockNumbers()
		assert.NoError(t, err)
		assert.Equal(t, []uint64{0}, nums)

		last, err := s.Last()
		assert.NoError(t, err)
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(last), protoutil.MarshalOrPanic(block0)))

		blocks, err := s.ListBlocks()
		assert.NoError(t, err)
		require.Len(t, blocks, 1)
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(blocks[0]), protoutil.MarshalOrPanic(block0)))
	})

	t.Run("already exists", func(t *testing.T) {
		err = s.Add(block0)
		assert.EqualError(t, err, "failed to add block 0: file already exists")
	})

	t.Run("nil block", func(t *testing.T) {
		err = s.Add(nil)
		assert.EqualError(t, err, "block is nil")
	})

	t.Run("missing header", func(t *testing.T) {
		err = s.Add(&common.Block{})
		assert.EqualError(t, err, "block header is nil")
	})
}

func TestStore_Get(t *testing.T) {
	dir := t.TempDir()
	s, err1 := configstore.NewStore(dir)
	assert.NoError(t, err1)
	assert.NotNil(t, s)

	block0 := &common.Block{Header: &common.BlockHeader{Number: 0}}
	block5 := &common.Block{Header: &common.BlockHeader{Number: 5}}
	block10 := &common.Block{Header: &common.BlockHeader{Number: 10}}

	t.Run("empty store", func(t *testing.T) {
		nums, err := s.ListBlockNumbers()
		assert.NoError(t, err)
		assert.Len(t, nums, 0)

		blocks, err := s.ListBlocks()
		assert.NoError(t, err)
		assert.Len(t, blocks, 0)

		block, err := s.Last()
		assert.EqualError(t, err, "config store is empty")
		assert.Nil(t, block)

		block, err = s.GetByNumber(0)
		assert.Contains(t, err.Error(), "failed reading block block-0:")
		assert.Contains(t, err.Error(), "no such file or directory")
		assert.Nil(t, block)
	})

	t.Run("more blocks", func(t *testing.T) {
		err := s.Add(block0)
		assert.NoError(t, err)

		err = s.Add(block5)
		assert.NoError(t, err)

		err = s.Add(block10)
		assert.NoError(t, err)

		b5, err := s.GetByNumber(5)
		assert.NoError(t, err)
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(b5), protoutil.MarshalOrPanic(block5)))

		b10, err := s.GetByNumber(10)
		assert.NoError(t, err)
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(b10), protoutil.MarshalOrPanic(block10)))

		nums, err := s.ListBlockNumbers()
		assert.NoError(t, err)
		assert.Equal(t, []uint64{0, 5, 10}, nums) // Should be sorted

		last, err := s.Last()
		assert.NoError(t, err)
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(last), protoutil.MarshalOrPanic(block10)))

		blocks, err := s.ListBlocks()
		assert.NoError(t, err)
		require.Len(t, blocks, 3)
		// Should be sorted
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(blocks[0]), protoutil.MarshalOrPanic(block0)))
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(blocks[1]), protoutil.MarshalOrPanic(block5)))
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(blocks[2]), protoutil.MarshalOrPanic(block10)))
	})

	t.Run("reopen with blocks", func(t *testing.T) {
		s2, err := configstore.NewStore(dir)
		assert.NoError(t, err)
		assert.NotNil(t, s2)

		blocks, err := s2.ListBlocks()
		assert.NoError(t, err)
		require.Len(t, blocks, 3)
		// Should be sorted
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(blocks[0]), protoutil.MarshalOrPanic(block0)))
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(blocks[1]), protoutil.MarshalOrPanic(block5)))
		assert.True(t, bytes.Equal(protoutil.MarshalOrPanic(blocks[2]), protoutil.MarshalOrPanic(block10)))
	})
}
