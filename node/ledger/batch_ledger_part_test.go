/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/blkstorage"
	"github.com/hyperledger/fabric-x-orderer/common/types"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/stretchr/testify/require"
)

func TestBatchLedgerPart(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(dir, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{},
	)
	require.NoError(t, err)

	part, err := newBatchLedgerPart(provider, 5, 1, 2, "test-channel", logger)
	require.NoError(t, err)
	require.NotNil(t, part)
	require.Equal(t, uint64(0), part.Height())
	require.Nil(t, part.RetrieveBatchByNumber(0))

	part, err = newBatchLedgerPart(provider, 5, 1, 2, "test-channel", logger) // no problem reopening the same part
	require.NoError(t, err)
	require.NotNil(t, part)
	require.Equal(t, uint64(0), part.Height())
	require.Nil(t, part.RetrieveBatchByNumber(0))

	batches := uint64(10)
	for seq := uint64(0); seq < batches; seq++ {
		batchedRequests := types.BatchedRequests{[]byte(fmt.Sprintf("tx1-%d", seq)), []byte(fmt.Sprintf("tx2-%d", seq))}
		primarySig := []byte(fmt.Sprintf("sig-%d", seq))
		part.Append(types.BatchSequence(seq), types.ConfigSequence(seq*10), batchedRequests, batchedRequests.Digest(), primarySig)
		require.Equal(t, seq+1, part.Height())
		batch := part.RetrieveBatchByNumber(seq)
		require.NotNil(t, batch)
		require.Equal(t, batchedRequests, batch.Requests())
		require.Equal(t, types.PartyID(2), batch.Primary())
		require.Equal(t, types.ShardID(5), batch.Shard())
		require.Equal(t, types.BatchSequence(seq), batch.Seq())
		require.Equal(t, types.ConfigSequence(seq*10), batch.ConfigSequence())
		require.Equal(t, primarySig, batch.PrimarySignature())
		require.Equal(t, batchedRequests.Digest(), batch.Digest())
	}
	require.Nil(t, part.RetrieveBatchByNumber(100))

	part, err = newBatchLedgerPart(provider, 5, 1, 2, "test-channel", logger) // no problem reopening the same part without loosing its content
	require.NoError(t, err)
	require.NotNil(t, part)
	require.Equal(t, batches, part.Height())
	require.NotNil(t, part.RetrieveBatchByNumber(0))
}

// TestBatchLedgerPart_AppendWithDigest verifies that Append persists the digest it was given rather than
// recomputing one over the payload.
func TestBatchLedgerPart_AppendWithDigest(t *testing.T) {
	logger := flogging.MustGetLogger("test")

	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(t.TempDir(), -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{},
	)
	require.NoError(t, err)
	t.Cleanup(provider.Close)

	part, err := newBatchLedgerPart(provider, 5, 1, 2, "test-channel", logger)
	require.NoError(t, err)

	fakeDigest := bytes.Repeat([]byte{0xAB}, sha256.Size)
	reqs := types.BatchedRequests{[]byte("tx1"), []byte("tx2")}
	part.Append(0, 0, reqs, fakeDigest, nil)

	stored := part.RetrieveBatchByNumber(0)
	require.NotNil(t, stored)
	require.Equal(t, fakeDigest, stored.Digest())
	require.NotEqual(t, reqs.Digest(), stored.Digest())
	require.Equal(t, reqs, stored.Requests())
}

func TestBatchLedgerPart_Iterator(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(dir, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{},
	)
	require.NoError(t, err)

	part, err := newBatchLedgerPart(provider, 1, 1, 2, "test-channel", logger)
	require.NoError(t, err)
	require.NotNil(t, part)

	for seq := uint64(0); seq < 10; seq++ {
		batchedRequests := types.BatchedRequests{[]byte(fmt.Sprintf("tx1-%d", seq)), []byte(fmt.Sprintf("tx2-%d", seq))}
		part.Append(types.BatchSequence(seq), 0, batchedRequests, batchedRequests.Digest(), nil)
	}

	ledger := part.Ledger()
	require.NotNil(t, ledger)

	pos := &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: 5}}}
	it, seq := ledger.Iterator(pos)
	require.NotNil(t, it)
	require.Equal(t, uint64(5), seq)
	defer it.Close()

	block, _ := it.Next()
	require.Equal(t, uint64(5), block.GetHeader().GetNumber())
	block, _ = it.Next()
	require.Equal(t, uint64(6), block.GetHeader().GetNumber())
}

// Scenario:
//  1. Append 30 batches to a part.
//  2. Call PruneBefore(20).
//  3. Expect batches below 20 to be unavailable, 20 and above to be returned, and Height to stay 30.
//  4. Call PruneBefore(20) again and PruneBefore(5), and expect neither to change anything.
func TestBatchLedgerPart_PruneBefore(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	array, err := NewBatchLedgerArray(1, 1, []types.PartyID{1}, "test-channel", dir, logger)
	require.NoError(t, err)
	defer array.Close()
	part := array.Part(1)

	const numBatches = 30
	for seq := uint64(0); seq < numBatches; seq++ {
		reqs := types.BatchedRequests{[]byte(fmt.Sprintf("tx-%d", seq))}
		part.Append(types.BatchSequence(seq), 0, reqs, reqs.Digest(), nil)
	}

	require.NoError(t, part.PruneBefore(20))

	require.Nil(t, part.RetrieveBatchByNumber(0))
	require.Nil(t, part.RetrieveBatchByNumber(19))
	require.NotNil(t, part.RetrieveBatchByNumber(20))
	require.NotNil(t, part.RetrieveBatchByNumber(numBatches-1))
	require.Equal(t, uint64(numBatches), part.Height())

	require.NoError(t, part.PruneBefore(20))
	require.NoError(t, part.PruneBefore(5))
	require.Nil(t, part.RetrieveBatchByNumber(19))
	require.NotNil(t, part.RetrieveBatchByNumber(20))
}
