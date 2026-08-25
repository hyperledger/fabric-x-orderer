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
	requireNoBatch(t, part, 0)

	part, err = newBatchLedgerPart(provider, 5, 1, 2, "test-channel", logger) // no problem reopening the same part
	require.NoError(t, err)
	require.NotNil(t, part)
	require.Equal(t, uint64(0), part.Height())
	requireNoBatch(t, part, 0)

	batches := uint64(10)
	for seq := uint64(0); seq < batches; seq++ {
		batchedRequests := types.BatchedRequests{[]byte(fmt.Sprintf("tx1-%d", seq)), []byte(fmt.Sprintf("tx2-%d", seq))}
		primarySig := []byte(fmt.Sprintf("sig-%d", seq))
		part.Append(types.BatchSequence(seq), types.ConfigSequence(seq*10), batchedRequests, batchedRequests.Digest(), primarySig)
		require.Equal(t, seq+1, part.Height())
		batch := mustGetBatch(t, part, seq)
		require.NotNil(t, batch)
		require.Equal(t, batchedRequests, batch.Requests())
		require.Equal(t, types.PartyID(2), batch.Primary())
		require.Equal(t, types.ShardID(5), batch.Shard())
		require.Equal(t, types.BatchSequence(seq), batch.Seq())
		require.Equal(t, types.ConfigSequence(seq*10), batch.ConfigSequence())
		require.Equal(t, primarySig, batch.PrimarySignature())
		require.Equal(t, batchedRequests.Digest(), batch.Digest())
	}
	requireNoBatch(t, part, 100)

	part, err = newBatchLedgerPart(provider, 5, 1, 2, "test-channel", logger) // no problem reopening the same part without loosing its content
	require.NoError(t, err)
	require.NotNil(t, part)
	require.Equal(t, batches, part.Height())
	_ = mustGetBatch(t, part, 0)
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

	stored := mustGetBatch(t, part, 0)
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

	requireNoBatch(t, part, 0)
	requireNoBatch(t, part, 19)
	_ = mustGetBatch(t, part, 20)
	_ = mustGetBatch(t, part, numBatches-1)
	require.Equal(t, uint64(numBatches), part.Height())

	require.NoError(t, part.PruneBefore(20))
	require.NoError(t, part.PruneBefore(5))
	requireNoBatch(t, part, 19)
	_ = mustGetBatch(t, part, 20)
}

// Scenario:
//  1. Append 30 batches to a part and prune below 20.
//  2. Expect retrieving batch 0 to fail with an error matching blkstorage.ErrPruned under errors.Is, so a
//     caller can tell it will never come back.
//  3. Expect retrieving a batch that was never written to fail with an error that does not match ErrPruned.
//  4. Expect the error to name the sequence, the primary and the shard.
func TestBatchLedgerPart_RetrieveBatchByNumberDistinguishesPruned(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	array, err := NewBatchLedgerArray(7, 1, []types.PartyID{1, 3}, "test-channel", dir, logger)
	require.NoError(t, err)
	defer array.Close()
	part := array.Part(3)

	const numBatches = 30
	for seq := uint64(0); seq < numBatches; seq++ {
		reqs := types.BatchedRequests{[]byte(fmt.Sprintf("tx-%d", seq))}
		part.Append(types.BatchSequence(seq), 0, reqs, reqs.Digest(), nil)
	}
	require.NoError(t, part.PruneBefore(20))

	_, err = part.RetrieveBatchByNumber(0)
	require.ErrorIs(t, err, blkstorage.ErrPruned)
	require.ErrorContains(t, err, "failed retrieving batch 0 of primary 3 in shard 7")

	_, err = part.RetrieveBatchByNumber(numBatches + 5)
	require.Error(t, err)
	require.NotErrorIs(t, err, blkstorage.ErrPruned)
}

// mustGetBatch retrieves a batch from a part, failing the test if it cannot be retrieved.
func mustGetBatch(t *testing.T, part *BatchLedgerPart, seq uint64) types.Batch {
	t.Helper()
	batch, err := part.RetrieveBatchByNumber(seq)
	require.NoError(t, err)
	require.NotNil(t, batch)
	return batch
}

// requireNoBatch asserts that a part cannot serve the batch at seq, whatever the reason.
func requireNoBatch(t *testing.T, part *BatchLedgerPart, seq uint64) {
	t.Helper()
	batch, err := part.RetrieveBatchByNumber(seq)
	require.Error(t, err)
	require.Nil(t, batch)
}

// mustGetBatchOf retrieves a batch of a given primary from an array, failing the test on error.
func mustGetBatchOf(t *testing.T, array *BatchLedgerArray, primary types.PartyID, seq uint64) types.Batch {
	t.Helper()
	batch, err := array.RetrieveBatchByNumber(primary, seq)
	require.NoError(t, err)
	require.NotNil(t, batch)
	return batch
}

// requireNoBatchOf asserts that an array cannot serve the batch of a given primary at seq.
func requireNoBatchOf(t *testing.T, array *BatchLedgerArray, primary types.PartyID, seq uint64) {
	t.Helper()
	batch, err := array.RetrieveBatchByNumber(primary, seq)
	require.Error(t, err)
	require.Nil(t, batch)
}
