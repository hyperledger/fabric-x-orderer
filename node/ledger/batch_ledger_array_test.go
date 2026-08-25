/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/types"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/stretchr/testify/require"
)

func TestNewBatchLedgerArray(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	parties := []types.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 1, parties, "test-channel", dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	list, err := a.List()
	require.NoError(t, err)
	require.Equal(t, []string{"shard1party1-test-channel", "shard1party2-test-channel", "shard1party3-test-channel", "shard1party4-test-channel"}, list)

	a.Close()
}

func TestBatchLedgerArray(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	t.Log("Open, write & read")
	parties := []types.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 3, parties, "test-channel", dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	numBatches := uint64(10)
	var batchedRequests types.BatchedRequests
	for _, pID := range parties {
		for seq := uint64(0); seq < numBatches; seq++ {
			batchedRequests = types.BatchedRequests{
				[]byte(fmt.Sprintf("tx1%d", seq)), []byte(fmt.Sprintf("tx2%d", seq)),
			}
			a.Append(pID, types.BatchSequence(seq), 0, batchedRequests, batchedRequests.Digest(), nil)
			require.Equal(t, seq+1, a.Height(pID))
			batch := mustGetBatchOf(t, a, pID, seq)
			require.NotNil(t, batch)
			require.Equal(t, batchedRequests, batch.Requests())
			require.Equal(t, pID, batch.Primary())
			require.NotNil(t, batch.Digest())
		}
	}

	t.Log("Close, reopen write and read")
	a.Close()
	a, err = NewBatchLedgerArray(1, 3, parties, "test-channel", dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	for _, pID := range parties {
		require.Equal(t, numBatches, a.Height(pID))
		batch := mustGetBatchOf(t, a, pID, numBatches-1)
		require.NotNil(t, batch)
		require.Equal(t, batchedRequests, batch.Requests())
		require.Equal(t, pID, batch.Primary())
		require.NotNil(t, batch.Digest())
	}

	for _, pID := range parties {
		for seq := numBatches; seq < 2*numBatches; seq++ {
			batchedRequests = types.BatchedRequests{
				[]byte(fmt.Sprintf("tx1%d", seq)), []byte(fmt.Sprintf("tx2%d", seq)),
			}
			a.Append(pID, types.BatchSequence(seq), 0, batchedRequests, batchedRequests.Digest(), nil)
			require.Equal(t, seq+1, a.Height(pID))
			batch := mustGetBatchOf(t, a, pID, seq)
			require.NotNil(t, batch)
			require.Equal(t, batchedRequests, batch.Requests())
			require.Equal(t, pID, batch.Primary())
			require.NotNil(t, batch.Digest())
		}
	}

	list, err := a.List()
	require.NoError(t, err)
	require.Equal(t, []string{"shard1party1-test-channel", "shard1party2-test-channel", "shard1party3-test-channel", "shard1party4-test-channel"}, list)

	t.Log("Close, reopen and read with new and old parties")
	a.Close()
	oldParties := parties
	newParty := types.PartyID(5)
	newParties := []types.PartyID{1, 2, 3, newParty}
	a, err = NewBatchLedgerArray(1, 3, newParties, "test-channel", dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	for _, pID := range oldParties {
		require.Equal(t, 2*numBatches, a.Height(pID))
		batch := mustGetBatchOf(t, a, pID, 2*numBatches-1)
		require.NotNil(t, batch)
		require.Equal(t, batchedRequests, batch.Requests())
		require.Equal(t, pID, batch.Primary())
		require.NotNil(t, batch.Digest())
	}

	require.Zero(t, a.Height(newParty))
	for seq := uint64(0); seq < numBatches; seq++ {
		batchedRequests = types.BatchedRequests{
			[]byte(fmt.Sprintf("tx1%d", seq)), []byte(fmt.Sprintf("tx2%d", seq)),
		}
		a.Append(5, types.BatchSequence(seq), 0, batchedRequests, batchedRequests.Digest(), nil)
		require.Equal(t, seq+1, a.Height(newParty))
		batch := mustGetBatchOf(t, a, newParty, seq)
		require.NotNil(t, batch)
		require.Equal(t, batchedRequests, batch.Requests())
		require.Equal(t, newParty, batch.Primary())
		require.NotNil(t, batch.Digest())
	}

	list, err = a.List()
	require.NoError(t, err)
	require.Equal(t, []string{"shard1party1-test-channel", "shard1party2-test-channel", "shard1party3-test-channel", "shard1party4-test-channel", "shard1party5-test-channel"}, list)
}

func TestBatchLedgerArrayPart(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	parties := []types.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 1, parties, "test-channel", dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	batchedRequests := types.BatchedRequests{[]byte("tx1"), []byte("tx2")}
	for _, pID := range parties {
		part := a.Part(pID)
		for seq := uint64(0); seq < 10; seq++ {
			part.Append(types.BatchSequence(seq), 0, batchedRequests, batchedRequests.Digest(), nil)
			require.Equal(t, seq+1, part.Height())
			batch := mustGetBatch(t, part, seq)
			require.NotNil(t, batch)
			require.Equal(t, batchedRequests, batch.Requests())
			require.Equal(t, pID, batch.Primary())
			require.NotNil(t, batch.Digest())
		}
	}
}

func TestBatchLedgerArrayMissingPartyID(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	parties := []types.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 1, parties, "test-channel", dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	missing := types.PartyID(99)

	// Part should return nil for a non-existent party
	part := a.Part(missing)
	require.Nil(t, part)

	// Height, Append and RetrieveBatchByNumber should panic for non-existent party
	require.Panics(t, func() { _ = a.Height(missing) })

	require.Panics(t, func() {
		a.Append(missing, types.BatchSequence(0), 0, types.BatchedRequests{[]byte("x")}, []byte("digest"), nil)
	})

	require.Panics(t, func() { _, _ = a.RetrieveBatchByNumber(missing, 0) })

	// PruneBefore returns an error .
	require.ErrorContains(t, a.PruneBefore(missing, 0), "partyID does not exist: 99")

	a.Close()
}

func TestBatchLedgerArrayWithPrimarySignature(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	parties := []types.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 1, parties, "test-channel", dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	// Create a batch with a non-nil primary signature
	primarySignature := []byte("test-primary-signature-data")
	batchedRequests := types.BatchedRequests{[]byte("tx1"), []byte("tx2"), []byte("tx3")}
	partyID := types.PartyID(1)
	seq := uint64(0)

	// Append batch with primary signature
	a.Append(partyID, types.BatchSequence(seq), 0, batchedRequests, batchedRequests.Digest(), primarySignature)
	require.Equal(t, uint64(1), a.Height(partyID))

	// Retrieve the batch and verify the primary signature
	batch := mustGetBatchOf(t, a, partyID, seq)
	require.NotNil(t, batch)
	require.Equal(t, batchedRequests, batch.Requests())
	require.Equal(t, partyID, batch.Primary())
	require.NotNil(t, batch.Digest())

	// Verify the primary signature is correctly stored and retrieved
	retrievedSignature := batch.PrimarySignature()
	require.NotNil(t, retrievedSignature)
	require.Equal(t, primarySignature, retrievedSignature)

	// Append another batch with a different signature
	primarySignature2 := []byte("another-signature-12345")
	batchedRequests2 := types.BatchedRequests{[]byte("tx4"), []byte("tx5")}
	seq2 := uint64(1)

	a.Append(partyID, types.BatchSequence(seq2), 0, batchedRequests2, batchedRequests2.Digest(), primarySignature2)
	require.Equal(t, uint64(2), a.Height(partyID))

	// Retrieve the second batch and verify its signature
	batch2 := mustGetBatchOf(t, a, partyID, seq2)
	require.NotNil(t, batch2)
	require.Equal(t, batchedRequests2, batch2.Requests())
	require.Equal(t, primarySignature2, batch2.PrimarySignature())

	// Verify the first batch signature is still intact
	batch1Again := mustGetBatchOf(t, a, partyID, seq)
	require.NotNil(t, batch1Again)
	require.Equal(t, primarySignature, batch1Again.PrimarySignature())

	// Close and reopen to verify persistence
	a.Close()
	a, err = NewBatchLedgerArray(1, 1, parties, "test-channel", dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	// Verify signatures are persisted correctly
	batchAfterReopen := mustGetBatchOf(t, a, partyID, seq)
	require.NotNil(t, batchAfterReopen)
	require.Equal(t, primarySignature, batchAfterReopen.PrimarySignature())

	batch2AfterReopen := mustGetBatchOf(t, a, partyID, seq2)
	require.NotNil(t, batch2AfterReopen)
	require.Equal(t, primarySignature2, batch2AfterReopen.PrimarySignature())

	a.Close()
}

// Scenario:
//  1. Open a four-party array and append 30 batches to each party's ledger.
//  2. Call PruneBefore(party 2, 20).
//  3. Expect batches below the prune point to be gone from party 2 and its Height to stay 30.
//  4. Expect the other parties' ledgers to be untouched, so routing went to the right part.
func TestBatchLedgerArrayPruneBefore(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	parties := []types.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 1, parties, "test-channel", dir, logger)
	require.NoError(t, err)
	defer a.Close()

	const numBatches = 30
	for _, pID := range parties {
		for seq := uint64(0); seq < numBatches; seq++ {
			reqs := types.BatchedRequests{[]byte(fmt.Sprintf("tx-%d-%d", pID, seq))}
			a.Append(pID, types.BatchSequence(seq), 0, reqs, reqs.Digest(), nil)
		}
	}

	const pruned = types.PartyID(2)
	require.NoError(t, a.PruneBefore(pruned, 20))

	requireNoBatchOf(t, a, pruned, 0)
	_ = mustGetBatchOf(t, a, pruned, 20)
	require.Equal(t, uint64(numBatches), a.Height(pruned))

	for _, pID := range parties {
		if pID == pruned {
			continue
		}
		_ = mustGetBatchOf(t, a, pID, 0)
		require.Equal(t, uint64(numBatches), a.Height(pID))
	}
}
