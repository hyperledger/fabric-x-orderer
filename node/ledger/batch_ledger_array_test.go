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
	a, err := NewBatchLedgerArray(1, 1, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	list, err := a.List()
	require.NoError(t, err)
	require.Equal(t, []string{"shard1party1", "shard1party2", "shard1party3", "shard1party4"}, list)

	a.Close()
}

func TestBatchLedgerArray(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	t.Log("Open, write & read")
	parties := []types.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 3, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	numBatches := uint64(10)
	var batchedRequests types.BatchedRequests
	for _, pID := range parties {
		for seq := uint64(0); seq < numBatches; seq++ {
			batchedRequests = types.BatchedRequests{
				[]byte(fmt.Sprintf("tx1%d", seq)), []byte(fmt.Sprintf("tx2%d", seq)),
			}
			a.Append(pID, types.BatchSequence(seq), 0, batchedRequests, nil)
			require.Equal(t, seq+1, a.Height(pID))
			batch := a.RetrieveBatchByNumber(pID, seq)
			require.NotNil(t, batch)
			require.Equal(t, batchedRequests, batch.Requests())
			require.Equal(t, pID, batch.Primary())
			require.NotNil(t, batch.Digest())
		}
	}

	t.Log("Close, reopen write and read")
	a.Close()
	a, err = NewBatchLedgerArray(1, 3, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	for _, pID := range parties {
		require.Equal(t, numBatches, a.Height(pID))
		batch := a.RetrieveBatchByNumber(pID, numBatches-1)
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
			a.Append(pID, types.BatchSequence(seq), 0, batchedRequests, nil)
			require.Equal(t, seq+1, a.Height(pID))
			batch := a.RetrieveBatchByNumber(pID, seq)
			require.NotNil(t, batch)
			require.Equal(t, batchedRequests, batch.Requests())
			require.Equal(t, pID, batch.Primary())
			require.NotNil(t, batch.Digest())
		}
	}

	list, err := a.List()
	require.NoError(t, err)
	require.Equal(t, []string{"shard1party1", "shard1party2", "shard1party3", "shard1party4"}, list)

	t.Log("Close, reopen and read with new and old parties")
	a.Close()
	oldParties := parties
	newParty := types.PartyID(5)
	newParties := []types.PartyID{1, 2, 3, newParty}
	a, err = NewBatchLedgerArray(1, 3, newParties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	for _, pID := range oldParties {
		require.Equal(t, 2*numBatches, a.Height(pID))
		batch := a.RetrieveBatchByNumber(pID, 2*numBatches-1)
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
		a.Append(5, types.BatchSequence(seq), 0, batchedRequests, nil)
		require.Equal(t, seq+1, a.Height(newParty))
		batch := a.RetrieveBatchByNumber(newParty, seq)
		require.NotNil(t, batch)
		require.Equal(t, batchedRequests, batch.Requests())
		require.Equal(t, newParty, batch.Primary())
		require.NotNil(t, batch.Digest())
	}

	list, err = a.List()
	require.NoError(t, err)
	require.Equal(t, []string{"shard1party1", "shard1party2", "shard1party3", "shard1party4", "shard1party5"}, list)
}

func TestBatchLedgerArrayPart(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	parties := []types.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 1, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	batchedRequests := types.BatchedRequests{[]byte("tx1"), []byte("tx2")}
	for _, pID := range parties {
		part := a.Part(pID)
		for seq := uint64(0); seq < 10; seq++ {
			part.Append(types.BatchSequence(seq), 0, batchedRequests, nil)
			require.Equal(t, seq+1, part.Height())
			batch := part.RetrieveBatchByNumber(seq)
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
	a, err := NewBatchLedgerArray(1, 1, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	missing := types.PartyID(99)

	// Part should return nil for a non-existent party
	part := a.Part(missing)
	require.Nil(t, part)

	// Height, Append and RetrieveBatchByNumber should panic for non-existent party
	require.Panics(t, func() { _ = a.Height(missing) })

	require.Panics(t, func() {
		a.Append(missing, types.BatchSequence(0), 0, types.BatchedRequests{[]byte("x")}, nil)
	})

	require.Panics(t, func() { _ = a.RetrieveBatchByNumber(missing, 0) })

	a.Close()
}

func TestBatchLedgerArrayWithPrimarySignature(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	parties := []types.PartyID{1, 2, 3, 4}
	a, err := NewBatchLedgerArray(1, 1, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	// Create a batch with a non-nil primary signature
	primarySignature := []byte("test-primary-signature-data")
	batchedRequests := types.BatchedRequests{[]byte("tx1"), []byte("tx2"), []byte("tx3")}
	partyID := types.PartyID(1)
	seq := uint64(0)

	// Append batch with primary signature
	a.Append(partyID, types.BatchSequence(seq), 0, batchedRequests, primarySignature)
	require.Equal(t, uint64(1), a.Height(partyID))

	// Retrieve the batch and verify the primary signature
	batch := a.RetrieveBatchByNumber(partyID, seq)
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

	a.Append(partyID, types.BatchSequence(seq2), 0, batchedRequests2, primarySignature2)
	require.Equal(t, uint64(2), a.Height(partyID))

	// Retrieve the second batch and verify its signature
	batch2 := a.RetrieveBatchByNumber(partyID, seq2)
	require.NotNil(t, batch2)
	require.Equal(t, batchedRequests2, batch2.Requests())
	require.Equal(t, primarySignature2, batch2.PrimarySignature())

	// Verify the first batch signature is still intact
	batch1Again := a.RetrieveBatchByNumber(partyID, seq)
	require.NotNil(t, batch1Again)
	require.Equal(t, primarySignature, batch1Again.PrimarySignature())

	// Close and reopen to verify persistence
	a.Close()
	a, err = NewBatchLedgerArray(1, 1, parties, dir, logger)
	require.NoError(t, err)
	require.NotNil(t, a)

	// Verify signatures are persisted correctly
	batchAfterReopen := a.RetrieveBatchByNumber(partyID, seq)
	require.NotNil(t, batchAfterReopen)
	require.Equal(t, primarySignature, batchAfterReopen.PrimarySignature())

	batch2AfterReopen := a.RetrieveBatchByNumber(partyID, seq2)
	require.NotNil(t, batch2AfterReopen)
	require.Equal(t, primarySignature2, batch2AfterReopen.PrimarySignature())

	a.Close()
}
