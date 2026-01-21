/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
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
		}, &disabled.Provider{})
	require.NoError(t, err)

	part, err := newBatchLedgerPart(provider, 5, 1, 2, logger)
	require.NoError(t, err)
	require.NotNil(t, part)
	require.Equal(t, uint64(0), part.Height())
	require.Nil(t, part.RetrieveBatchByNumber(0))

	part, err = newBatchLedgerPart(provider, 5, 1, 2, logger) // no problem reopening the same part
	require.NoError(t, err)
	require.NotNil(t, part)
	require.Equal(t, uint64(0), part.Height())
	require.Nil(t, part.RetrieveBatchByNumber(0))

	batches := uint64(10)
	for seq := uint64(0); seq < batches; seq++ {
		batchedRequests := types.BatchedRequests{[]byte(fmt.Sprintf("tx1-%d", seq)), []byte(fmt.Sprintf("tx2-%d", seq))}
		part.Append(types.BatchSequence(seq), types.ConfigSequence(seq*10), batchedRequests)
		require.Equal(t, seq+1, part.Height())
		batch := part.RetrieveBatchByNumber(seq)
		require.NotNil(t, batch)
		require.Equal(t, batchedRequests, batch.Requests())
		require.Equal(t, types.PartyID(2), batch.Primary())
		require.Equal(t, types.ShardID(5), batch.Shard())
		require.Equal(t, types.BatchSequence(seq), batch.Seq())
		require.Equal(t, types.ConfigSequence(seq*10), batch.ConfigSequence())
		require.NotNil(t, batch.Digest())
	}
	require.Nil(t, part.RetrieveBatchByNumber(100))

	part, err = newBatchLedgerPart(provider, 5, 1, 2, logger) // no problem reopening the same part without loosing its content
	require.NoError(t, err)
	require.NotNil(t, part)
	require.Equal(t, batches, part.Height())
	require.NotNil(t, part.RetrieveBatchByNumber(0))
}

func TestBatchLedgerPart_Iterator(t *testing.T) {
	dir := t.TempDir()
	logger := flogging.MustGetLogger("test")

	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(dir, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{})
	require.NoError(t, err)

	part, err := newBatchLedgerPart(provider, 1, 1, 2, logger)
	require.NoError(t, err)
	require.NotNil(t, part)

	for seq := uint64(0); seq < 10; seq++ {
		batchedRequests := types.BatchedRequests{[]byte(fmt.Sprintf("tx1-%d", seq)), []byte(fmt.Sprintf("tx2-%d", seq))}
		part.Append(types.BatchSequence(seq), 0, batchedRequests)
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
