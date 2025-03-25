package ledger_test

import (
	"testing"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"
	node_ledger "github.ibm.com/decentralized-trust-research/arma/node/ledger"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestAssemblerBlockMetadataToFromBytes(t *testing.T) {
	batchedRequests := types.BatchedRequests{
		[]byte("tx1-1"), []byte("tx2"),
	}

	fb, err := node_ledger.NewFabricBatchFromRequests(1, 2, 3, batchedRequests, []byte("bogus"))
	assert.NoError(t, err)

	oi := &state.OrderingInformation{
		BlockHeader: &state.BlockHeader{
			Number:   0,
			PrevHash: []byte{},
			Digest:   fb.Digest(),
		},
		Signatures: []smartbft_types.Signature{{
			ID:    1,
			Value: []byte("sig1"),
		}, {
			ID:    2,
			Value: []byte("sig2"),
		}},
		DecisionNum: 4,
		BatchIndex:  5,
		BatchCount:  6,
	}

	expectedTransactionCount := uint64(len(batchedRequests))

	metadata, err := node_ledger.AssemblerBlockMetadataToBytes(fb, oi, expectedTransactionCount)
	assert.NoError(t, err)
	primary, shard, seq, num, batchIndex, batchCount, transactionCount, err := node_ledger.AssemblerBlockMetadataFromBytes(metadata)
	assert.NoError(t, err)
	assert.Equal(t, types.PartyID(1), primary)
	assert.Equal(t, types.ShardID(2), shard)
	assert.Equal(t, types.BatchSequence(3), seq)
	assert.Equal(t, oi.DecisionNum, num)
	assert.Equal(t, uint32(oi.BatchIndex), batchIndex)
	assert.Equal(t, uint32(oi.BatchCount), batchCount)
	assert.Equal(t, expectedTransactionCount, transactionCount)
}
