package ledger

import (
	"math"
	"slices"
	"sync/atomic"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/node/consensus/state"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protoutil"
)

type AssemblerLedger struct {
	Logger types.Logger
	Ledger blockledger.ReadWriter

	// TODO We need to recover this value when the assembler restarts
	transactionCount uint64
}

func (l *AssemblerLedger) TrackThroughput() {
	// TODO we need to be able to stop this routine
	firstProbe := true
	lastTxCount := uint64(0)
	for {
		txCount := atomic.LoadUint64(&l.transactionCount)
		if !firstProbe {
			l.Logger.Infof("Tx Count: %d, Commit throughput: %.2f", txCount, float64(txCount-lastTxCount)/10.0)
		}
		lastTxCount = txCount
		firstProbe = false
		time.Sleep(time.Second * 10)
	}
}

func (l *AssemblerLedger) GetTxCount() uint64 {
	c := atomic.LoadUint64(&l.transactionCount)
	return c
}

func (l *AssemblerLedger) Append(batch core.Batch, orderingInfo interface{}) {
	ordInfo := orderingInfo.(*state.OrderingInformation)
	t1 := time.Now()
	defer func() {
		l.Logger.Infof("Appended block %d of %d requests to ledger in %v",
			ordInfo.BlockHeader.Number, len(batch.Requests()), time.Since(t1))
	}()

	block := &common.Block{
		Header: &common.BlockHeader{
			Number:       ordInfo.Number, // TODO make sure we start from 0
			DataHash:     ordInfo.Digest,
			PreviousHash: ordInfo.PrevHash,
		},
		Data: &common.BlockData{
			Data: batch.Requests(),
		},
	}

	var metadataContents [][]byte
	for i := 0; i < len(common.BlockMetadataIndex_name); i++ {
		metadataContents = append(metadataContents, []byte{})
	}
	block.Metadata = &common.BlockMetadata{Metadata: metadataContents}

	var sigs []*common.MetadataSignature
	var signers []uint64

	for _, s := range ordInfo.Signatures {
		sigs = append(sigs, &common.MetadataSignature{
			Signature: s.Value,
			IdentifierHeader: protoutil.MarshalOrPanic(&common.IdentifierHeader{
				Identifier: uint32(s.ID),
				Nonce:      []byte{},
			}),
		})

		signers = append(signers, s.ID)
	}

	block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&common.Metadata{
		Signatures: sigs,
	})

	// TODO carry the last config somewhere, we do it the old fabric way
	block.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
		Value: protoutil.MarshalOrPanic(&common.LastConfig{Index: 0}),
	})

	//===
	// TODO Ordering metadata  marshal orderingInfo and batchID
	ordererBlockMetadata, err := AssemblerBlockMetadataToBytes(batch, ordInfo)
	if err != nil {
		l.Logger.Panicf("failed to invoke AssemblerBlockMetadataToBytes: %s", err)
	}
	block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = ordererBlockMetadata

	l.Logger.Debugf("Block: H: %+v; D: %d TXs; M: <primary=%d, shard=%d, seq=%d> <dec=%d, index=%d, count=%d> <signers: %+v>",
		block.Header,                                // Header
		len(block.GetData().GetData()),              // Data
		batch.Primary(), batch.Shard(), batch.Seq(), // Metadata batchID
		ordInfo.DecisionNum, ordInfo.BatchIndex, ordInfo.BatchCount, // Metadata ordering
		signers, // Metadata signers
	)

	if err := l.Ledger.Append(block); err != nil {
		panic(err)
	}

	atomic.AddUint64(&l.transactionCount, uint64(len(batch.Requests())))
}

// LastOrderingInfo returns the ordering information from the last block.
// If the ledger is empty it returns `nil,nil`.
//
// It is typically used in recovery of the assembler, to deduce from where to start consuming decisions from consensus.
func (l *AssemblerLedger) LastOrderingInfo() (*state.OrderingInformation, error) {
	h := l.Ledger.Height()
	if h == 0 {
		return nil, nil
	}

	block, err := l.Ledger.RetrieveBlockByNumber(h - 1)
	if err != nil {
		return nil, err
	}

	_, ordInfo, err := AssemblerBatchIdOrderingInfoFromBlock(block)
	if err != nil {
		return nil, err
	}

	return ordInfo, nil
}

// BatchFrontier retrieves, for each shard and party, the last batch-sequence that was committed.
// It skips config blocks and tries to cover the set {shards}X{parties} before the timeout expires.
func (l *AssemblerLedger) BatchFrontier(
	shards []types.ShardID,
	parties []types.PartyID,
	scanTimeout time.Duration,
) (map[types.ShardID]map[types.PartyID]types.BatchSequence, error) {
	height := l.Ledger.Height()
	if height == 0 {
		return nil, nil
	}

	shardParty2Seq := make(map[types.ShardID]map[types.PartyID]types.BatchSequence)
	count := len(shards) * len(parties)
	deadline := time.Now().Add(scanTimeout)

	for h := height; h > 0; h-- {
		block, err := l.Ledger.RetrieveBlockByNumber(h - 1)
		if err != nil {
			return nil, err
		}
		batchID, _, err := AssemblerBatchIdOrderingInfoFromBlock(block)
		if err != nil {
			return nil, err
		}

		l.Logger.Debugf("Block %d, Sh %d, Pr %d, Seq %d", block.GetHeader().GetNumber(), batchID.Shard(), batchID.Primary(), batchID.Seq())

		// If it is a config block, we skip it
		if batchID.Shard() == math.MaxUint16 || batchID.Primary() == 0 {
			continue
		}

		if _, exists := shardParty2Seq[batchID.Shard()]; !exists {
			shardParty2Seq[batchID.Shard()] = make(map[types.PartyID]types.BatchSequence)
		}

		// If we had already encountered this <shard,primary> we don't count it again
		if _, exists := shardParty2Seq[batchID.Shard()][batchID.Primary()]; exists {
			continue
		}
		// This is a <shard,party> we see for the first time, so we save it
		shardParty2Seq[batchID.Shard()][batchID.Primary()] = batchID.Seq()
		// We only count <shard,party> combinations that are currently configured, as stated in the input parameters.
		// We will return shards and parties that were removed, but try to cover the currently defined space.
		if slices.Contains(parties, batchID.Primary()) && slices.Contains(shards, batchID.Shard()) {
			count--
			if count == 0 {
				break
			}
		}

		if time.Now().After(deadline) {
			break
		}
	}

	return shardParty2Seq, nil
}
