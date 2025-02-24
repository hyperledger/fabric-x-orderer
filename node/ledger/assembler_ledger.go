package ledger

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/node/consensus/state"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
	"github.com/hyperledger/fabric/protoutil"
)

//go:generate counterfeiter -o ./mocks/assembler_ledger.go . AssemblerLedgerReaderWriter
type AssemblerLedgerReaderWriter interface {
	GetTxCount() uint64
	Append(batch core.Batch, orderingInfo interface{})
	AppendConfig(configBlock *common.Block, decisionNum types.DecisionNum)
	LastOrderingInfo() (*state.OrderingInformation, error)
	LedgerReader() blockledger.Reader
	BatchFrontier(shards []types.ShardID, parties []types.PartyID, scanTimeout time.Duration) (map[types.ShardID]map[types.PartyID]types.BatchSequence, error)
	Close()
}

//go:generate counterfeiter -o ./mocks/assembler_ledger_factory.go . AssemblerLedgerFactory
type AssemblerLedgerFactory interface {
	Create(logger types.Logger, ledgerPath string) (AssemblerLedgerReaderWriter, error)
}

type DefaultAssemblerLedgerFactory struct{}

func (f *DefaultAssemblerLedgerFactory) Create(logger types.Logger, ledgerPath string) (AssemblerLedgerReaderWriter, error) {
	return NewAssemblerLedger(logger, ledgerPath)
}

type AssemblerLedger struct {
	Logger               types.Logger
	Ledger               blockledger.ReadWriter
	transactionCount     uint64
	blockStorageProvider *blkstorage.BlockStoreProvider
	blockStore           *blkstorage.BlockStore
	cancellationContext  context.Context
	cancelContextFunc    context.CancelFunc
}

func NewAssemblerLedger(logger types.Logger, ledgerPath string) (*AssemblerLedger, error) {
	// Create the ledger
	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(ledgerPath, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{})
	if err != nil {
		logger.Panicf("Failed creating provider: %v", err)
	}
	logger.Infof("Assembler ledger opened block ledger provider, dir: %s", ledgerPath)
	armaLedger, err := provider.Open("arma")
	if err != nil {
		logger.Panicf("Failed opening ledger: %v", err)
	}
	logger.Infof("Assembler ledger opened block store: %+v", armaLedger)
	ledger := fileledger.NewFileLedger(armaLedger)
	transactionCount := uint64(0)
	height := ledger.Height()
	if height > 0 {
		block, err := ledger.RetrieveBlockByNumber(height - 1)
		if err != nil {
			return nil, fmt.Errorf("error while fetching last block from ledger %w", err)
		}
		_, _, transactionCount, err = AssemblerBatchIdOrderingInfoAndTxCountFromBlock(block)
		if err != nil {
			return nil, fmt.Errorf("error while fetching last block ordering info %w", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	al := &AssemblerLedger{
		Logger:               logger,
		Ledger:               ledger,
		transactionCount:     transactionCount,
		blockStorageProvider: provider,
		blockStore:           armaLedger,
		cancellationContext:  ctx,
		cancelContextFunc:    cancel,
	}
	go al.trackThroughput()
	return al, nil
}

func (l *AssemblerLedger) Close() {
	l.cancelContextFunc()
	l.blockStore.Shutdown()
	l.blockStorageProvider.Close()
}

func (l *AssemblerLedger) trackThroughput() {
	firstProbe := true
	lastTxCount := uint64(0)
	for {
		txCount := atomic.LoadUint64(&l.transactionCount)
		if !firstProbe {
			l.Logger.Infof("Tx Count: %d, Commit throughput: %.2f", txCount, float64(txCount-lastTxCount)/10.0)
		}
		lastTxCount = txCount
		firstProbe = false
		select {
		case <-time.After(time.Second * 10):
		case <-l.cancellationContext.Done():
			return
		}
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

	transactionCount := atomic.AddUint64(&l.transactionCount, uint64(len(batch.Requests())))

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
	ordererBlockMetadata, err := AssemblerBlockMetadataToBytes(batch, ordInfo, transactionCount)
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
}

func (l *AssemblerLedger) AppendConfig(configBlock *common.Block, decisionNum types.DecisionNum) {
	t1 := time.Now()
	defer func() {
		l.Logger.Infof("Appended config block %d, decision %d, in %s",
			configBlock.GetHeader().GetNumber(), decisionNum, time.Since(t1))
	}()

	if !protoutil.IsConfigBlock(configBlock) {
		l.Logger.Panicf("attempting to AppendConfig a block which is not a config block: %d", configBlock.GetHeader().GetNumber())
	}

	transactionCount := atomic.AddUint64(&l.transactionCount, 1) // len(configBlock.GetData().GetData()) = should always be a single TX
	batchID := types.NewSimpleBatch(0, types.ShardIDConsensus, 0, nil, nil)
	ordInfo := &state.OrderingInformation{
		DecisionNum: decisionNum,
		BatchIndex:  0,
		BatchCount:  1,
	}
	ordererBlockMetadata, err := AssemblerBlockMetadataToBytes(batchID, ordInfo, transactionCount)
	if err != nil {
		l.Logger.Panicf("failed to invoke AssemblerBlockMetadataToBytes: %s", err)
	}
	configBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = ordererBlockMetadata

	l.Logger.Debugf("Config Block: H: %+v; D: %d TXs; M: <primary=%d, shard=%d, seq=%d> <dec=%d, index=%d, count=%d>",
		configBlock.Header,                                // Header
		len(configBlock.GetData().GetData()),              // Data
		batchID.Primary(), batchID.Shard(), batchID.Seq(), // Metadata batchID
		ordInfo.DecisionNum, ordInfo.BatchIndex, ordInfo.BatchCount, // Metadata ordering
	)

	if err := l.Ledger.Append(configBlock); err != nil {
		panic(err)
	}
}

func (l *AssemblerLedger) LedgerReader() blockledger.Reader {
	return l.Ledger
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

	_, ordInfo, _, err := AssemblerBatchIdOrderingInfoAndTxCountFromBlock(block)
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
		batchID, _, _, err := AssemblerBatchIdOrderingInfoAndTxCountFromBlock(block)
		if err != nil {
			return nil, err
		}

		l.Logger.Debugf("Block %d, Sh %d, Pr %d, Seq %d", block.GetHeader().GetNumber(), batchID.Shard(), batchID.Primary(), batchID.Seq())

		// If it is a config block, we skip it
		if batchID.Shard() == types.ShardIDConsensus || batchID.Primary() == 0 {
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
