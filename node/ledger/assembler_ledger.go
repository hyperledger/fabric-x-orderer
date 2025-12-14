/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
	"context"
	"slices"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blkstorage"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blockledger"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blockledger/fileledger"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	PartySequenceMap = map[types.PartyID]types.BatchSequence
	BatchFrontier    = map[types.ShardID]PartySequenceMap
)

//go:generate counterfeiter -o ./mocks/assembler_ledger.go . AssemblerLedgerReaderWriter
type AssemblerLedgerReaderWriter interface {
	GetTxCount() uint64
	Metrics() *AssemblerLedgerMetrics
	Append(batch types.Batch, orderingInfo *state.OrderingInformation)
	AppendConfig(orderingInfo *state.OrderingInformation)
	LastOrderingInfo() (*state.OrderingInformation, error)
	LedgerReader() blockledger.Reader
	BatchFrontier(shards []types.ShardID, parties []types.PartyID, scanTimeout time.Duration) (BatchFrontier, error)
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
	blockStorageProvider *blkstorage.BlockStoreProvider
	blockStore           *blkstorage.BlockStore
	cancellationContext  context.Context
	cancelContextFunc    context.CancelFunc
	metrics              AssemblerLedgerMetrics
	blockHeaderSize      uint64
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
	channelName := "arma" // TODO this will change when we'll support configurable channel name
	armaLedger, err := provider.Open(channelName)
	if err != nil {
		logger.Panicf("Failed opening ledger: %v", err)
	}
	logger.Infof("Assembler ledger opened block store: path: %s, ledger-ID: %s", ledgerPath, channelName)
	ledger := fileledger.NewFileLedger(armaLedger)

	ctx, cancel := context.WithCancel(context.Background())
	al := &AssemblerLedger{
		Logger:               logger,
		Ledger:               ledger,
		blockStorageProvider: provider,
		blockStore:           armaLedger,
		cancellationContext:  ctx,
		cancelContextFunc:    cancel,
	}

	al.blockHeaderSize = uint64(0)
	return al, nil
}

func (l *AssemblerLedger) Close() {
	l.cancelContextFunc()
	l.blockStore.Shutdown()
	l.blockStorageProvider.Close()
}

func (l *AssemblerLedger) GetTxCount() uint64 {
	return uint64(monitoring.GetMetricValue(l.metrics.TransactionCount.(prometheus.Counter), l.Logger))
}

func (l *AssemblerLedger) Metrics() *AssemblerLedgerMetrics {
	return &l.metrics
}

func (l *AssemblerLedger) estimatedBlockSize(block *common.Block) uint64 {
	blockSize := uint64(0)
	for _, data := range block.GetData().GetData() {
		if len(data) == 0 {
			continue
		}
		blockSize += uint64(len(data))
	}
	if l.blockHeaderSize != 0 {
		l.blockHeaderSize += uint64(len(protoutil.MarshalOrPanic(block.Header)))
	}
	for _, md := range block.GetMetadata().GetMetadata() {
		if len(md) == 0 {
			continue
		}
		blockSize += uint64(len(md))
	}
	return blockSize + l.blockHeaderSize
}

func (l *AssemblerLedger) Append(batch types.Batch, ordInfo *state.OrderingInformation) {
	t1 := time.Now()
	defer func() {
		l.Logger.Infof("Appended block %d of %d requests to ledger in %v",
			ordInfo.CommonBlock.Header.Number, len(batch.Requests()), time.Since(t1))
	}()

	blockToAppend := &common.Block{
		Header: ordInfo.CommonBlock.Header,
		Data: &common.BlockData{
			Data: batch.Requests(),
		},
		Metadata: ordInfo.CommonBlock.Metadata,
	}

	// TODO update the signature on the block in consensus
	sigs, signers := arrangeSignatures(ordInfo)

	blockToAppend.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&common.Metadata{
		Signatures: sigs,
	})

	// TODO update the tx count in the consensus.
	numOfRequests := uint64(len(batch.Requests()))
	newTXcount := numOfRequests + uint64(monitoring.GetMetricValue(l.metrics.TransactionCount.(prometheus.Counter), l.Logger))
	ordererBlockMetadata, err := AssemblerBlockMetadataToBytes(batch, ordInfo, newTXcount)
	if err != nil {
		l.Logger.Panicf("failed to invoke AssemblerBlockMetadataToBytes: %s", err)
	}
	blockToAppend.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = ordererBlockMetadata

	l.Logger.Debugf("Block: H: %+v; D: %d TXs; M: <primary=%d, shard=%d, seq=%d> <dec=%d, index=%d, count=%d> <signers: %+v>",
		blockToAppend.Header,                        // Header
		len(blockToAppend.GetData().GetData()),      // Data
		batch.Primary(), batch.Shard(), batch.Seq(), // Metadata batchID
		ordInfo.DecisionNum, ordInfo.BatchIndex, ordInfo.BatchCount, // Metadata ordering
		signers, // Metadata signers
	)

	if err := l.Ledger.Append(blockToAppend); err != nil {
		panic(err)
	}

	l.metrics.TransactionCount.Add(float64(numOfRequests))
	l.metrics.BlocksSize.Add(float64(l.estimatedBlockSize(blockToAppend)))
	l.metrics.BlocksCount.Add(1)
}

func (l *AssemblerLedger) AppendConfig(orderingInfo *state.OrderingInformation) {
	t1 := time.Now()
	defer func() {
		l.Logger.Infof("Appended config block %d, decision %d, in %s",
			orderingInfo.CommonBlock.GetHeader().GetNumber(), orderingInfo.DecisionNum, time.Since(t1))
	}()

	if orderingInfo == nil {
		l.Logger.Panicf("attempting to AppendConfig with nil OrderingInformation")
	}
	if orderingInfo.CommonBlock == nil {
		l.Logger.Panicf("attempting to AppendConfig with nil block")
	}

	configBlock := orderingInfo.CommonBlock
	if !protoutil.IsConfigBlock(configBlock) {
		l.Logger.Panicf("attempting to AppendConfig a block which is not a config block: %d", configBlock.GetHeader().GetNumber())
	}

	// TODO update the signature on the block in consensus
	sigs, signers := arrangeSignatures(orderingInfo)

	if len(sigs) > 0 {
		configBlock.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&common.Metadata{
			Signatures: sigs,
		})
	}

	// TODO update the tx count in the consensus.
	l.metrics.TransactionCount.Add(1) // for the config TX
	transactionCount := uint64(monitoring.GetMetricValue(l.metrics.TransactionCount.(prometheus.Counter), l.Logger))
	batchID := types.NewSimpleBatch(types.ShardIDConsensus, 0, 0, nil, 0)
	ordererBlockMetadata, err := AssemblerBlockMetadataToBytes(batchID, orderingInfo, transactionCount)
	if err != nil {
		l.Logger.Panicf("failed to invoke AssemblerBlockMetadataToBytes: %s", err)
	}

	configBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = ordererBlockMetadata
	l.Logger.Debugf("Config Block: H: %+v; D: %d TXs; M: <primary=%d, shard=%d, seq=%d> <dec=%d, index=%d, count=%d> <signers: %+v>",
		configBlock.Header,                                // Header
		len(configBlock.GetData().GetData()),              // Data
		batchID.Primary(), batchID.Shard(), batchID.Seq(), // Metadata batchID
		orderingInfo.DecisionNum, orderingInfo.BatchIndex, orderingInfo.BatchCount, // Metadata ordering
		signers, // Metadata signers
	)

	if err := l.Ledger.Append(configBlock); err != nil {
		panic(err)
	}

	l.metrics.BlocksSize.Add(float64(l.estimatedBlockSize(configBlock)))
	l.metrics.BlocksCount.Add(1)
}

func arrangeSignatures(orderingInfo *state.OrderingInformation) ([]*common.MetadataSignature, []uint64) {
	var sigs []*common.MetadataSignature
	var signers []uint64

	for _, s := range orderingInfo.Signatures {
		sigs = append(sigs, &common.MetadataSignature{
			Signature: s.Value,
			IdentifierHeader: protoutil.MarshalOrPanic(&common.IdentifierHeader{
				Identifier: uint32(s.ID),
				Nonce:      []byte{},
			}),
		})

		signers = append(signers, s.ID)
	}
	return sigs, signers
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
) (BatchFrontier, error) {
	height := l.Ledger.Height()
	if height == 0 {
		return nil, nil
	}

	shardParty2Seq := make(BatchFrontier)
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
			shardParty2Seq[batchID.Shard()] = make(PartySequenceMap)
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
