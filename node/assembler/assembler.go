/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"encoding/hex"
	"fmt"

	"github.com/hyperledger/fabric/protoutil"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
)

type NetStopper interface {
	Stop()
}

type Assembler struct {
	collator     Collator
	logger       types.Logger
	ds           delivery.DeliverService // TODO the assembler need only one reader, not a map.
	prefetcher   PrefetcherController
	baReplicator delivery.ConsensusBringer
	netStopper   NetStopper
	metrics      *Metrics
}

func (a *Assembler) Broadcast(server orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("should not be used")
}

func (a *Assembler) Deliver(server orderer.AtomicBroadcast_DeliverServer) error {
	return a.ds.Deliver(server)
}

// GetTxCount returns the number of transactions the assembler stored in the ledger. This method is used only in testing.
func (a *Assembler) GetTxCount() uint64 {
	return a.collator.Ledger.(node_ledger.AssemblerLedgerReaderWriter).GetTxCount()
}

// Stop stops the assembler and all its components.
func (a *Assembler) Stop() {
	a.metrics.Stop()
	a.netStopper.Stop()
	a.prefetcher.Stop()
	a.collator.Index.Stop()
	a.baReplicator.Stop()
	a.collator.Stop()
	a.collator.Ledger.Close()
}

func (a *Assembler) SoftStop() {
	a.logger.Infof("Initiating soft stop of assembler")
	a.prefetcher.Stop()
	a.collator.Index.Stop()
	a.baReplicator.Stop()
	a.collator.Stop()
	a.logger.Warnf("Assembler has been partially stopped, delivery service is available. Pending restart")
}

func NewDefaultAssembler(
	logger types.Logger,
	net NetStopper,
	config *config.AssemblerNodeConfig,
	genesisBlock *common.Block,
	assemblerLedgerFactory node_ledger.AssemblerLedgerFactory,
	prefetchIndexFactory PrefetchIndexerFactory,
	prefetcherFactory PrefetcherFactory,
	batchBringerFactory BatchBringerFactory,
	consensusBringerFactory delivery.ConsensusBringerFactory,
) *Assembler {
	logger.Infof("Creating assembler, party: %d, address: %s, with genesis block: %t", config.PartyId, config.ListenAddress, genesisBlock != nil)

	al, err := assemblerLedgerFactory.Create(logger, config.Directory)
	if err != nil {
		logger.Panicf("Failed creating assembler: %v", err)
	}

	metrics := NewMetrics(config, al.Metrics(), logger)

	var transactionCount, blocksCount uint64
	height := al.LedgerReader().Height()
	if height > 0 {
		block, err := al.LedgerReader().RetrieveBlockByNumber(height - 1)
		if err != nil {
			logger.Panicf("error while fetching last block from ledger %v", err)
		}
		_, _, transactionCount, err = node_ledger.AssemblerBatchIdOrderingInfoAndTxCountFromBlock(block)
		if err != nil {
			logger.Panicf("error while fetching last block ordering info %v", err)
		}

		blocksCount = height
	}
	al.Metrics().TransactionCount.Add(float64(transactionCount))
	al.Metrics().BlocksCount.Add(float64(blocksCount))

	logger.Infof("Starting with ledger height: %d", al.LedgerReader().Height())

	if al.LedgerReader().Height() == 0 {
		if genesisBlock == nil {
			logger.Panicf("Error creating Assembler%d, genesis block is nil", config.PartyId)
		}
		al.AppendConfig(genesisBlock, 0)
		logger.Infof("Appended genesis block, header digest: %s", hex.EncodeToString(protoutil.BlockHeaderHash(genesisBlock.GetHeader())))
	}

	shardIds := shardsFromAssemblerConfig(config)
	partyIds := partiesFromAssemblerConfig(config)

	batchFrontier, err := al.BatchFrontier(shardIds, partyIds, config.RestartLedgerScanTimeout)
	if err != nil {
		logger.Panicf("Failed fetching batch frontier: %v", err)
	}
	logger.Infof("Starting with BatchFrontier: %s", node_ledger.BatchFrontierToString(batchFrontier))

	index := prefetchIndexFactory.Create(shardIds, partyIds, logger, config.PrefetchEvictionTtl, config.PrefetchBufferMemoryBytes, config.BatchRequestsChannelSize, &DefaultTimerFactory{}, &DefaultBatchCacheFactory{}, &DefaultPartitionPrefetchIndexerFactory{}, config.PopWaitMonitorTimeout)

	baReplicator := consensusBringerFactory.Create(config.Consenter.TLSCACerts, config.TLSPrivateKeyFile, config.TLSCertificateFile, config.Consenter.Endpoint, al, logger)

	br := batchBringerFactory.Create(batchFrontier, config, logger)

	prefetcher := prefetcherFactory.Create(shardIds, partyIds, index, br, logger)
	prefetcher.Start()

	assembler := &Assembler{
		ds: make(delivery.DeliverService),
		collator: Collator{
			Shards:                            shardIds,
			OrderedBatchAttestationReplicator: baReplicator,
			Index:                             index,
			Logger:                            logger,
			Ledger:                            al,
			ShardCount:                        len(config.Shards),
		},
		logger:       logger,
		netStopper:   net,
		prefetcher:   prefetcher,
		baReplicator: baReplicator,
		metrics:      metrics,
	}

	// TODO: we do not need multiple ledgers in the assembler
	assembler.ds["arma"] = al.LedgerReader()

	assembler.collator.AssemblerRestarter = assembler

	assembler.collator.Run()
	assembler.metrics.Start()

	return assembler
}

func NewAssembler(config *config.AssemblerNodeConfig, net NetStopper, genesisBlock *common.Block, logger types.Logger) *Assembler {
	return NewDefaultAssembler(
		logger,
		net,
		config,
		genesisBlock,
		&node_ledger.DefaultAssemblerLedgerFactory{},
		&DefaultPrefetchIndexerFactory{},
		&DefaultPrefetcherFactory{},
		&DefaultBatchBringerFactory{},
		&delivery.DefaultConsensusBringerFactory{},
	)
}
