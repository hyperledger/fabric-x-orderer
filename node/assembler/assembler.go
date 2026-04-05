/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"

	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric/protoutil"
)

type Net interface {
	Stop()
	Address() string
}

type Assembler struct {
	logger       *flogging.FabricLogger
	ledger       node_ledger.AssemblerLedgerReaderWriter
	mainExitChan chan struct{}

	lock                  sync.RWMutex
	collator              Collator
	prefetcher            PrefetcherController
	status                utils.NodeStatus
	net                   Net
	assemblerNodeConfig   *node_config.AssemblerNodeConfig
	lastConfigBlockNumber uint64
	metrics               *Metrics
	ds                    *AssemblerDeliverService
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
	a.lock.Lock()
	defer a.lock.Unlock()

	state := a.status.GetState()
	if state == utils.StateStopped {
		return
	}

	a.logger.Infof("Stopping assembler")

	if state != utils.StateSoftStopped {
		a.prefetcher.Stop()
		a.collator.Stop()
	}

	a.metrics.Stop()
	a.net.Stop()
	a.collator.Ledger.Close()

	a.status.SetState(utils.StateStopped)
	a.logger.Info("Assembler has been stopped")
	// close the whole process.
	close(a.mainExitChan)
}

func (a *Assembler) SoftStop() {
	a.lock.Lock()
	defer a.lock.Unlock()

	state := a.status.GetState()
	if state == utils.StateStopped || state == utils.StateSoftStopped {
		return
	}

	a.logger.Infof("Initiating soft stop of assembler")
	a.prefetcher.Stop()
	a.collator.Stop()

	a.status.SetState(utils.StateSoftStopped)
	a.logger.Warnf("Assembler has been partially stopped, delivery service is available. Pending restart")
}

func (a *Assembler) ConfigBlockNumber() uint64 {
	return a.lastConfigBlockNumber
}

func NewDefaultAssembler(
	logger *flogging.FabricLogger,
	net Net,
	nodeConfig *node_config.AssemblerNodeConfig,
	configBlock *common.Block,
	mainExitChan chan struct{},
	assemblerLedgerFactory node_ledger.AssemblerLedgerFactory,
	prefetchIndexFactory PrefetchIndexerFactory,
	prefetcherFactory PrefetcherFactory,
	batchBringerFactory BatchBringerFactory,
	consensusBringerFactory delivery.ConsensusBringerFactory,
) *Assembler {
	logger.Infof("Creating assembler, party: %d, address: %s", nodeConfig.PartyId, nodeConfig.ListenAddress)
	if configBlock == nil {
		logger.Panicf("Error creating Assembler%d, config block is nil", nodeConfig.PartyId)
		return nil
	}
	if configBlock.Header == nil {
		logger.Panicf("Error creating Assembler%d, config block header is nil", nodeConfig.PartyId)
		return nil
	}

	ledger, err := assemblerLedgerFactory.Create(logger, nodeConfig.Directory)
	if err != nil {
		logger.Panicf("Failed creating assembler: %v", err)
	}

	assembler := &Assembler{
		logger:       logger,
		ledger:       ledger,
		mainExitChan: mainExitChan,
		status:       utils.NodeStatus{},
		metrics:      NewMetrics(nodeConfig, ledger.Metrics(), logger),
	}

	assembler.initLedger(configBlock)

	assembler.initFromConfig(net, nodeConfig, configBlock, prefetchIndexFactory, prefetcherFactory, batchBringerFactory, consensusBringerFactory)

	assembler.metrics.Start()

	return assembler
}

func (a *Assembler) initFromConfig(
	net Net,
	nodeConfig *node_config.AssemblerNodeConfig,
	configBlock *common.Block,
	prefetchIndexFactory PrefetchIndexerFactory,
	prefetcherFactory PrefetcherFactory,
	batchBringerFactory BatchBringerFactory,
	consensusBringerFactory delivery.ConsensusBringerFactory,
) {
	a.lock.Lock()
	defer a.lock.Unlock()

	configSequence := nodeConfig.Bundle.ConfigtxValidator().Sequence()

	a.logger.Infof("Initializing assembler with config sequence number: %d, assembler ledger height: %d", configSequence, a.ledger.LedgerReader().Height())
	a.status.Set(utils.StateInitializing, configSequence)

	a.assemblerNodeConfig = nodeConfig

	shardIds := shardsFromAssemblerConfig(nodeConfig)
	partyIds := partiesFromAssemblerConfig(nodeConfig)
	batchFrontier, err := a.ledger.BatchFrontier(shardIds, partyIds, nodeConfig.RestartLedgerScanTimeout)
	if err != nil {
		a.logger.Panicf("Failed fetching batch frontier: %v", err)
	}
	a.logger.Infof("Starting with BatchFrontier: %s", node_ledger.BatchFrontierToString(batchFrontier))

	index := prefetchIndexFactory.Create(shardIds, partyIds, a.logger, nodeConfig.PrefetchEvictionTtl, nodeConfig.PrefetchBufferMemoryBytes, nodeConfig.BatchRequestsChannelSize, &DefaultTimerFactory{}, &DefaultBatchCacheFactory{}, &DefaultPartitionPrefetchIndexerFactory{}, nodeConfig.PopWaitMonitorTimeout)
	br := batchBringerFactory.Create(batchFrontier, nodeConfig, a.logger)

	a.prefetcher = prefetcherFactory.Create(shardIds, partyIds, index, br, a.logger)

	baReplicator := consensusBringerFactory.Create(nodeConfig.Consenter.TLSCACerts, nodeConfig.TLSPrivateKeyFile, nodeConfig.TLSCertificateFile, nodeConfig.Consenter.Endpoint, a.ledger, a.logger)
	a.collator = Collator{
		Shards:                            shardIds,
		OrderedBatchAttestationReplicator: baReplicator,
		Index:                             index,
		Logger:                            a.logger,
		Ledger:                            a.ledger,
		ShardCount:                        len(nodeConfig.Shards),
		AssemblerRestarter:                a,
	}

	a.ds = NewAssemblerDeliverService(a.ledger.LedgerReader(), a.logger, nodeConfig, a.metrics.deliverMetrics)
	a.lastConfigBlockNumber = configBlock.GetHeader().GetNumber()

	if net != nil {
		a.net = net
	}

	a.prefetcher.Start()
	a.collator.Run()

	a.logger.Infof("Assembler initialized successfully with config sequence number: %d", configSequence)
}

func (a *Assembler) initLedger(configBlock *common.Block) {
	var transactionCount, blocksCount uint64
	height := a.ledger.LedgerReader().Height()
	if height > 0 {
		block, err := a.ledger.LedgerReader().RetrieveBlockByNumber(height - 1)
		if err != nil {
			a.logger.Panicf("error while fetching last block from ledger %v", err)
		}
		_, _, transactionCount, err = node_ledger.AssemblerBatchIdOrderingInfoAndTxCountFromBlock(block)
		if err != nil {
			a.logger.Panicf("error while fetching last block ordering info %v", err)
		}

		blocksCount = height
	}
	a.ledger.Metrics().TransactionCount.Add(float64(transactionCount))
	a.ledger.Metrics().BlocksCount.Add(float64(blocksCount))

	a.logger.Infof("Starting with ledger height: %d", a.ledger.LedgerReader().Height())

	if a.ledger.LedgerReader().Height() == 0 {
		// append config block only if it is the genesis block
		blockNumber := configBlock.GetHeader().Number
		if blockNumber == 0 {
			configBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = node_ledger.AssemblerGenesisBlockMetadataToBytes()
			ordInfo := &state.OrderingInformation{
				CommonBlock: configBlock,
				DecisionNum: 0,
				BatchIndex:  0,
				BatchCount:  1,
			}
			a.ledger.AppendConfig(ordInfo)
			a.logger.Infof("Appended genesis block, header digest: %s", hex.EncodeToString(protoutil.BlockHeaderHash(configBlock.GetHeader())))
		} else {
			a.logger.Infof("Assembler started with non-genesis config block, block number: %d", blockNumber)
		}
	}
}

func NewAssembler(nodeConfig *node_config.AssemblerNodeConfig, configBlock *common.Block, mainExitChan chan struct{}, logger *flogging.FabricLogger) *Assembler {
	return NewDefaultAssembler(
		logger,
		nil,
		nodeConfig,
		configBlock,
		mainExitChan,
		&node_ledger.DefaultAssemblerLedgerFactory{},
		&DefaultPrefetchIndexerFactory{},
		&DefaultPrefetcherFactory{},
		&DefaultBatchBringerFactory{},
		&delivery.DefaultConsensusBringerFactory{},
	)
}

func (a *Assembler) Address() string {
	a.lock.RLock()
	defer a.lock.RUnlock()
	if a.net == nil {
		return ""
	}

	return a.net.Address()
}

func (a *Assembler) StartAssemblerService() {
	a.lock.Lock()
	defer a.lock.Unlock()
	srv := utils.CreateGRPCAssembler(a.assemblerNodeConfig)
	a.net = srv
	orderer.RegisterAtomicBroadcastServer(srv.Server(), a)
	go func() {
		a.logger.Infof("Assembler network service is starting on %s", srv.Address())
		err := srv.Start()
		if err != nil {
			panic(err)
		}
		a.logger.Infof("Assembler network service was stopped")
	}()

	a.status.SetState(utils.StateRunning)
}

func (a *Assembler) GetStatus() utils.NodeStatus {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.status
}
