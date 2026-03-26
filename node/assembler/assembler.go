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
	ds           *AssemblerDeliverService
	mainExitChan chan struct{}

	lock                  sync.RWMutex
	collator              Collator
	prefetcher            PrefetcherController
	status                utils.NodeStatus
	net                   Net
	stopSignalListenChan  chan struct{}
	assemblerNodeConfig   *node_config.AssemblerNodeConfig
	lastConfigBlockNumber uint64
	metrics               *Metrics
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
	close(a.stopSignalListenChan)

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

	al, err := assemblerLedgerFactory.Create(logger, nodeConfig.Directory)
	if err != nil {
		logger.Panicf("Failed creating assembler: %v", err)
	}

	metrics := NewMetrics(nodeConfig, al.Metrics(), logger)

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
			al.AppendConfig(ordInfo)
			logger.Infof("Appended genesis block, header digest: %s", hex.EncodeToString(protoutil.BlockHeaderHash(configBlock.GetHeader())))
		} else {
			logger.Infof("Assembler started with non-genesis config block, block number: %d", blockNumber)
		}
	}

	shardIds := shardsFromAssemblerConfig(nodeConfig)
	partyIds := partiesFromAssemblerConfig(nodeConfig)

	batchFrontier, err := al.BatchFrontier(shardIds, partyIds, nodeConfig.RestartLedgerScanTimeout)
	if err != nil {
		logger.Panicf("Failed fetching batch frontier: %v", err)
	}
	logger.Infof("Starting with BatchFrontier: %s", node_ledger.BatchFrontierToString(batchFrontier))

	index := prefetchIndexFactory.Create(shardIds, partyIds, logger, nodeConfig.PrefetchEvictionTtl, nodeConfig.PrefetchBufferMemoryBytes, nodeConfig.BatchRequestsChannelSize, &DefaultTimerFactory{}, &DefaultBatchCacheFactory{}, &DefaultPartitionPrefetchIndexerFactory{}, nodeConfig.PopWaitMonitorTimeout)

	baReplicator := consensusBringerFactory.Create(nodeConfig.Consenter.TLSCACerts, nodeConfig.TLSPrivateKeyFile, nodeConfig.TLSCertificateFile, nodeConfig.Consenter.Endpoint, al, logger)

	br := batchBringerFactory.Create(batchFrontier, nodeConfig, logger)

	prefetcher := prefetcherFactory.Create(shardIds, partyIds, index, br, logger)
	prefetcher.Start()

	assembler := &Assembler{
		assemblerNodeConfig: nodeConfig,

		ds: NewAssemblerDeliverService(al.LedgerReader(), logger, nodeConfig, metrics.deliverMetrics),
		collator: Collator{
			Shards:                            shardIds,
			OrderedBatchAttestationReplicator: baReplicator,
			Index:                             index,
			Logger:                            logger,
			Ledger:                            al,
			ShardCount:                        len(nodeConfig.Shards),
		},
		logger:                logger,
		prefetcher:            prefetcher,
		metrics:               metrics,
		lastConfigBlockNumber: configBlock.GetHeader().GetNumber(),
		mainExitChan:          mainExitChan,
		stopSignalListenChan:  make(chan struct{}),
		status:                utils.NodeStatus{},
	}

	assembler.lock.Lock()
	assembler.status.Set(utils.StateRunning, nodeConfig.Bundle.ConfigtxValidator().Sequence())
	assembler.lock.Unlock()

	if net != nil {
		assembler.net = net
	}

	assembler.collator.AssemblerRestarter = assembler

	assembler.collator.Run()
	assembler.metrics.Start()

	return assembler
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

	utils.StopSignalListen(a.stopSignalListenChan, a, a.logger, srv.Address())

	a.status.SetState(utils.StateRunning)
}

func (a *Assembler) GetStatus() utils.NodeStatus {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.status
}
