package assembler

import (
	"fmt"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/node/config"
	"arma/node/delivery"
	node_ledger "arma/node/ledger"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
)

// TODO: move to config
const (
	maxSizeBytes      = 1 * 1024 * 1024 * 1024 // 1GB
	ledgerScanTimeout = 5 * time.Second
	evictionTtl       = time.Hour
)

type Assembler struct {
	assembler core.Assembler
	logger    types.Logger
	ds        delivery.DeliverService
}

func (a *Assembler) Broadcast(server orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("should not be used")
}

func (a *Assembler) Deliver(server orderer.AtomicBroadcast_DeliverServer) error {
	return a.ds.Deliver(server)
}

func (a *Assembler) GetTxCount() uint64 {
	// TODO do this in a cleaner fashion
	return a.assembler.Ledger.(*node_ledger.AssemblerLedger).GetTxCount()
}

func NewAssembler(config config.AssemblerNodeConfig, logger types.Logger) *Assembler {
	// Create the ledger
	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(config.Directory, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{})
	if err != nil {
		logger.Panicf("Failed creating provider: %v", err)
	}
	logger.Infof("Assembler %d opened block ledger provider, dir: %s", config.PartyId, config.Directory)
	armaLedger, err := provider.Open("arma")
	if err != nil {
		logger.Panicf("Failed opening ledger: %v", err)
	}
	logger.Infof("Assembler %d opened block store: %+v", config.PartyId, armaLedger)
	ledger := fileledger.NewFileLedger(armaLedger)
	al, err := node_ledger.NewAssemblerLedger(logger, ledger)
	if err != nil {
		logger.Panicf("Failed creating assembler: %v", err)
	}

	shardIds := shardsFromAssemblerConfig(config)
	partyIds := partiesFromAssemblerConfig(config)

	batchFrontier, err := al.BatchFrontier(shardIds, partyIds, ledgerScanTimeout)
	if err != nil {
		logger.Panicf("Failed fetching batch frontier: %v", err)
	}

	lastOrderingInfo, err := al.LastOrderingInfo()
	if err != nil {
		logger.Panicf("Failed fetching last ordering info: %v", err)
	}
	var lastDecisionNum types.DecisionNum
	if lastOrderingInfo != nil {
		lastDecisionNum = lastOrderingInfo.DecisionNum + 1
	}

	index := NewPrefetchIndex(shardIds, partyIds, logger, evictionTtl, maxSizeBytes, &DefaultTimerFactory{}, &DefaultBatchCacheFactory{}, &DefaultPartitionPrefetchIndexerFactory{})
	if err != nil {
		logger.Panicf("Failed creating index: %v", err)
	}

	baReplicator := delivery.NewConsensusReplicator(config.Consenter.TLSCACerts, config.TLSPrivateKeyFile, config.TLSCertificateFile, config.Consenter.Endpoint, logger)

	br := NewBatchFetcher(batchFrontier, config, logger)

	prefetcher := NewPrefetcher(shardIds, partyIds, index, br, logger)
	prefetcher.Start()

	assembler := &Assembler{
		ds: make(delivery.DeliverService),
		assembler: core.Assembler{
			Shards:                            shardIds,
			OrderedBatchAttestationReplicator: baReplicator,
			Replicator:                        br,
			Index:                             index,
			Logger:                            logger,
			Ledger:                            al,
			ShardCount:                        len(config.Shards),
			StartingDesicion:                  lastDecisionNum,
		},
		logger: logger,
	}

	// TODO: we do not need multiple ledgers in the assembler
	assembler.ds["arma"] = ledger

	assembler.assembler.Run()

	return assembler
}
