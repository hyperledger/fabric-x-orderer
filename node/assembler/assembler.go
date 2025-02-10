package assembler

import (
	"fmt"
	"time"

	"arma/common/types"
	"arma/common/utils"
	"arma/core"
	"arma/node/config"
	"arma/node/delivery"
	node_ledger "arma/node/ledger"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
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

func NewAssembler(config config.AssemblerNodeConfig, genesisBlock *common.Block, logger types.Logger) *Assembler {
	logger.Infof("Creating assembler, party: %d, address: %s, with genesis block: %t", config.PartyId, config.ListenAddress, genesisBlock != nil)

	al, err := node_ledger.NewAssemblerLedger(logger, config.Directory)
	if err != nil {
		logger.Panicf("Failed creating assembler: %v", err)
	}

	if al.Ledger.Height() == 0 {
		if genesisBlock == nil {
			genesisBlock = utils.EmptyGenesisBlock("arma")
		}
		al.AppendConfig(genesisBlock, 0)
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
	assembler.ds["arma"] = al.Ledger

	assembler.assembler.Run()

	return assembler
}
