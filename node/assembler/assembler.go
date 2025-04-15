package assembler

import (
	"fmt"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/common/utils"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"
	"github.ibm.com/decentralized-trust-research/arma/node/delivery"
	node_ledger "github.ibm.com/decentralized-trust-research/arma/node/ledger"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
)

func NewInitialAssemblerConsensusPosition(oi *state.OrderingInformation) core.AssemblerConsensusPosition {
	if oi.BatchIndex != oi.BatchCount-1 {
		return core.AssemblerConsensusPosition{
			DecisionNum: oi.DecisionNum,
			BatchIndex:  oi.BatchIndex + 1,
		}
	}
	return core.AssemblerConsensusPosition{
		DecisionNum: oi.DecisionNum + 1,
	}
}

type Assembler struct {
	assembler    core.Assembler
	logger       types.Logger
	ds           delivery.DeliverService
	prefetcher   PrefetcherController
	baReplicator delivery.ConsensusBringer
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

func (a *Assembler) Stop() {
	a.prefetcher.Stop()
	a.assembler.Index.Stop()
	a.assembler.WaitTermination()
	a.assembler.Ledger.Close()
	a.baReplicator.Stop()
}

func NewDefaultAssembler(
	logger types.Logger,
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

	if al.LedgerReader().Height() == 0 {
		if genesisBlock == nil {
			genesisBlock = utils.EmptyGenesisBlock("arma")
		}
		al.AppendConfig(genesisBlock, 0)
	}

	shardIds := shardsFromAssemblerConfig(config)
	partyIds := partiesFromAssemblerConfig(config)

	batchFrontier, err := al.BatchFrontier(shardIds, partyIds, config.RestartLedgerScanTimeout)
	if err != nil {
		logger.Panicf("Failed fetching batch frontier: %v", err)
	}

	index := prefetchIndexFactory.Create(shardIds, partyIds, logger, config.PrefetchEvictionTtl, config.PrefetchBufferMemoryBytes, config.BatchRequestsChannelSize, &DefaultTimerFactory{}, &DefaultBatchCacheFactory{}, &DefaultPartitionPrefetchIndexerFactory{})
	if err != nil {
		logger.Panicf("Failed creating index: %v", err)
	}

	baReplicator := consensusBringerFactory.Create(config.Consenter.TLSCACerts, config.TLSPrivateKeyFile, config.TLSCertificateFile, config.Consenter.Endpoint, logger)

	br := batchBringerFactory.Create(batchFrontier, config, logger)

	prefetcher := prefetcherFactory.Create(shardIds, partyIds, index, br, logger)
	prefetcher.Start()

	lastOrderingInfo, err := al.LastOrderingInfo()
	if err != nil {
		logger.Panicf("Failed fetching last ordering info: %v", err)
	}

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
			InitialDecisionPosition:           NewInitialAssemblerConsensusPosition(lastOrderingInfo),
		},
		logger:       logger,
		prefetcher:   prefetcher,
		baReplicator: baReplicator,
	}

	// TODO: we do not need multiple ledgers in the assembler
	assembler.ds["arma"] = al.LedgerReader()

	assembler.assembler.Run()

	return assembler
}

func NewAssembler(config *config.AssemblerNodeConfig, genesisBlock *common.Block, logger types.Logger) *Assembler {
	return NewDefaultAssembler(
		logger,
		config,
		genesisBlock,
		&node_ledger.DefaultAssemblerLedgerFactory{},
		&DefaultPrefetchIndexerFactory{},
		&DefaultPrefetcherFactory{},
		&DefaultBatchBringerFactory{},
		&delivery.DefaultConsensusBringerFactory{},
	)
}
