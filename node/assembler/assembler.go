package assembler

import (
	"fmt"

	arma "arma/core"
	"arma/node/config"
	"arma/node/delivery"
	node_ledger "arma/node/ledger"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
)

type Assembler struct {
	assembler arma.Assembler
	logger    arma.Logger
	getHeight func() uint64
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

func newAssembler(logger arma.Logger, config config.AssemblerNodeConfig, blockStores map[string]*blkstorage.BlockStore) *Assembler {
	index := NewIndex(config, blockStores, logger)

	tlsKey := config.TLSPrivateKeyFile

	tlsCert := config.TLSCertificateFile

	ledger := fileledger.NewFileLedger(blockStores["arma"])

	baReplicator := newBAReplicator(logger, config, tlsKey, tlsCert)

	br := &BatchFetcher{
		ledgerHeightReader: index,
		logger:             logger,
		config:             config,
		tlsKey:             tlsKey,
		tlsCert:            tlsCert,
	}

	var shards []arma.ShardID
	for _, shard := range config.Shards {
		shards = append(shards, arma.ShardID(shard.ShardId))
	}

	al := &node_ledger.AssemblerLedger{Ledger: ledger, Logger: logger}
	go al.TrackThroughput()
	assembler := &Assembler{
		ds:        make(delivery.DeliverService),
		getHeight: ledger.Height,
		assembler: arma.Assembler{
			Shards:                     shards,
			BatchAttestationReplicator: baReplicator,
			Replicator:                 br,
			Index:                      index,
			Logger:                     logger,
			Ledger:                     al,
			ShardCount:                 len(config.Shards),
		},
		logger: logger,
	}

	assembler.ds["arma"] = ledger

	assembler.assembler.Run()

	return assembler
}

func newBAReplicator(logger arma.Logger, config config.AssemblerNodeConfig, tlsKey config.RawBytes, tlsCert config.RawBytes) *delivery.BAReplicator {
	r := delivery.NewBAReplicator(config.Consenter.TLSCACerts, tlsKey, tlsCert, config.Consenter.Endpoint, logger)
	return r
}

type FactoryCreator func(string) blockledger.Factory

func NewAssembler(config config.AssemblerNodeConfig, logger arma.Logger) *Assembler {
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

	blockStores := make(map[string]*blkstorage.BlockStore)

	// This is the store where final blocks are stored
	blockStores["arma"] = armaLedger

	parties := partiesFromAssemblerConfig(config)
	for _, shard := range config.Shards {
		// Open an array for each shard
		for _, p := range parties {
			name := node_ledger.ShardPartyToChannelName(arma.ShardID(shard.ShardId), p)
			batcherLedger, err := provider.Open(name)
			if err != nil {
				logger.Panicf("Failed opening ledger: %v", err)
			}
			blockStores[name] = batcherLedger
		}
	}

	logger.Infof("Assembler %d opened block stores: %+v", config.PartyId, blockStores)

	assembler := newAssembler(logger, config, blockStores)

	return assembler
}
