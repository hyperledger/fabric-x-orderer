/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"fmt"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/common/utils"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	"github.ibm.com/decentralized-trust-research/arma/node/delivery"
	node_ledger "github.ibm.com/decentralized-trust-research/arma/node/ledger"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
)

type NetStopper interface {
	Stop()
}

type Assembler struct {
	assembler    core.Assembler
	logger       types.Logger
	ds           delivery.DeliverService
	prefetcher   PrefetcherController
	baReplicator delivery.ConsensusBringer
	netStopper   NetStopper
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
	a.netStopper.Stop()
	a.prefetcher.Stop()
	a.assembler.Index.Stop()
	a.assembler.WaitTermination()
	a.assembler.Ledger.Close()
	a.baReplicator.Stop()
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

	baReplicator := consensusBringerFactory.Create(config.Consenter.TLSCACerts, config.TLSPrivateKeyFile, config.TLSCertificateFile, config.Consenter.Endpoint, al, logger)

	br := batchBringerFactory.Create(batchFrontier, config, logger)

	prefetcher := prefetcherFactory.Create(shardIds, partyIds, index, br, logger)
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
		},
		logger:       logger,
		netStopper:   net,
		prefetcher:   prefetcher,
		baReplicator: baReplicator,
	}

	// TODO: we do not need multiple ledgers in the assembler
	assembler.ds["arma"] = al.LedgerReader()

	assembler.assembler.Run()

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
