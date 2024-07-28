package assembler

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"arma/node/delivery"

	arma "arma/core"
	"arma/node/comm"
	"arma/node/config"
	node_ledger "arma/node/ledger"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
	"github.com/hyperledger/fabric/protoutil"
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

func NewAssembler(logger arma.Logger, config config.AssemblerNodeConfig, blockStores map[string]*blkstorage.BlockStore) *Assembler {
	index := NewIndex(config, blockStores, logger)

	tlsKey := config.TLSPrivateKeyFile

	tlsCert := config.TLSCertificateFile

	ledger := fileledger.NewFileLedger(blockStores["arma"])

	baReplicator := newBAReplicator(logger, config, tlsKey, tlsCert)

	br := &BatchReplicator{
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

const defaultMaxCacheSizeBytes = 1024 * 1024 * 1024

type Index struct {
	indexes  map[arma.ShardID]map[arma.PartyID]*blkstorage.BlockStore
	logger   arma.Logger
	lock     sync.RWMutex
	cacheMap map[arma.ShardID]map[arma.PartyID]*cache
}

func NewIndex(config config.AssemblerNodeConfig, blockStores map[string]*blkstorage.BlockStore, logger arma.Logger) *Index {
	parties := partiesFromAssemblerConfig(config)
	indexes := make(map[arma.ShardID]map[arma.PartyID]*blkstorage.BlockStore)

	for _, s := range config.Shards {
		shardID := arma.ShardID(s.ShardId)
		indexes[shardID] = make(map[arma.PartyID]*blkstorage.BlockStore)
		for _, partyID := range parties {
			name := node_ledger.ShardPartyToChannelName(shardID, partyID)
			batcherLedger, exists := blockStores[name]
			if !exists {
				logger.Panicf("Block store %s does not exist", name)
			}

			indexes[shardID][partyID] = batcherLedger
		}
	}

	cacheMap := make(map[arma.ShardID]map[arma.PartyID]*cache)
	for _, s := range config.Shards {
		shardID := arma.ShardID(s.ShardId)
		cacheMap[shardID] = make(map[arma.PartyID]*cache)
		for _, partyID := range parties {
			cacheMap[shardID][partyID] = newCache(defaultMaxCacheSizeBytes) // TODO expose in config
		}

	}

	return &Index{logger: logger, indexes: indexes, cacheMap: cacheMap}
}

func (i *Index) Index(party arma.PartyID, shard arma.ShardID, sequence uint64, batch arma.Batch) {
	t1 := time.Now()
	defer func() {
		i.logger.Infof("Indexed batch %d for shard %d in %v", sequence, shard, time.Since(t1))
	}()
	buff := make([]byte, 4)
	binary.BigEndian.PutUint16(buff, uint16(batch.Party()))

	block := &common.Block{
		Header: &common.BlockHeader{
			DataHash: batch.Digest(),
			Number:   sequence,
		},
		Data: &common.BlockData{Data: batch.Requests()},
		Metadata: &common.BlockMetadata{
			Metadata: [][]byte{{}, {}, {}, {}, {}, buff},
		},
	}

	var size int
	for _, req := range batch.Requests() {
		size += len(req)
	}

	i.lock.Lock()
	i.cacheMap[shard][party].put(block, size)
	i.lock.Unlock()

	i.indexes[shard][party].AddBlock(block)
}

func (i *Index) Retrieve(party arma.PartyID, shard arma.ShardID, sequence uint64, digest []byte) (arma.Batch, bool) {
	t1 := time.Now()

	defer func() {
		i.logger.Infof("Retrieved batch %d for shard %d in %v", sequence, shard, time.Since(t1))
	}()

	i.lock.RLock()
	blockFromCache, exists := i.cacheMap[shard][party].get(sequence)
	i.lock.RUnlock()

	if exists {
		fb := node_ledger.FabricBatch(*blockFromCache)
		return &fb, true
	}

	ledger := i.indexes[shard][party]

	bcInfo, err := ledger.GetBlockchainInfo()
	if err != nil {
		i.logger.Panicf("Failed retrieving blockchain info: %v", err)
	}

	if bcInfo.Height < sequence+1 {
		return nil, false
	}

	block, err := ledger.RetrieveBlockByNumber(sequence)
	if err != nil {
		i.logger.Panicf("Failed retrieving block: %v", err)
	}

	fb := node_ledger.FabricBatch(*block)
	return &fb, true
}

func (i *Index) Height(shard arma.ShardID, party arma.PartyID) uint64 {
	shardIndex, ok := i.indexes[shard]
	if !ok {
		i.logger.Panicf("Failed retrieving shardIndex for shard: %d", shard)
	}
	partyLedger, ok := shardIndex[party]
	if !ok {
		i.logger.Panicf("Failed retrieving ledger for shard: %d, party %d", shard, party)
	}
	info, err := partyLedger.GetBlockchainInfo()
	if err != nil {
		i.logger.Panicf("Failed retrieving blockchain info: %v", err)
	}
	return info.GetHeight()
}

type AssemblerLedgerHeightReader interface {
	Height(shardID arma.ShardID, partyID arma.PartyID) uint64
}

type BatchReplicator struct {
	ledgerHeightReader AssemblerLedgerHeightReader
	tlsKey, tlsCert    []byte
	config             config.AssemblerNodeConfig
	logger             arma.Logger
}

func (br *BatchReplicator) clientConfig() comm.ClientConfig {
	var tlsCAs [][]byte
	for _, shard := range br.config.Shards {
		for _, batcher := range shard.Batchers {
			for _, tlsCA := range batcher.TLSCACerts {
				tlsCAs = append(tlsCAs, tlsCA)
			}
		}
	}

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               br.tlsKey,
			Certificate:       br.tlsCert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}

func (br *BatchReplicator) Replicate(shardID arma.ShardID) <-chan arma.Batch {
	br.logger.Infof("Assembler %d Replicate from shard %d", br.config.PartyId, shardID)

	// Find the batcher from my party in this shard.
	// TODO we need retry mechanisms with timeouts and be able to connect to another party on that shard.
	batcherToPullFrom := br.findShardID(shardID)

	br.logger.Infof("Assembler %d Replicate from shard %d batcher info %+v", br.config.PartyId, shardID, batcherToPullFrom)

	res := make(chan arma.Batch, 100)

	for _, p := range partiesFromAssemblerConfig(br.config) {
		br.pullFromParty(shardID, batcherToPullFrom, p, res)
	}

	return res
}

func (br *BatchReplicator) pullFromParty(shardID arma.ShardID, batcherToPullFrom config.BatcherInfo, partyID arma.PartyID, resultChan chan arma.Batch) {
	seq := br.ledgerHeightReader.Height(shardID, partyID)

	endpoint := func() string {
		return batcherToPullFrom.Endpoint
	}

	channelName := node_ledger.ShardPartyToChannelName(shardID, partyID)
	br.logger.Infof("Assembler replicating from channel %s ", channelName)

	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		channelName,
		nil,
		delivery.NextSeekInfo(seq),
		int32(0),
		uint64(0),
		nil,
	)
	if err != nil {
		br.logger.Panicf("Failed creating signed envelope: %v", err)
	}

	go delivery.Pull(
		context.Background(),
		channelName,
		br.logger, endpoint,
		requestEnvelope,
		br.clientConfig(),
		func(block *common.Block) {
			fb := node_ledger.FabricBatch(*block)
			br.logger.Infof("Assembler Pulled <%d,%d,%d> with digest %s", shardID, fb.Party(), fb.Sequence(), hex.EncodeToString(fb.Digest()[:8]))
			resultChan <- &fb
		},
	)
	br.logger.Infof("Started pulling from: %s, sqn=%d", channelName, seq)
}

func (br *BatchReplicator) findShardID(shardID arma.ShardID) config.BatcherInfo {
	for _, shard := range br.config.Shards {
		if shard.ShardId == uint16(shardID) {
			for _, b := range shard.Batchers {
				if b.PartyID == br.config.PartyId {
					return b
				}
			}

			br.logger.Panicf("Failed finding our party %d within %v", br.config.PartyId, shard.Batchers)
		}
	}

	br.logger.Panicf("Failed finding shard ID %d within %v", shardID, br.config.Shards)
	return config.BatcherInfo{}
}

func CreateAssembler(config config.AssemblerNodeConfig, logger arma.Logger) *Assembler {
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

	assembler := NewAssembler(logger, config, blockStores)

	return assembler
}

func partiesFromAssemblerConfig(config config.AssemblerNodeConfig) []arma.PartyID {
	var parties []arma.PartyID
	for _, b := range config.Shards[0].Batchers {
		parties = append(parties, arma.PartyID(b.PartyID))
	}
	return parties
}
