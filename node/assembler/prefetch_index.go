package assembler

import (
	"encoding/binary"
	"sync"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/node/config"
	node_ledger "arma/node/ledger"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
)

type Index struct {
	indexes  map[types.ShardID]map[types.PartyID]*blkstorage.BlockStore
	logger   types.Logger
	lock     sync.RWMutex
	cacheMap map[types.ShardID]map[types.PartyID]*cache
}

func NewIndex(config config.AssemblerNodeConfig, blockStores map[string]*blkstorage.BlockStore, logger types.Logger) *Index {
	parties := partiesFromAssemblerConfig(config)
	indexes := make(map[types.ShardID]map[types.PartyID]*blkstorage.BlockStore)

	for _, s := range config.Shards {
		shardID := types.ShardID(s.ShardId)
		indexes[shardID] = make(map[types.PartyID]*blkstorage.BlockStore)
		for _, partyID := range parties {
			name := node_ledger.ShardPartyToChannelName(shardID, partyID)
			batcherLedger, exists := blockStores[name]
			if !exists {
				logger.Panicf("Block store %s does not exist", name)
			}

			indexes[shardID][partyID] = batcherLedger
		}
	}

	cacheMap := make(map[types.ShardID]map[types.PartyID]*cache)
	for _, s := range config.Shards {
		shardID := types.ShardID(s.ShardId)
		cacheMap[shardID] = make(map[types.PartyID]*cache)
		for _, partyID := range parties {
			cacheMap[shardID][partyID] = newCache(defaultMaxCacheSizeBytes) // TODO expose in config
		}

	}

	return &Index{logger: logger, indexes: indexes, cacheMap: cacheMap}
}

func (i *Index) Index(party types.PartyID, shard types.ShardID, sequence types.BatchSequence, batch core.Batch) {
	t1 := time.Now()
	defer func() {
		i.logger.Infof("Indexed batch %d for shard %d in %v", sequence, shard, time.Since(t1))
	}()
	buff := make([]byte, 4)
	binary.BigEndian.PutUint16(buff, uint16(batch.Primary()))

	block := &common.Block{
		Header: &common.BlockHeader{
			DataHash: batch.Digest(),
			Number:   uint64(sequence),
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

func (i *Index) Retrieve(party types.PartyID, shard types.ShardID, sequence types.BatchSequence, digest []byte) (core.Batch, bool) {
	t1 := time.Now()

	defer func() {
		i.logger.Infof("Retrieved batch %d for shard %d in %v", sequence, shard, time.Since(t1))
	}()

	i.lock.RLock()
	blockFromCache, exists := i.cacheMap[shard][party].get(uint64(sequence))
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

	if bcInfo.Height < uint64(sequence)+1 {
		return nil, false
	}

	block, err := ledger.RetrieveBlockByNumber(uint64(sequence))
	if err != nil {
		i.logger.Panicf("Failed retrieving block: %v", err)
	}

	fb := node_ledger.FabricBatch(*block)
	return &fb, true
}

func (i *Index) Height(shard types.ShardID, party types.PartyID) uint64 {
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
