package node

import (
	arma "arma/pkg"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protoutil"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm"
	"google.golang.org/grpc"
)

type Assembler struct {
	assembler arma.Assembler
	logger    arma.Logger
	getHeight func() uint64
	ds        DeliverService
}

func (a *Assembler) Broadcast(server orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("should not be used")
}

func (a *Assembler) Deliver(server orderer.AtomicBroadcast_DeliverServer) error {
	return a.ds.Deliver(server)
}

func NewAssembler(logger arma.Logger, dir string, config AssemblerNodeConfig, blockStores map[string]*blkstorage.BlockStore) *Assembler {
	id := config.PartyId
	name := fmt.Sprintf("assembler%d", id)
	dir = filepath.Join(dir, name)

	index := NewIndex(config, blockStores, logger)

	tlsKey := config.TLSPrivateKeyFile

	tlsCert := config.TLSCertificateFile

	ledger := fileledger.NewFileLedger(blockStores["arma"])

	baReplicator := &BAReplicator{
		logger:  logger,
		config:  config,
		tlsKey:  tlsKey,
		tlsCert: tlsCert,
	}

	heightRetrievers := createHeightRetrievers(logger, config, blockStores)

	br := &BatchReplicator{
		heightRetrievers: heightRetrievers,
		logger:           logger,
		config:           config,
		tlsKey:           tlsKey,
		tlsCert:          tlsCert,
	}

	var shards []arma.ShardID
	for _, shard := range config.Shards {
		shards = append(shards, arma.ShardID(shard.ShardId))
	}

	assembler := &Assembler{
		ds:        make(DeliverService),
		getHeight: ledger.Height,
		assembler: arma.Assembler{
			Shards:                     shards,
			BatchAttestationReplicator: baReplicator,
			Replicator:                 br,
			Index:                      index,
			Logger:                     logger,
			Ledger:                     &AssemblerLedger{Ledger: ledger},
			ShardCount:                 len(config.Shards),
		},
		logger: logger,
	}

	assembler.ds["arma"] = ledger

	assembler.assembler.Run()

	return assembler
}

func createHeightRetrievers(logger arma.Logger, config AssemblerNodeConfig, blockStores map[string]*blkstorage.BlockStore) map[arma.ShardID]func() uint64 {
	heightRetrievers := make(map[arma.ShardID]func() uint64)

	for _, shard := range config.Shards {
		heightRetrievers[arma.ShardID(shard.ShardId)] = func() uint64 {
			bcInfo, err := blockStores[fmt.Sprintf("shard%d", shard.ShardId)].GetBlockchainInfo()
			if err != nil {
				logger.Panicf("Failed obtaining blockchain info: %v", err)
			}

			return bcInfo.Height
		}
	}
	return heightRetrievers
}

type FactoryCreator func(string) blockledger.Factory

type Index struct {
	indexes map[arma.ShardID]*blkstorage.BlockStore
	logger  arma.Logger
	lock    sync.RWMutex
	cache   map[arma.ShardID]*cache
}

func NewIndex(config AssemblerNodeConfig, blockStores map[string]*blkstorage.BlockStore, logger arma.Logger) *Index {
	indexes := make(map[arma.ShardID]*blkstorage.BlockStore)

	for _, s := range config.Shards {
		shardID := s.ShardId
		name := fmt.Sprintf("shard%d", shardID)
		batcherLedger, exists := blockStores[name]
		if !exists {
			logger.Panicf("Block store %s does not exist", name)
		}

		indexes[arma.ShardID(shardID)] = batcherLedger
	}

	cache := make(map[arma.ShardID]*cache)
	for _, shard := range config.Shards {
		cache[arma.ShardID(shard.ShardId)] = newCache(1024 * 1024 * 1024)
	}

	return &Index{logger: logger, indexes: indexes, cache: cache}
}

func (i *Index) Index(party arma.PartyID, shard arma.ShardID, sequence uint64, batch arma.Batch) {
	defer i.logger.Infof("Indexed batch %d for shard %d", sequence, shard)
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
	i.cache[shard].put(block, size)
	i.lock.Unlock()

	i.indexes[shard].AddBlock(block)
}

func (i *Index) Retrieve(party arma.PartyID, shard arma.ShardID, sequence uint64, digest []byte) (arma.Batch, bool) {
	i.logger.Infof("Retrieving batch %d for shard %d", sequence, shard)

	i.lock.RLock()
	blockFromCache, exists := i.cache[shard].get(sequence)
	i.lock.RUnlock()

	if exists {
		fb := fabricBatch(*blockFromCache)
		return &fb, true
	}

	ledger := i.indexes[shard]

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

	fb := fabricBatch(*block)
	i.logger.Infof("Retrieved batch %d for shard %d", sequence, shard)
	return &fb, true
}

type BatchReplicator struct {
	heightRetrievers map[arma.ShardID]func() uint64
	tlsKey, tlsCert  []byte
	config           AssemblerNodeConfig
	logger           arma.Logger
}

func (br *BatchReplicator) clientConfig() comm.ClientConfig {
	var tlsCAs [][]byte
	for _, cert := range br.config.Consenter.TLSCACerts {
		tlsCAs = append(tlsCAs, cert)
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
	batcherToPullFrom := br.findShardID(shardID)

	seq := br.heightRetrievers[shardID]()

	endpoint := batcherToPullFrom.Endpoint

	shardName := fmt.Sprintf("shard%d", shardID)
	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		shardName,
		nil,
		nextSeekInfo(seq),
		int32(0),
		uint64(0),
		nil,
	)

	if err != nil {
		br.logger.Panicf("Failed creating signed envelope: %v", err)
	}

	res := make(chan arma.Batch)

	go pull(shardName, br.logger, endpoint, requestEnvelope, br.clientConfig(), func(block *common.Block) {
		fb := fabricBatch(*block)
		res <- &fb
	})

	return res
}

func (br *BatchReplicator) findShardID(shardID arma.ShardID) BatcherInfo {
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
	return BatcherInfo{}
}

type BAReplicator struct {
	tlsKey, tlsCert []byte
	config          AssemblerNodeConfig
	logger          arma.Logger
}

func (bar *BAReplicator) Replicate(seq uint64) <-chan arma.BatchAttestation {
	endpoint := bar.config.Consenter.Endpoint

	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		"consensus",
		nil,
		nextSeekInfo(seq),
		int32(0),
		uint64(0),
		nil,
	)

	if err != nil {
		bar.logger.Panicf("Failed creating signed envelope: %v", err)
	}

	res := make(chan arma.BatchAttestation)

	go pull("consensus", bar.logger, endpoint, requestEnvelope, bar.clientConfig(), func(block *common.Block) {
		header := extractHeaderFromBlock(block, bar.logger)

		for _, ab := range header.AvailableBatches {
			bar.logger.Infof("Replicated batch attestation with seq %d and shard %d", ab.Seq(), ab.Shard())
			res <- &ab
		}
	})

	return res
}

func pull(channel string, logger arma.Logger, endpoint string, requestEnvelope *common.Envelope, cc comm.ClientConfig, parseBlock func(block *common.Block)) {
	for {
		time.Sleep(time.Second)

		conn, err := cc.Dial(endpoint)
		if err != nil {
			logger.Errorf("Failed connecting to %s: %v", endpoint, err)
			continue
		}

		abc := orderer.NewAtomicBroadcastClient(conn)

		stream, err := abc.Deliver(context.Background())
		if err != nil {
			logger.Errorf("Failed creating Deliver stream to %s: %v", endpoint, err)
			conn.Close()
			continue
		}

		err = stream.Send(requestEnvelope)
		if err != nil {
			logger.Errorf("Failed sending request envelope to %s: %v", endpoint, err)
			stream.CloseSend()
			conn.Close()
			continue
		}

		pullBlocks(channel, logger, stream, endpoint, conn, parseBlock)
	}
}

func pullBlocks(channel string, logger arma.Logger, stream orderer.AtomicBroadcast_DeliverClient, endpoint string, conn *grpc.ClientConn, parseBlock func(block *common.Block)) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			logger.Errorf("Failed receiving block for %s from %s: %v", channel, endpoint, err)
			stream.CloseSend()
			conn.Close()
			return
		}

		if resp.GetBlock() == nil {
			logger.Errorf("Received a non block message from %s: %v", endpoint, resp)
			stream.CloseSend()
			conn.Close()
			return
		}

		block := resp.GetBlock()
		if block.Data == nil || len(block.Data.Data) == 0 {
			logger.Errorf("Received empty block from %s", endpoint)
			stream.CloseSend()
			conn.Close()
			return
		}

		parseBlock(block)
	}
}

func extractHeaderFromBlock(block *common.Block, logger arma.Logger) *Header {
	decisionAsBytes := block.Data.Data[0]

	headerSize := decisionAsBytes[:4]

	rawHeader := decisionAsBytes[12 : 12+binary.BigEndian.Uint32(headerSize)]

	header := &Header{}
	if err := header.FromBytes(rawHeader); err != nil {
		logger.Panicf("Failed parsing rawHeader")
	}
	return header
}

func (bar *BAReplicator) clientConfig() comm.ClientConfig {
	var tlsCAs [][]byte
	for _, cert := range bar.config.Consenter.TLSCACerts {
		tlsCAs = append(tlsCAs, cert)
	}

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               bar.tlsKey,
			Certificate:       bar.tlsCert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}

func nextSeekInfo(startSeq uint64) *orderer.SeekInfo {
	return &orderer.SeekInfo{
		Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: startSeq}}},
		Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}},
		Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
		ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
	}
}

type fabricBatch common.Block

func (f *fabricBatch) Digest() []byte {
	return f.Header.DataHash
}

func (f *fabricBatch) Requests() arma.BatchedRequests {
	return f.Data.Data
}

func (f *fabricBatch) Party() arma.PartyID {
	buff := f.Metadata.Metadata[5]
	return arma.PartyID(binary.BigEndian.Uint16(buff[:2]))
}

func CreateAssembler(config AssemblerNodeConfig, logger arma.Logger) *Assembler {
	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(config.Directory, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{})
	if err != nil {
		logger.Panicf("Failed creating provider: %v", err)
	}

	armaLedger, err := provider.Open("arma")
	if err != nil {
		logger.Panicf("Failed opening ledger: %v", err)
	}

	blockStores := make(map[string]*blkstorage.BlockStore)
	blockStores["arma"] = armaLedger

	for _, shard := range config.Shards {
		name := fmt.Sprintf("shard%d", shard.ShardId)
		batcherLedger, err := provider.Open(name)
		if err != nil {
			logger.Panicf("Failed opening ledger: %v", err)
		}
		blockStores[name] = batcherLedger
	}

	assembler := NewAssembler(logger, config.Directory, config, blockStores)

	return assembler
}
