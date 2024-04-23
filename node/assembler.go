package node

import (
	arma "arma/pkg"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
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
}

func NewAssembler(logger arma.Logger, dir string, config AssemblerNodeConfig, factory blockledger.Factory) *Assembler {
	id := config.PartyId
	name := fmt.Sprintf("assembler%d", id)
	dir = filepath.Join(dir, name)

	index := NewIndex(config, factory, logger)

	tlsKey, err := base64.StdEncoding.DecodeString(string(config.TLSPrivateKeyFile))
	if err != nil {
		logger.Panicf("Cannot decode TLS key as base64 string: %v", err)
	}

	tlsCert, err := base64.StdEncoding.DecodeString(string(config.TLSCertificateFile))
	if err != nil {
		logger.Panicf("Cannot decode TLS certificate as base64 string: %v", err)
	}

	ledger, err := factory.GetOrCreate("arma")
	if err != nil {
		logger.Panicf("Failed creating arma ledger: %v", err)
	}

	baReplicator := &BAReplicator{
		logger:  logger,
		config:  config,
		tlsKey:  tlsKey,
		tlsCert: tlsCert,
	}

	br := &BatchReplicator{
		logger:  logger,
		config:  config,
		tlsKey:  tlsKey,
		tlsCert: tlsCert,
		index:   index.indexes,
	}

	var shards []arma.ShardID
	for _, shard := range config.Shards {
		shards = append(shards, arma.ShardID(shard.ShardId))
	}

	assembler := &Assembler{
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

	assembler.assembler.Run()

	return assembler
}

type FactoryCreator func(string) blockledger.Factory

type Index struct {
	indexes map[arma.ShardID]blockledger.ReadWriter
	logger  arma.Logger
}

func NewIndex(config AssemblerNodeConfig, factory blockledger.Factory, logger arma.Logger) *Index {
	indexes := make(map[arma.ShardID]blockledger.ReadWriter)

	for _, s := range config.Shards {
		shardID := s.ShardId
		name := fmt.Sprintf("shard%d", shardID)
		rw, err := factory.GetOrCreate(name)
		if err != nil {
			logger.Panicf("Failed creating ledger %s: %v", name, err)
		}

		indexes[arma.ShardID(shardID)] = rw
	}

	return &Index{logger: logger, indexes: indexes}
}

func (i *Index) Index(party arma.PartyID, shard arma.ShardID, sequence uint64, batch arma.Batch) {
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

	i.indexes[shard].Append(block)
}

func (i *Index) Retrieve(party arma.PartyID, shard arma.ShardID, sequence uint64, digest []byte) (arma.Batch, bool) {
	reader := i.indexes[shard]
	block := GetBlock(reader, sequence)

	if block == nil {
		return nil, false
	}

	fb := fabricBatch(*block)
	return &fb, true
}

type BatchReplicator struct {
	index           map[arma.ShardID]blockledger.ReadWriter
	tlsKey, tlsCert []byte
	config          AssemblerNodeConfig
	logger          arma.Logger
}

func (br *BatchReplicator) clientConfig() comm.ClientConfig {
	var tlsCAs [][]byte
	for _, certbase64 := range br.config.Consenter.TLSCACerts {
		cert, err := base64.StdEncoding.DecodeString(string(certbase64))
		if err != nil {
			br.logger.Panicf("Failed decoding TLS CA cert: %s", string(certbase64))
		}
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

	seq := br.index[shardID].Height()

	endpoint := batcherToPullFrom.Endpoint

	requestEnvelope, err := protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		fmt.Sprintf("shard%d", shardID),
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

	go pull(br.logger, endpoint, requestEnvelope, br.clientConfig(), func(block *common.Block) {
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

	go pull(bar.logger, endpoint, requestEnvelope, bar.clientConfig(), func(block *common.Block) {
		header := extractHeaderFromBlock(block, bar.logger)

		fmt.Println("Received consensus block with", len(header.AvailableBatches), "batch attestations")

		for _, ab := range header.AvailableBatches {
			bar.logger.Infof("Replicated batch attestation with seq %d and shard %d", ab.Seq(), ab.Shard())
			res <- &ab
		}
	})

	return res
}

func pull(logger arma.Logger, endpoint string, requestEnvelope *common.Envelope, cc comm.ClientConfig, parseBlock func(block *common.Block)) {
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

		pullBlocks(logger, stream, endpoint, conn, parseBlock)
	}
}

func pullBlocks(logger arma.Logger, stream orderer.AtomicBroadcast_DeliverClient, endpoint string, conn *grpc.ClientConn, parseBlock func(block *common.Block)) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			logger.Errorf("Failed receiving block from %s: %v", endpoint, err)
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
	for _, certbase64 := range bar.config.Consenter.TLSCACerts {
		cert, err := base64.StdEncoding.DecodeString(string(certbase64))
		if err != nil {
			bar.logger.Panicf("Failed decoding TLS CA cert: %s", string(certbase64))
		}
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

func GetBlock(reader blockledger.Reader, index uint64) *common.Block {
	iterator, _ := reader.Iterator(&orderer.SeekPosition{
		Type: &orderer.SeekPosition_Specified{
			Specified: &orderer.SeekSpecified{Number: index},
		},
	})
	if iterator == nil {
		return nil
	}
	defer iterator.Close()
	block, status := iterator.Next()
	if status != common.Status_SUCCESS {
		return nil
	}
	return block
}
