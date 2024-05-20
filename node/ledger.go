package node

import (
	arma "arma/pkg"
	"crypto/sha256"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protoutil"
	"slices"
	"sync/atomic"
	"time"
)

type AssemblerLedger struct {
	Logger           arma.Logger
	Ledger           blockledger.ReadWriter
	PrevHash         []byte
	TransactionCount uint64
	NextSeq          uint64
}

func (l *AssemblerLedger) trackThroughput() {
	for {
		commitCountSinceLastProbe := atomic.LoadUint64(&l.TransactionCount)
		atomic.StoreUint64(&l.TransactionCount, 0)
		l.Logger.Infof("Commit throughput: %d", commitCountSinceLastProbe/10)
		time.Sleep(time.Second * 10)
	}
}

func (l *AssemblerLedger) Append(seq uint64, batch arma.Batch, ba arma.BatchAttestation) {
	t1 := time.Now()
	defer func() {
		l.Logger.Infof("Appended block of %d requests to ledger in %v", len(batch.Requests()), time.Since(t1))
	}()
	block := &common.Block{
		Header: &common.BlockHeader{
			Number:       l.NextSeq,
			DataHash:     ba.Digest(),
			PreviousHash: l.PrevHash,
		},
		Data: &common.BlockData{
			Data: batch.Requests(),
		},
		Metadata: &common.BlockMetadata{
			Metadata: [][]byte{{}, {}, {}, {}, {}},
		},
	}

	l.NextSeq++

	defer func() {
		atomic.AddUint64(&l.TransactionCount, uint64(len(batch.Requests())))
	}()

	defer func() {
		if len(block.Data.Data) < 200 {
			return
		}

		latencies := make([]uint64, 0, len(block.Data.Data))
		for _, tx := range block.Data.Data {
			var env common.Envelope
			proto.Unmarshal(tx, &env)
			sendTime := binary.BigEndian.Uint64(env.Payload[8:16])
			latencies = append(latencies, sendTime)
		}

		slices.Sort(latencies)
		nnPercentileIndex := int(0.99 * float64(len(latencies)))
		if nnPercentileIndex >= len(latencies) {
			nnPercentileIndex = len(latencies) - 1
		}

		l.Logger.Infof("99% latency: %d ms", latencies[nnPercentileIndex])
	}()

	l.PrevHash = protoutil.BlockHeaderHash(block.Header)

	if err := l.Ledger.Append(block); err != nil {
		panic(err)
	}
}

type BatcherLedger struct {
	Ledger   blockledger.ReadWriter
	Logger   arma.Logger
	PrevHash []byte
}

func (b *BatcherLedger) Height() uint64 {
	return b.Ledger.Height()
}

func (b *BatcherLedger) Append(partyID arma.PartyID, seq uint64, batchBytes []byte) {
	b.Logger.Infof("Appended block with sequence %d of size %d bytes", seq, len(batchBytes))
	buff := make([]byte, 2)
	binary.BigEndian.PutUint16(buff[:2], uint16(partyID))

	digest := sha256.Sum256(batchBytes)

	block := &common.Block{
		Header: &common.BlockHeader{Number: seq, DataHash: digest[:]},
		Data: &common.BlockData{
			Data: arma.BatchFromRaw(batchBytes),
		},
		Metadata: &common.BlockMetadata{
			Metadata: [][]byte{{}, {}, {}, {}, {}, buff},
		},
	}

	block.Header.DataHash, _ = protoutil.BlockDataHash(block.Data)
	block.Header.PreviousHash = b.PrevHash

	b.PrevHash = protoutil.BlockHeaderHash(block.Header)

	if err := b.Ledger.Append(block); err != nil {
		panic(err)
	}
}

type ConsensusLedger struct {
	PrevHash []byte
	ledger   blockledger.ReadWriter
	onCommit func(block *common.Block)
}

func (c *ConsensusLedger) Append(bytes []byte) {
	headerSizeBuff := bytes[:4]
	headerSize := binary.BigEndian.Uint32(headerSizeBuff)
	headerBytes := bytes[12 : 12+headerSize]
	header := &Header{}
	if err := header.FromBytes(headerBytes); err != nil {
		panic(err)
	}

	digest := sha256.Sum256(bytes)

	block := &common.Block{
		Header: &common.BlockHeader{
			Number:       header.Num - 1,
			DataHash:     digest[:],
			PreviousHash: c.PrevHash,
		},
		Data: &common.BlockData{
			Data: [][]byte{bytes},
		},
		Metadata: &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {}, {}}},
	}

	c.onCommit(block)

	defer func() {
		c.PrevHash = protoutil.BlockHeaderHash(block.Header)
	}()

	c.ledger.Append(block)
}
