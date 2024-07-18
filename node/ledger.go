package node

import (
	"crypto/sha256"
	"encoding/binary"
	"sync/atomic"
	"time"

	arma "arma/core"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protoutil"
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

	l.PrevHash = protoutil.BlockHeaderHash(block.Header)

	if err := l.Ledger.Append(block); err != nil {
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
