package node

import (
	arma "arma/pkg"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
	_ "github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
	"github.com/hyperledger/fabric/common/metrics/disabled"
	_ "github.com/hyperledger/fabric/common/metrics/disabled"
	"github.com/hyperledger/fabric/protoutil"
)

func NewAssemblerLedger(name, dir string) (*AssemblerLedger, error) {
	lf, err := fileledger.New(dir, &disabled.Provider{})
	if err != nil {
		return nil, err
	}

	l, err := lf.GetOrCreate(name)
	if err != nil {
		return nil, err
	}

	return &AssemblerLedger{
		Ledger: l,
	}, nil
}

type AssemblerLedger struct {
	Ledger blockledger.ReadWriter
}

func (l *AssemblerLedger) Append(seq uint64, batch arma.Batch, ba arma.BatchAttestation) {
	block := &common.Block{}
	if err := proto.Unmarshal(ba.Serialize(), block); err != nil {
		panic(err)
	}

	block.Data.Data = batch.Requests()

	if err := l.Ledger.Append(block); err != nil {
		panic(err)
	}
}

func NewBatcherLedger(name, dir string) (*BatcherLedger, error) {
	lf, err := fileledger.New(dir, &disabled.Provider{})
	if err != nil {
		return nil, err
	}

	l, err := lf.GetOrCreate(name)
	if err != nil {
		return nil, err
	}

	return &BatcherLedger{
		Ledger: l,
	}, nil
}

type BatcherLedger struct {
	ShardID  uint16
	Ledger   blockledger.ReadWriter
	PrevHash []byte
}

func (b *BatcherLedger) Append(_ uint16, seq uint64, batchBytes []byte) {
	shardBuff := make([]byte, 10)
	binary.BigEndian.PutUint16(shardBuff[:2], b.ShardID)
	binary.BigEndian.PutUint64(shardBuff[2:], seq)
	block := &common.Block{
		Header: &common.BlockHeader{Number: seq},
		Data: &common.BlockData{
			Data: arma.BatchFromRaw(batchBytes),
		},
		Metadata: &common.BlockMetadata{
			Metadata: [][]byte{{}, {}, {}, {}, {}, shardBuff},
		},
	}

	block.Header.DataHash = protoutil.BlockDataHash(block.Data)
	block.Header.PreviousHash = b.PrevHash

	b.PrevHash = protoutil.BlockHeaderHash(block.Header)

	if err := b.Ledger.Append(block); err != nil {
		panic(err)
	}
}
