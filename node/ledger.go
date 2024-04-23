package node

import (
	arma "arma/pkg"
	"crypto/sha256"
	"encoding/binary"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protoutil"
)

type AssemblerLedger struct {
	Ledger blockledger.ReadWriter
}

func (l *AssemblerLedger) Append(seq uint64, batch arma.Batch, ba arma.BatchAttestation) {
	block := &common.Block{
		Header: &common.BlockHeader{
			Number:   ba.Seq(),
			DataHash: ba.Digest(),
		},
		Data: &common.BlockData{
			Data: batch.Requests(),
		},
		Metadata: &common.BlockMetadata{
			Metadata: [][]byte{{}, {}, {}, {}, {}},
		},
	}

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
			Number:       header.Num,
			DataHash:     digest[:],
			PreviousHash: c.PrevHash,
		},
		Data: &common.BlockData{
			Data: [][]byte{bytes},
		},
		Metadata: &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {}, {}}},
	}

	defer func() {
		c.PrevHash = protoutil.BlockHeaderHash(block.Header)
	}()

	c.ledger.Append(block)
}
