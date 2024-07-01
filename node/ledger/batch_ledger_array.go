package ledger

import (
	"arma/pkg"
	"crypto/sha256"
	"encoding/binary"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protoutil"
)

type BatcherLedger struct {
	Ledger   blockledger.ReadWriter
	Logger   arma.Logger
	PrevHash []byte
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

func (b *BatcherLedger) Height(partyID arma.PartyID) uint64 {
	//TODO get the correct ledger part using partyID
	return b.Ledger.Height()
}

func (b *BatcherLedger) RetrieveBatchByNumber(partyID arma.PartyID, seq uint64) arma.Batch {
	//TODO get the correct ledger part using partyID, then retrieve the batch
	return nil
}
