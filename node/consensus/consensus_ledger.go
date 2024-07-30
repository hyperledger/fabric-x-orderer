package consensus

import (
	"crypto/sha256"
	"encoding/binary"

	"arma/node/consensus/state"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protoutil"
)

type ConsensusLedger struct {
	PrevHash []byte
	ledger   blockledger.ReadWriter
	onCommit func(block *common.Block)
}

func (c *ConsensusLedger) Append(bytes []byte) {
	headerSizeBuff := bytes[:4]
	headerSize := binary.BigEndian.Uint32(headerSizeBuff)
	headerBytes := bytes[12 : 12+headerSize]
	header := &state.Header{}
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
