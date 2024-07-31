package ledger

import (
	"crypto/sha256"
	"encoding/binary"

	"arma/node/consensus/state"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

type AppendListener interface {
	OnAppend(block *common.Block)
}

type ConsensusLedger struct {
	prevHash       []byte
	ledger         blockledger.ReadWriter
	appendListener AppendListener
}

func NewConsensusLedger(ledgerDir string) (*ConsensusLedger, error) {
	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(ledgerDir, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{})
	if err != nil {
		return nil, errors.Wrap(err, "failed creating block provider")
	}

	consensusLedger, err := provider.Open("consensus")
	if err != nil {
		return nil, errors.Wrap(err, "failed creating consensus ledger")
	}

	fl := fileledger.NewFileLedger(consensusLedger)
	consLedger := newConsensusLedger(fl)

	return consLedger, nil
}

func newConsensusLedger(rwLedger blockledger.ReadWriter) *ConsensusLedger {
	l := &ConsensusLedger{
		ledger: rwLedger,
	}

	return l
}

func (l *ConsensusLedger) RegisterAppendListener(listener AppendListener) {
	l.appendListener = listener
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
			PreviousHash: c.prevHash,
		},
		Data: &common.BlockData{
			Data: [][]byte{bytes},
		},
		Metadata: &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {}, {}}},
	}

	err := c.ledger.Append(block)
	if err != nil {
		panic(err)
	}

	c.prevHash = protoutil.BlockHeaderHash(block.Header)

	if c.appendListener != nil {
		c.appendListener.OnAppend(block)
	}
}

func (c *ConsensusLedger) Iterator(startType *ab.SeekPosition) (blockledger.Iterator, uint64) {
	return c.ledger.Iterator(startType)
}

func (c *ConsensusLedger) Height() uint64 {
	return c.ledger.Height()
}

func (c *ConsensusLedger) RetrieveBlockByNumber(blockNumber uint64) (*common.Block, error) {
	return c.ledger.RetrieveBlockByNumber(blockNumber)
}
