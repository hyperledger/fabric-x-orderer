package ledger

import (
	"crypto/sha256"
	"encoding/binary"

	"github.ibm.com/decentralized-trust-research/arma/common/ledger/blkstorage"
	"github.ibm.com/decentralized-trust-research/arma/common/ledger/blockledger"
	"github.ibm.com/decentralized-trust-research/arma/common/ledger/blockledger/fileledger"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/append_listener.go . AppendListener
type AppendListener interface {
	OnAppend(block *common.Block)
}

type ConsensusLedger struct {
	blockStore     *blkstorage.BlockStore
	provider       *blkstorage.BlockStoreProvider
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

	blockStore, err := provider.Open("consensus")
	if err != nil {
		return nil, errors.Wrap(err, "failed creating consensus ledger")
	}

	fl := fileledger.NewFileLedger(blockStore)

	consensusLedger := &ConsensusLedger{
		blockStore: blockStore,
		provider:   provider,
		ledger:     fl,
	}

	height := fl.Height()
	if height > 0 {
		block, err := fl.RetrieveBlockByNumber(height - 1)
		if err != nil {
			return nil, errors.Wrap(err, "failed retrieving last block")
		}
		consensusLedger.prevHash = protoutil.BlockHeaderHash(block.Header)
	}

	return consensusLedger, nil
}

func (l *ConsensusLedger) RegisterAppendListener(listener AppendListener) {
	l.appendListener = listener
}

func (c *ConsensusLedger) Append(bytes []byte) {
	headerSizeBuff := bytes[:4]
	headerSize := binary.BigEndian.Uint32(headerSizeBuff)
	headerBytes := bytes[12 : 12+headerSize]
	header := &state.Header{}
	if err := header.Deserialize(headerBytes); err != nil {
		panic(err)
	}

	digest := sha256.Sum256(bytes)

	block := &common.Block{
		Header: &common.BlockHeader{
			Number:       uint64(header.Num),
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

func (c *ConsensusLedger) Close() {
	c.blockStore.Shutdown()
	c.provider.Close()
}
