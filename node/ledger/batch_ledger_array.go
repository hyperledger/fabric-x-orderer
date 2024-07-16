package ledger

import (
	arma "arma/pkg"
	"crypto/sha256"
	"encoding/binary"
	"slices"

	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// BatchLedgerArray holds a BatchLedgerPart for each party, for a given shard.
type BatchLedgerArray struct {
	shardID     arma.ShardID                      // The shard this array belongs to.
	partyID     arma.PartyID                      // The party that operates this object.
	ledgerParts map[arma.PartyID]*BatchLedgerPart // A BatchLedgerPart for each party in the system.
	provider    *blkstorage.BlockStoreProvider
	logger      arma.Logger
}

func NewBatchLedgerArray(shardID arma.ShardID, partyID arma.PartyID, parties []arma.PartyID, batchLedgerDir string, logger arma.Logger) (*BatchLedgerArray, error) {
	if !slices.Contains(parties, partyID) {
		return nil, errors.Errorf("partyID %d not in parties %v", partyID, parties)
	}

	logger.Infof("Creating batch ledger array for shard=%d, party=%d, parties=%v, dir=%s", shardID, partyID, parties, batchLedgerDir)

	ledgerPartsMap := make(map[arma.PartyID]*BatchLedgerPart)

	//TODO We are using the Fabric block storage for now even though it is not ideal.
	// (1) We don't need the hash chain, and
	// (2) we don't need to index TXs.
	// In addition, in the future we may want to (3) prune batches that had already been received by a quorum of
	// assemblers; this however requires additional protocols between assemblers and consensus.
	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(batchLedgerDir, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{})
	if err != nil {
		return nil, errors.Errorf("failed creating block provider: %s", err)
	}

	for _, primaryPartyID := range parties {
		part, err := newBatchLedgerPart(provider, shardID, partyID, primaryPartyID, logger)
		if err != nil {
			return nil, err
		}

		ledgerPartsMap[primaryPartyID] = part
	}

	return &BatchLedgerArray{
		shardID:     shardID,
		partyID:     partyID,
		ledgerParts: ledgerPartsMap,
		provider:    provider,
		logger:      logger,
	}, nil
}

func (bla *BatchLedgerArray) ShardID() arma.ShardID {
	return bla.shardID
}

func (bla *BatchLedgerArray) Height(partyID arma.PartyID) uint64 {
	part, ok := bla.ledgerParts[partyID]
	if !ok {
		bla.logger.Panicf("partyID does not exist: %d", partyID)
	}
	return part.Height()
}

func (bla *BatchLedgerArray) Append(partyID arma.PartyID, seq uint64, batchBytes []byte) {
	part, ok := bla.ledgerParts[partyID]
	if !ok {
		bla.logger.Panicf("partyID does not exist: %d", partyID)
	}
	part.Append(seq, batchBytes)
}

func (bla *BatchLedgerArray) RetrieveBatchByNumber(partyID arma.PartyID, seq uint64) arma.Batch {
	part, ok := bla.ledgerParts[partyID]
	if !ok {
		bla.logger.Panicf("partyID does not exist: %d", partyID)
	}
	return part.RetrieveBatchByNumber(seq)
}

func (bla *BatchLedgerArray) Part(partyID arma.PartyID) *BatchLedgerPart {
	part, ok := bla.ledgerParts[partyID]
	if !ok {
		bla.logger.Panicf("partyID does not exist: %d", partyID)
	}
	return part
}

func (bla *BatchLedgerArray) List() ([]string, error) {
	return bla.provider.List()
}

func (bla *BatchLedgerArray) Close() {
	bla.provider.Close()
}

//TODO deprecate it.
// This implementation will be removed from use once we implement some prerequisites for the integration of the new
// implementation above, such as augmenting the delivery service.
// This is currently being used in the batcher and assembler.

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
		Header: &common.BlockHeader{
			Number:   seq,
			DataHash: digest[:]},
		Data: &common.BlockData{
			Data: arma.BatchFromRaw(batchBytes),
		},
		Metadata: &common.BlockMetadata{
			Metadata: [][]byte{{}, {}, {}, {}, {}, buff},
		},
	}

	// Note: We do this only because we reuse the Fabric ledger, we don't really need a hash chain here.
	// block.Header.DataHash, _ = protoutil.BlockDataHash(block.Data)
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
