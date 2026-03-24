/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
	"slices"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blkstorage"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/pkg/errors"
)

// BatchLedgerArray holds a BatchLedgerPart for each party, for a given shard.
type BatchLedgerArray struct {
	shardID     types.ShardID                      // The shard this array belongs to.
	partyID     types.PartyID                      // The party that operates this object.
	ledgerParts map[types.PartyID]*BatchLedgerPart // A BatchLedgerPart for each party in the system.
	provider    *blkstorage.BlockStoreProvider
	logger      *flogging.FabricLogger
}

func NewBatchLedgerArray(shardID types.ShardID, partyID types.PartyID, parties []types.PartyID, batchLedgerDir string, logger *flogging.FabricLogger) (*BatchLedgerArray, error) {
	if !slices.Contains(parties, partyID) {
		return nil, errors.Errorf("partyID %d not in parties %v", partyID, parties)
	}

	logger.Infof("Creating batch ledger array for shard=%d, party=%d, parties=%v, dir=%s", shardID, partyID, parties, batchLedgerDir)

	ledgerPartsMap := make(map[types.PartyID]*BatchLedgerPart)

	// TODO We are using the Fabric block storage for now even though it is not ideal.
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

	names, err := provider.List()
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		_, primaryPartyID, err := ChannelNameToShardParty(name)
		if err != nil {
			return nil, err
		}
		if ledgerPartsMap[primaryPartyID] != nil {
			continue
		}
		part, err := newBatchLedgerPart(provider, shardID, partyID, primaryPartyID, logger)
		if err != nil {
			return nil, err
		}

		ledgerPartsMap[primaryPartyID] = part
	}
	// TODO consider saving parties and checking when appending to avoid mistakes (appending to a stale part)

	return &BatchLedgerArray{
		shardID:     shardID,
		partyID:     partyID,
		ledgerParts: ledgerPartsMap,
		provider:    provider,
		logger:      logger,
	}, nil
}

func (bla *BatchLedgerArray) ShardID() types.ShardID {
	return bla.shardID
}

func (bla *BatchLedgerArray) Height(partyID types.PartyID) uint64 {
	part, ok := bla.ledgerParts[partyID]
	if !ok {
		bla.logger.Panicf("partyID does not exist: %d", partyID)
	}
	return part.Height()
}

func (bla *BatchLedgerArray) Append(partyID types.PartyID, batchSeq types.BatchSequence, configSeq types.ConfigSequence, batchedRequests types.BatchedRequests) {
	part, ok := bla.ledgerParts[partyID]
	if !ok {
		bla.logger.Panicf("partyID does not exist: %d", partyID)
	}
	part.Append(batchSeq, configSeq, batchedRequests)
}

func (bla *BatchLedgerArray) RetrieveBatchByNumber(partyID types.PartyID, seq uint64) types.Batch {
	part, ok := bla.ledgerParts[partyID]
	if !ok {
		bla.logger.Panicf("partyID does not exist: %d", partyID)
	}
	return part.RetrieveBatchByNumber(seq)
}

func (bla *BatchLedgerArray) Part(partyID types.PartyID) *BatchLedgerPart {
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
