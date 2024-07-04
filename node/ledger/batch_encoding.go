package ledger

import (
	arma "arma/pkg"
	"crypto/sha256"
	"encoding/binary"

	"github.com/hyperledger/fabric-protos-go/common"
)

// BlockMetadataIndex_Party is one location after the last entry that Fabric uses.
var BlockMetadataIndex_Party = len(common.BlockMetadataIndex_name)

// FabricBatch is an arma.Batch encoded in a Fabric block
type FabricBatch common.Block

func (b *FabricBatch) Digest() []byte {
	return b.Header.DataHash
}

func (b *FabricBatch) Requests() arma.BatchedRequests {
	return b.Data.Data
}

func (b *FabricBatch) Party() arma.PartyID {
	buff := b.Metadata.Metadata[BlockMetadataIndex_Party]
	return arma.PartyID(binary.BigEndian.Uint16(buff[:2]))
}

func NewFabricBatchFromRaw(partyID arma.PartyID, seq uint64, batchBytes []byte, prevHash []byte) (*FabricBatch, error) {
	batchedRequests := arma.BatchFromRaw(batchBytes) // TODO return an error, don't panic. See: https://github.ibm.com/decentralized-trust-research/ARMA/issues/132

	buff := make([]byte, 2)
	binary.BigEndian.PutUint16(buff[:2], uint16(partyID))

	digest := sha256.Sum256(batchBytes)

	block := &common.Block{
		Header: &common.BlockHeader{
			Number:       seq,
			PreviousHash: prevHash,
			DataHash:     digest[:],
		},
		Data: &common.BlockData{
			Data: batchedRequests,
		},
		Metadata: &common.BlockMetadata{
			Metadata: [][]byte{{}, {}, {}, {}, {}, buff},
		},
	}

	return (*FabricBatch)(block), nil
}
