/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"
)

func serializeBlock(block *common.Block) []byte {
	var buf []byte
	buf = addHeaderBytes(block.Header, buf)
	buf = addDataBytes(block.Data, buf)
	buf = addMetadataBytes(block.Metadata, buf)
	return buf
}

func deserializeBlock(serializedBlockBytes []byte) (*common.Block, error) {
	block := &common.Block{}
	var err error
	b := newBuffer(serializedBlockBytes)
	if block.Header, err = extractHeader(b); err != nil {
		return nil, err
	}
	if block.Data, err = extractData(b); err != nil {
		return nil, err
	}
	if block.Metadata, err = extractMetadata(b); err != nil {
		return nil, err
	}
	return block, nil
}

func extractSerializedBlockHeader(serializedBlockBytes []byte) (*common.BlockHeader, error) {
	return extractHeader(newBuffer(serializedBlockBytes))
}

func addHeaderBytes(blockHeader *common.BlockHeader, buf []byte) []byte {
	buf = protowire.AppendVarint(buf, blockHeader.Number)
	buf = protowire.AppendBytes(buf, blockHeader.DataHash)
	buf = protowire.AppendBytes(buf, blockHeader.PreviousHash)
	return buf
}

func addDataBytes(blockData *common.BlockData, buf []byte) []byte {
	buf = protowire.AppendVarint(buf, uint64(len(blockData.Data)))
	for _, txEnvelopeBytes := range blockData.Data {
		buf = protowire.AppendBytes(buf, txEnvelopeBytes)
	}
	return buf
}

func addMetadataBytes(blockMetadata *common.BlockMetadata, buf []byte) []byte {
	numItems := uint64(0)
	if blockMetadata != nil {
		numItems = uint64(len(blockMetadata.Metadata))
	}
	buf = protowire.AppendVarint(buf, numItems)
	if blockMetadata == nil {
		return buf
	}
	for _, b := range blockMetadata.Metadata {
		buf = protowire.AppendBytes(buf, b)
	}
	return buf
}

func extractHeader(buf *buffer) (*common.BlockHeader, error) {
	header := &common.BlockHeader{}
	var err error
	if header.Number, err = buf.DecodeVarint(); err != nil {
		return nil, errors.Wrap(err, "error decoding the block number")
	}
	if header.DataHash, err = buf.DecodeRawBytes(false); err != nil {
		return nil, errors.Wrap(err, "error decoding the data hash")
	}
	if header.PreviousHash, err = buf.DecodeRawBytes(false); err != nil {
		return nil, errors.Wrap(err, "error decoding the previous hash")
	}
	if len(header.PreviousHash) == 0 {
		header.PreviousHash = nil
	}
	return header, nil
}

func extractData(buf *buffer) (*common.BlockData, error) {
	data := &common.BlockData{}
	var numItems uint64
	var err error

	if numItems, err = buf.DecodeVarint(); err != nil {
		return nil, errors.Wrap(err, "error decoding the length of block data")
	}
	for i := uint64(0); i < numItems; i++ {
		var txEnvBytes []byte
		if txEnvBytes, err = buf.DecodeRawBytes(false); err != nil {
			return nil, errors.Wrap(err, "error decoding the transaction envelope")
		}
		data.Data = append(data.Data, txEnvBytes)
	}
	return data, nil
}

func extractMetadata(buf *buffer) (*common.BlockMetadata, error) {
	metadata := &common.BlockMetadata{}
	var numItems uint64
	var metadataEntry []byte
	var err error
	if numItems, err = buf.DecodeVarint(); err != nil {
		return nil, errors.Wrap(err, "error decoding the length of block metadata")
	}
	for i := uint64(0); i < numItems; i++ {
		if metadataEntry, err = buf.DecodeRawBytes(false); err != nil {
			return nil, errors.Wrap(err, "error decoding the block metadata")
		}
		metadata.Metadata = append(metadata.Metadata, metadataEntry)
	}
	return metadata, nil
}
