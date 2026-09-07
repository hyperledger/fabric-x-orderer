/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"fmt"

	"github.com/hyperledger/fabric-x-orderer/common/ledger/util"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/util/leveldbhelper"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	blockNumIdxKeyPrefix = 'n'
	indexSavePointKeyStr = "indexCheckpointKey"
)

var (
	indexSavePointKey              = []byte(indexSavePointKeyStr)
	errIndexSavePointKeyNotPresent = errors.New("NoBlockIndexed")
)

type blockIdxInfo struct {
	blockNum uint64
	flp      *fileLocPointer
}

// blockIndex maps a block number to the location of the block in the block files. It is the only
// index the orderer keeps: blocks are addressed by number on every read path, and nothing addresses
// them by hash or by the transactions they carry.
type blockIndex struct {
	db *leveldbhelper.DBHandle
}

func newBlockIndex(db *leveldbhelper.DBHandle) *blockIndex {
	return &blockIndex{db: db}
}

func (index *blockIndex) getLastBlockIndexed() (uint64, error) {
	var blockNumBytes []byte
	var err error
	if blockNumBytes, err = index.db.Get(indexSavePointKey); err != nil {
		return 0, err
	}
	if blockNumBytes == nil {
		return 0, errIndexSavePointKeyNotPresent
	}
	return decodeBlockNum(blockNumBytes), nil
}

func (index *blockIndex) indexBlock(blockIdxInfo *blockIdxInfo) error {
	logger.Debugf("Indexing block [%s]", blockIdxInfo)
	batch := index.db.NewUpdateBatch()
	batch.Put(constructBlockNumKey(blockIdxInfo.blockNum), blockIdxInfo.flp.marshal())
	batch.Put(indexSavePointKey, encodeBlockNum(blockIdxInfo.blockNum))
	// Setting sync to true as a precaution, false may be an ok optimization after further testing.
	return index.db.WriteBatch(batch, true)
}

func (index *blockIndex) getBlockLocByBlockNum(blockNum uint64) (*fileLocPointer, error) {
	b, err := index.db.Get(constructBlockNumKey(blockNum))
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, errors.Errorf("no such block number [%d] in index", blockNum)
	}
	blkLoc := &fileLocPointer{}
	if err := blkLoc.unmarshal(b); err != nil {
		return nil, err
	}
	return blkLoc, nil
}

func constructBlockNumKey(blockNum uint64) []byte {
	blkNumBytes := util.EncodeOrderPreservingVarUint64(blockNum)
	return append([]byte{blockNumIdxKeyPrefix}, blkNumBytes...)
}

func encodeBlockNum(blockNum uint64) []byte {
	return protowire.AppendVarint(nil, blockNum)
}

func decodeBlockNum(blockNumBytes []byte) uint64 {
	blockNum, num := protowire.ConsumeVarint(blockNumBytes)
	if num < 0 {
		return 0
	}
	return blockNum
}

type locPointer struct {
	offset      int
	bytesLength int
}

func (lp *locPointer) String() string {
	return fmt.Sprintf("offset=%d, bytesLength=%d",
		lp.offset, lp.bytesLength)
}

// fileLocPointer
type fileLocPointer struct {
	fileSuffixNum int
	locPointer
}

func (flp *fileLocPointer) marshal() []byte {
	var buf []byte
	buf = protowire.AppendVarint(buf, uint64(flp.fileSuffixNum))
	buf = protowire.AppendVarint(buf, uint64(flp.offset))
	buf = protowire.AppendVarint(buf, uint64(flp.bytesLength))

	return buf
}

func (flp *fileLocPointer) unmarshal(b []byte) error {
	var position int

	i, n := protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return errors.Wrapf(protowire.ParseError(n), "unexpected error while unmarshalling bytes [%#v] into fileLocPointer", b)
	}
	position += n
	flp.fileSuffixNum = int(i)

	i, n = protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return errors.Wrapf(protowire.ParseError(n), "unexpected error while unmarshalling bytes [%#v] into fileLocPointer", b)
	}
	position += n
	flp.offset = int(i)

	i, n = protowire.ConsumeVarint(b[position:])
	if n < 0 {
		return errors.Wrapf(protowire.ParseError(n), "unexpected error while unmarshalling bytes [%#v] into fileLocPointer", b)
	}
	flp.bytesLength = int(i)
	return nil
}

func (flp *fileLocPointer) String() string {
	return fmt.Sprintf("fileSuffixNum=%d, %s", flp.fileSuffixNum, flp.locPointer.String())
}

func (blockIdxInfo *blockIdxInfo) String() string {
	return fmt.Sprintf("blockNum=%d, flp=[%s]", blockIdxInfo.blockNum, blockIdxInfo.flp)
}
