/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"bytes"

	"github.com/pkg/errors"
)

type AvailableBlock struct {
	Header *BlockHeader
	Batch  *AvailableBatch
}

const availableBlockSerializedSize = blockHeaderBytesSize + availableBatchSerializedSize

func (ab *AvailableBlock) Serialize() []byte {
	if ab.Header == nil || ab.Batch == nil {
		panic("nil header or batch")
	}
	if !bytes.Equal(ab.Header.Digest, ab.Batch.Digest()) {
		panic("header digest is not equal to batch digest")
	}
	buff := make([]byte, blockHeaderBytesSize+availableBatchSerializedSize)
	copy(buff, ab.Header.Bytes())
	copy(buff[blockHeaderBytesSize:], ab.Batch.Serialize())
	return buff
}

func (ab *AvailableBlock) Deserialize(bytes []byte) error {
	if bytes == nil {
		return errors.New("nil bytes")
	}
	if len(bytes) != blockHeaderBytesSize+availableBatchSerializedSize {
		return errors.Errorf("len of bytes %d does not equal the block header size plus available batch size %d", len(bytes), blockHeaderBytesSize+availableBatchSerializedSize)
	}
	ab.Header = &BlockHeader{}
	if err := ab.Header.FromBytes(bytes[:blockHeaderBytesSize]); err != nil {
		return errors.Errorf("could not deserialize header; err: %v", err)
	}
	ab.Batch = &AvailableBatch{}
	if err := ab.Batch.Deserialize(bytes[blockHeaderBytesSize:]); err != nil {
		return errors.Errorf("could not deserialize batch; err: %v", err)
	}
	return nil
}
