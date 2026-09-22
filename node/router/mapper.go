/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"encoding/binary"
	"hash/crc64"
)

type ShardMapper interface {
	Map(request []byte) (shard uint16, reqID []byte)
}

type MapperCRC64 struct {
	ShardCount uint16
}

func CreateMapperCRC64(shardCount uint16) MapperCRC64 {
	return MapperCRC64{
		ShardCount: shardCount,
	}
}

func (m MapperCRC64) Map(request []byte) (shard uint16, reqID []byte) {
	reqID, shardID := CRC64RequestToShard(m.ShardCount)(request)
	return shardID, reqID
}

var table = crc64.MakeTable(crc64.ECMA)

func CRC64RequestToShard(shardCount uint16) func([]byte) ([]byte, uint16) {
	return func(request []byte) ([]byte, uint16) {
		reqID := crc64.Checksum(request, table)
		buff := make([]byte, 8)
		binary.BigEndian.PutUint64(buff, reqID)

		return buff, uint16(reqID) % shardCount
	}
}
