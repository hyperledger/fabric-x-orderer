/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package core

import (
	"encoding/binary"
	"hash/crc64"

	"github.com/hyperledger/fabric-x-orderer/common/types"
)

type Router struct {
	Logger     types.Logger
	ShardCount uint16
}

func (r *Router) Map(request []byte) (shard uint16, reqID []byte) {
	reqID, shardID := CRC64RequestToShard(r.ShardCount)(request)
	r.Logger.Debugf("Forwarding request %d to shard %d", reqID, shardID)
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
