/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"fmt"
	"testing"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/stretchr/testify/assert"
)

func TestAvailableBatchOrdered(t *testing.T) {
	var ab AvailableBatch
	ab.digest = make([]byte, 32)
	ab.primary = 42
	ab.shard = 666
	ab.seq = 100
	assert.Equal(t, "Sh,Pr,Sq,Dg: <666,42,100,0000000000000000000000000000000000000000000000000000000000000000>", ab.String())

	var oi OrderingInformation
	oi.DecisionNum = 10
	oi.BatchIndex = 2
	oi.BatchCount = 3
	oi.Signatures = make([]smartbft_types.Signature, 3)
	oi.CommonBlock = &common.Block{
		Header: &common.BlockHeader{
			Number:       3,
			PreviousHash: []byte{1, 2, 3, 4},
			DataHash:     []byte{0xa, 0xb, 0xc, 0xd},
		},
	}
	assert.Equal(t, "DecisionNum: 10, BatchIndex: 2, BatchCount: 3; No. Sigs: 3, Common Block: Number: 3, PreviousHash: 01020304, DataHash: 0a0b0c0d", oi.String())

	var abo AvailableBatchOrdered
	abo.AvailableBatch = &ab
	abo.OrderingInformation = &oi

	assert.Equal(t,
		"{AvailableBatch:Sh,Pr,Sq,Dg: <666,42,100,0000000000000000000000000000000000000000000000000000000000000000> OrderingInformation:DecisionNum: 10, BatchIndex: 2, BatchCount: 3; No. Sigs: 3, Common Block: Number: 3, PreviousHash: 01020304, DataHash: 0a0b0c0d}",
		fmt.Sprintf("%+v", abo))

	oi.CommonBlock = &common.Block{Header: &common.BlockHeader{Number: 1, PreviousHash: []byte{1}, DataHash: []byte{2}}}
	assert.Equal(t, "DecisionNum: 10, BatchIndex: 2, BatchCount: 3; No. Sigs: 3, Common Block: Number: 1, PreviousHash: 01, DataHash: 02", oi.String())
}
