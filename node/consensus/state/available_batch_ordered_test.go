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
	oi.BlockHeader = &BlockHeader{
		Number:   3,
		PrevHash: []byte{1, 2, 3, 4},
		Digest:   []byte{0xa, 0xb, 0xc, 0xd},
	}
	assert.Equal(t, "DecisionNum: 10, BatchIndex: 2, BatchCount: 3; No. Sigs: 3, BlockHeader: Number: 3, PrevHash: 01020304, Digest: 0a0b0c0d, Common Block: <nil>", oi.String())

	var abo AvailableBatchOrdered
	abo.AvailableBatch = &ab
	abo.OrderingInformation = &oi

	assert.Equal(t,
		"{AvailableBatch:Sh,Pr,Sq,Dg: <666,42,100,0000000000000000000000000000000000000000000000000000000000000000> OrderingInformation:DecisionNum: 10, BatchIndex: 2, BatchCount: 3; No. Sigs: 3, BlockHeader: Number: 3, PrevHash: 01020304, Digest: 0a0b0c0d, Common Block: <nil>}",
		fmt.Sprintf("%+v", abo))

	oi.CommonBlock = &common.Block{Header: &common.BlockHeader{Number: 1}}
	assert.Equal(t, "DecisionNum: 10, BatchIndex: 2, BatchCount: 3; No. Sigs: 3, BlockHeader: Number: 3, PrevHash: 01020304, Digest: 0a0b0c0d, Common Block: header:{number:1}", oi.String())
}
