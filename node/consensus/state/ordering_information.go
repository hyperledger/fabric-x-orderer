/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"fmt"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
)

type OrderingInformation struct {
	CommonBlock *common.Block
	Signatures  []smartbft_types.Signature
	DecisionNum types.DecisionNum
	BatchIndex  int
	BatchCount  int
}

func (oi *OrderingInformation) String() string {
	if oi == nil {
		return "<nil>"
	}

	return fmt.Sprintf("DecisionNum: %d, BatchIndex: %d, BatchCount: %d; No. Sigs: %d, Common Block: %s", oi.DecisionNum, oi.BatchIndex, oi.BatchCount, len(oi.Signatures), types.CommonBlockToString(oi.CommonBlock))
}
