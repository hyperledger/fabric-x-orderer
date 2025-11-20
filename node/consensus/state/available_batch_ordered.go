/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"github.com/hyperledger/fabric-x-orderer/common/types"
)

type AvailableBatchOrdered struct {
	AvailableBatch      *AvailableBatch
	OrderingInformation *OrderingInformation
}

func (abo *AvailableBatchOrdered) BatchAttestation() types.BatchAttestation {
	return abo.AvailableBatch
}

// OrderingInfo returns an opaque object that provides extra information on the order of the batch attestation and
// metadata to be used in the construction of the block.
func (abo *AvailableBatchOrdered) OrderingInfomation() *OrderingInformation {
	return abo.OrderingInformation
}
