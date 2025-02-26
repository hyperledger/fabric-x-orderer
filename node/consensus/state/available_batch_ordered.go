package state

import (
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
)

type OrderingInformation struct {
	*BlockHeader
	Signatures  []smartbft_types.Signature
	DecisionNum types.DecisionNum
	BatchIndex  int
	BatchCount  int
}

type AvailableBatchOrdered struct {
	AvailableBatch      *AvailableBatch
	OrderingInformation *OrderingInformation
}

func (abo *AvailableBatchOrdered) BatchAttestation() core.BatchAttestation {
	return abo.AvailableBatch
}

// OrderingInfo returns an opaque object that provides extra information on the order of the batch attestation and
// metadata to be used in the construction of the block.
func (abo *AvailableBatchOrdered) OrderingInfo() interface{} {
	return abo.OrderingInformation
}
