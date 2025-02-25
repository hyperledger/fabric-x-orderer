package consensus

import (
	arma_types "arma/common/types"
	"arma/core"
)

func toBeSignedBAF(baf core.BatchAttestationFragment) []byte {
	simpleBAF, ok := baf.(*arma_types.SimpleBatchAttestationFragment)
	if !ok {
		return nil
	}
	return simpleBAF.ToBeSigned()
}
