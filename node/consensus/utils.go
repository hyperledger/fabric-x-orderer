/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	arma_types "github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
)

func toBeSignedBAF(baf core.BatchAttestationFragment) []byte {
	simpleBAF, ok := baf.(*arma_types.SimpleBatchAttestationFragment)
	if !ok {
		return nil
	}
	return simpleBAF.ToBeSigned()
}
