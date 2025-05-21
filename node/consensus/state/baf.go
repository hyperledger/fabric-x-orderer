/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	arma_types "github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
)

type BAFDeserializer struct{}

func (bafd *BAFDeserializer) Deserialize(bytes []byte) (core.BatchAttestationFragment, error) {
	var baf arma_types.SimpleBatchAttestationFragment
	if err := baf.Deserialize(bytes); err != nil {
		return nil, err
	}
	return &baf, nil
}
