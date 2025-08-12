/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
)

type BAFDeserializer interface {
	Deserialize([]byte) (arma_types.BatchAttestationFragment, error)
}

type BAFDeserialize struct{}

func (bafd *BAFDeserialize) Deserialize(bytes []byte) (arma_types.BatchAttestationFragment, error) {
	var baf arma_types.SimpleBatchAttestationFragment
	if err := baf.Deserialize(bytes); err != nil {
		return nil, err
	}
	return &baf, nil
}
