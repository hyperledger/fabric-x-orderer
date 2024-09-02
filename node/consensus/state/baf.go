package state

import (
	arma_types "arma/common/types"
	"arma/core"
)

type BAFDeserializer struct{}

func (bafd *BAFDeserializer) Deserialize(bytes []byte) (core.BatchAttestationFragment, error) {
	var baf arma_types.SimpleBatchAttestationFragment
	if err := baf.Deserialize(bytes); err != nil {
		return nil, err
	}
	return &baf, nil
}
