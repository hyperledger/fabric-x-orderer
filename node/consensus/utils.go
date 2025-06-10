/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"fmt"

	arma_types "github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node/consensus/state"
)

func toBeSignedBAF(baf core.BatchAttestationFragment) []byte {
	simpleBAF, ok := baf.(*arma_types.SimpleBatchAttestationFragment)
	if !ok {
		return nil
	}
	return simpleBAF.ToBeSigned()
}

func printEvent(event []byte) string {
	var ce core.ControlEvent
	bafd := &state.BAFDeserializer{}
	if err := ce.FromBytes(event, bafd.Deserialize); err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return ce.String()
}
