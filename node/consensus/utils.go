/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"fmt"

	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
)

func toBeSignedBAF(baf arma_types.BatchAttestationFragment) []byte {
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
