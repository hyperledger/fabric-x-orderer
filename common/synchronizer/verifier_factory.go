/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package synchronizer

import (
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient"
)

//go:generate counterfeiter -o mocks/verifier_factory.go --fake-name VerifierFactory . VerifierFactory
type VerifierFactory interface {
	CreateBlockVerifier(
		configBlock *common.Block,
		lastBlock *common.Block,
		cryptoProvider bccsp.BCCSP,
		lg *flogging.FabricLogger,
	) (deliverclient.CloneableUpdatableBlockVerifier, error)
}
