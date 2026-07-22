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
	"github.com/pkg/errors"
)

type AssemblerBlockVerifierCreator struct{}

func (*AssemblerBlockVerifierCreator) CreateBlockVerifier(
	configBlock *common.Block,
	lastBlock *common.Block,
	cryptoProvider bccsp.BCCSP,
	lg *flogging.FabricLogger,
) (deliverclient.CloneableUpdatableBlockVerifier, error) {
	verifier, err := deliverclient.NewBlockVerificationAssistant(configBlock, lastBlock, cryptoProvider, lg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create assembler block verifier")
	}
	return verifier, nil
}
