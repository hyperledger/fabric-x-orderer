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

// ==== TODO remove the noopVerifierCreator and use the real verifier in synchronizer after the real verifier is implemented. The noopVerifierCreator and noopBlockVerifier can be used in tests or when block verification is not needed.

// noopVerifierCreator creates a block verifier that does not actually verify blocks, which can be used in tests or when block verification is not needed.
type noopVerifierCreator struct{}

func (*noopVerifierCreator) CreateBlockVerifier(
	configBlock *common.Block,
	lastBlock *common.Block,
	cryptoProvider bccsp.BCCSP,
	lg *flogging.FabricLogger,
) (deliverclient.CloneableUpdatableBlockVerifier, error) {
	return &noopBlockVerifier{}, nil
}

// noopBlockVerifier is a block verifier that does not actually verify blocks, which can be used in tests or when block verification is not needed.
type noopBlockVerifier struct{}

// VerifyBlock checks block integrity and its relation to the chain, and verifies the signatures.
func (*noopBlockVerifier) VerifyBlock(block *common.Block) error {
	// TODO
	return nil
}

func (*noopBlockVerifier) VerifyBlockAttestation(block *common.Block) error {
	// TODO
	return nil
}

func (*noopBlockVerifier) UpdateConfig(configBlock *common.Block) error {
	// TODO
	return nil
}

func (*noopBlockVerifier) UpdateBlockHeader(block *common.Block) {
	// TODO
}

func (*noopBlockVerifier) Clone() deliverclient.CloneableUpdatableBlockVerifier {
	return &noopBlockVerifier{}
}
