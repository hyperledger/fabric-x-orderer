/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"fmt"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
)

func GetConfigSequenceFromBlock(logger *flogging.FabricLogger, configBlock *common.Block, bccsp bccsp.BCCSP) (uint32, error) {
	env, err := protoutil.ExtractEnvelope(configBlock, 0)
	if err != nil {
		logger.Warningf("failed to extract envelope from new config block, err: %v", err)
		return 0, fmt.Errorf("failed to extract envelope from new config block, err: %v", err)
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(env, bccsp)
	if err != nil {
		logger.Warnf("failed to extract bundle from new config block, err: %v", err)
		return 0, fmt.Errorf("failed to extract bundle from new config block, err: %v", err)

	}

	return uint32(bundle.ConfigtxValidator().Sequence()), nil
}
