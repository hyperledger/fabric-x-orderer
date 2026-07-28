/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
)

func GetConfigSequenceFromBlock(configBlock *common.Block, bccsp bccsp.BCCSP) (uint32, error) {
	env, err := protoutil.ExtractEnvelope(configBlock, 0)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to extract envelope from new config block")
	}
	bundle, err := channelconfig.NewBundleFromEnvelope(env, bccsp)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to extract bundle from new config block")
	}

	return uint32(bundle.ConfigtxValidator().Sequence()), nil
}
