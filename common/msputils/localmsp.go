/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msputils

import (
	"fmt"
	"path/filepath"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-x-common/msp"
)

func BuildLocalMSP(localMSPDir string, localMSPID string, factoryOpts *factory.FactoryOpts) msp.MSP {
	mspConfig, err := msp.GetLocalMspConfig(localMSPDir, factoryOpts, localMSPID)
	if err != nil {
		panic(fmt.Sprintf("Failed to get local msp config: %v", err))
	}

	bccspConfig := msp.SetupBCCSPKeystoreConfig(
		factoryOpts,
		filepath.Join(localMSPDir, "keystore"),
	)

	cryptoProvider, err := factory.GetBCCSPFromOpts(bccspConfig)
	if err != nil {
		panic(fmt.Sprintf("Failed to create BCCSP from config: %v", err))
	}

	typ := msp.ProviderTypeToString(msp.FABRIC)
	opts, found := msp.Options[typ]
	if !found {
		panic(fmt.Sprintf("MSP option for type %s is not found", typ))
	}

	localmsp, err := msp.New(opts, cryptoProvider)
	if err != nil {
		panic(fmt.Sprintf("Failed to load local msp config: %v", err))
	}

	if err = localmsp.Setup(mspConfig); err != nil {
		panic(fmt.Sprintf("Failed to setup local msp with config: %v", err))
	}

	return localmsp
}
