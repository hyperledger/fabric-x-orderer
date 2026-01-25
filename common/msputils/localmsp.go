/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msputils

import (
	"fmt"
	"path/filepath"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric/msp"
)

func BuildLocalMSP(localMSPDir string, localMSPID string, bccspConfig *factory.FactoryOpts) msp.MSP {
	cspOpts := &factory.FactoryOpts{
		Default: "SW",
		SW: &factory.SwOpts{
			Security: 256,
			Hash:     "SHA2",
			FileKeystore: &factory.FileKeystoreOpts{
				KeyStorePath: filepath.Join(localMSPDir, "keystore"),
			},
		},
	}

	mspConfig, err := msp.GetLocalMspConfig(localMSPDir, cspOpts, localMSPID)
	if err != nil {
		panic(fmt.Sprintf("Failed to get local msp config: %v", err))
	}

	typ := msp.ProviderTypeToString(msp.FABRIC)
	opts, found := msp.Options[typ]
	if !found {
		panic(fmt.Sprintf("MSP option for type %s is not found", typ))
	}

	csp, err := factory.GetBCCSPFromOpts(cspOpts)
	if err != nil {
		panic(fmt.Sprintf("Failed to get bccsp from options: %v", err))
	}

	localmsp, err := msp.New(opts, csp)
	if err != nil {
		panic(fmt.Sprintf("Failed to load local msp config: %v", err))
	}

	if err = localmsp.Setup(mspConfig); err != nil {
		panic(fmt.Sprintf("Failed to setup local msp with config: %v", err))
	}

	return localmsp
}
