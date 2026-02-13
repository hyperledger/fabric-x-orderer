//go:build pkcs11
// +build pkcs11

/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msputils

import (
	"fmt"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-lib-go/bccsp/pkcs11"
	"github.com/hyperledger/fabric/msp"
)

func BuildLocalMSP(localMSPDir string, localMSPID string, bccspConfig *factory.FactoryOpts) msp.MSP {
	var factoryOpts *factory.FactoryOpts
	if bccspConfig != nil {
		if bccspConfig.Default != "" {
			factoryOpts = &factory.FactoryOpts{
				Default: bccspConfig.Default,
			}
		}

		if bccspConfig.PKCS11 != nil {
			if factoryOpts == nil {
				factoryOpts = &factory.FactoryOpts{Default: bccspConfig.Default}
			}
			// Create a new PKCS11Opts and copy all fields
			p11Opts := &pkcs11.PKCS11Opts{
				Security: bccspConfig.PKCS11.Security,
				Hash:     bccspConfig.PKCS11.Hash,
				Library:  bccspConfig.PKCS11.Library,
				Label:    bccspConfig.PKCS11.Label,
				Pin:      bccspConfig.PKCS11.Pin,
			}

			// Copy optional boolean fields
			p11Opts.SoftwareVerify = bccspConfig.PKCS11.SoftwareVerify
			p11Opts.Immutable = bccspConfig.PKCS11.Immutable

			// Copy optional string fields
			if bccspConfig.PKCS11.AltID != "" {
				p11Opts.AltID = bccspConfig.PKCS11.AltID
			}

			// Copy KeyIDs if present (for SKI to CKA_ID mapping)
			if len(bccspConfig.PKCS11.KeyIDs) > 0 {
				p11Opts.KeyIDs = make([]pkcs11.KeyIDMapping, len(bccspConfig.PKCS11.KeyIDs))
				for i, km := range bccspConfig.PKCS11.KeyIDs {
					p11Opts.KeyIDs[i] = pkcs11.KeyIDMapping{
						SKI: km.SKI,
						ID:  km.ID,
					}
				}
			}

			factoryOpts.PKCS11 = p11Opts
		}

		if bccspConfig.SW != nil {
			if factoryOpts == nil {
				factoryOpts = &factory.FactoryOpts{Default: bccspConfig.Default}
			}
			factoryOpts.SW = &factory.SwOpts{
				Security: bccspConfig.SW.Security,
				Hash:     bccspConfig.SW.Hash,
			}

			if bccspConfig.SW.FileKeystore != nil {
				factoryOpts.SW.FileKeystore = &factory.FileKeystoreOpts{
					KeyStorePath: bccspConfig.SW.FileKeystore.KeyStorePath,
				}
			}
		}
	}

	mspConfig, err := msp.GetLocalMspConfig(localMSPDir, factoryOpts, localMSPID)
	if err != nil {
		panic(fmt.Sprintf("Failed to get local msp config: %v", err))
	}

	typ := msp.ProviderTypeToString(msp.FABRIC)
	opts, found := msp.Options[typ]
	if !found {
		panic(fmt.Sprintf("MSP option for type %s is not found", typ))
	}

	localmsp, err := msp.New(opts, factory.GetDefault())
	if err != nil {
		panic(fmt.Sprintf("Failed to load local msp config: %v", err))
	}

	if err = localmsp.Setup(mspConfig); err != nil {
		panic(fmt.Sprintf("Failed to setup local msp with config: %v", err))
	}

	return localmsp
}
