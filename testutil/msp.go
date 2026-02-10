/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"path/filepath"

	"github.com/hyperledger/fabric-lib-go/bccsp/sw"
	"github.com/hyperledger/fabric-x-common/msp"
)

func BuildTestLocalMSP(localMSPDir string, orgName string) (msp.MSP, msp.SigningIdentity, error) {
	conf, err := msp.GetLocalMspConfig(localMSPDir, nil, orgName)
	if err != nil {
		return nil, nil, err
	}

	ks, err := sw.NewFileBasedKeyStore(nil, filepath.Join(localMSPDir, "keystore"), true)
	if err != nil {
		return nil, nil, err
	}
	cryptoProvider, err := sw.NewDefaultSecurityLevelWithKeystore(ks)
	if err != nil {
		return nil, nil, err
	}
	localMSP, err := msp.NewBccspMspWithKeyStore(msp.MSPv3_0, ks, cryptoProvider)
	if err != nil {
		return nil, nil, err
	}

	err = localMSP.Setup(conf)
	if err != nil {
		return nil, nil, err
	}

	signer, err := localMSP.GetDefaultSigningIdentity()
	if err != nil {
		return nil, nil, err
	}
	return localMSP, signer, nil
}
