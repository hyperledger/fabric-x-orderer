/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msp

import (
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/common/util"
)

var mspLogger = util.MustGetLogger("msp")

type mspManagerImpl struct {
	// map that contains all MSPs that we have setup or otherwise added
	mspsMap map[string]MSP

	// map that maps MSPs by their provider types
	mspsByProviders map[ProviderType][]MSP

	// error that might have occurred at startup
	up bool
}

// NewMSPManager returns a new MSP manager instance;
// note that this instance is not initialized until
// the Setup method is called
func NewMSPManager() MSPManager {
	return &mspManagerImpl{}
}

// Setup initializes the internal data structures of this manager and creates MSPs
func (mgr *mspManagerImpl) Setup(msps []MSP) error {
	if mgr.up {
		mspLogger.Infof("MSP manager already up")
		return nil
	}

	mspLogger.Debugf("Setting up the MSP manager (%d msps)", len(msps))

	// create the map that assigns MSP IDs to their manager instance - once
	mgr.mspsMap = make(map[string]MSP)

	// create the map that sorts MSPs by their provider types
	mgr.mspsByProviders = make(map[ProviderType][]MSP)

	for _, msp := range msps {
		// add the MSP to the map of active MSPs
		mspID, err := msp.GetIdentifier()
		if err != nil {
			return errors.WithMessage(err, "could not extract msp identifier")
		}
		mgr.mspsMap[mspID] = msp
		providerType := msp.GetType()
		mgr.mspsByProviders[providerType] = append(mgr.mspsByProviders[providerType], msp)
	}

	mgr.up = true

	mspLogger.Debugf("MSP manager setup complete, setup %d msps", len(msps))

	return nil
}

// GetMSPs returns the MSPs that are managed by this manager
func (mgr *mspManagerImpl) GetMSPs() (map[string]MSP, error) {
	return mgr.mspsMap, nil
}

// DeserializeIdentity returns an identity given its serialized version supplied as argument
func (mgr *mspManagerImpl) DeserializeIdentity(sID *applicationpb.Identity) (Identity, error) { //nolint:ireturn
	if !mgr.up {
		return nil, errors.New("channel doesn't exist")
	}

	// we can now attempt to obtain the MSP
	msp := mgr.mspsMap[sID.MspId]
	if msp == nil {
		return nil, errors.Errorf("MSP %s is not defined on channel", sID.MspId)
	}

	switch t := msp.(type) {
	case *bccspmsp:
		switch sID.Creator.(type) {
		case *applicationpb.Identity_Certificate:
			return t.deserializeIdentityInternal(sID.GetCertificate())
		case *applicationpb.Identity_CertificateId:
			return msp.GetKnownDeserializedIdentity(
				IdentityIdentifier{Mspid: sID.MspId, Id: sID.GetCertificateId()}), nil
		default:
			return nil, errors.New("unknown creator type")
		}
	case *idemixMSPWrapper:
		return t.deserializeIdentityInternal(sID.GetCertificate())
	default:
		return t.DeserializeIdentity(sID)
	}
}

func (mgr *mspManagerImpl) IsWellFormed(identity *applicationpb.Identity) error {
	// Iterate over all the MSPs by their providers, and find at least 1 MSP that can attest
	// that this identity is well formed
	for _, mspList := range mgr.mspsByProviders {
		// We are guaranteed to have at least 1 MSP in each list from the initialization at Setup()
		msp := mspList[0]
		if err := msp.IsWellFormed(identity); err == nil {
			return nil
		}
	}
	return errors.New("no MSP provider recognizes the identity")
}

//nolint:ireturn //Identity is an interface.
func (mgr *mspManagerImpl) GetKnownDeserializedIdentity(i IdentityIdentifier) Identity {
	m := mgr.mspsMap[i.Mspid]
	if m == nil {
		return nil
	}

	return m.GetKnownDeserializedIdentity(i)
}
