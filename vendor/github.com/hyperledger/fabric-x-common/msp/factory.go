/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msp

import (
	"github.com/IBM/idemix"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	msppb "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/pkg/errors"
)

type MSPVersion int

const (
	MSPv1_0 = iota
	MSPv1_1
	MSPv1_3
	MSPv1_4_3
	MSPv3_0
)

// NewOpts represent
type NewOpts interface {
	// GetVersion returns the MSP's version to be instantiated
	GetVersion() MSPVersion
}

// NewBaseOpts is the default base type for all MSP instantiation Opts
type NewBaseOpts struct {
	Version MSPVersion
}

func (o *NewBaseOpts) GetVersion() MSPVersion {
	return o.Version
}

// BCCSPNewOpts contains the options to instantiate a new BCCSP-based (X509) MSP
type BCCSPNewOpts struct {
	NewBaseOpts
}

// IdemixNewOpts contains the options to instantiate a new Idemix-based MSP
type IdemixNewOpts struct {
	NewBaseOpts
}

// New create a new MSP instance depending on the passed Opts
func New(opts NewOpts, cryptoProvider bccsp.BCCSP) (MSP, error) {
	switch opts.(type) {
	case *BCCSPNewOpts:
		switch opts.GetVersion() {
		case MSPv1_0, MSPv1_1, MSPv1_3, MSPv1_4_3, MSPv3_0:
			return newBccspMsp(opts.GetVersion(), cryptoProvider)
		default:
			return nil, errors.Errorf("Invalid *BCCSPNewOpts. Version not recognized [%v]", opts.GetVersion())
		}
	case *IdemixNewOpts:
		switch opts.GetVersion() {
		case MSPv1_3, MSPv1_4_3:
			msp, err := idemix.NewIdemixMsp(MSPv1_3)
			if err != nil {
				return nil, err
			}

			return &idemixMSPWrapper{msp.(*idemix.Idemixmsp)}, nil
		case MSPv1_1:
			msp, err := idemix.NewIdemixMsp(MSPv1_1)
			if err != nil {
				return nil, err
			}

			return &idemixMSPWrapper{msp.(*idemix.Idemixmsp)}, nil
		default:
			return nil, errors.Errorf("Invalid *IdemixNewOpts. Version not recognized [%v]", opts.GetVersion())
		}
	default:
		return nil, errors.Errorf(
			"Invalid msp.NewOpts instance. It must be either *BCCSPNewOpts or *IdemixNewOpts. It was [%v]", opts,
		)
	}
}

// DirLoadParameters describes the parameters for loading an MSP directory.
type DirLoadParameters struct {
	MspDir  string
	MspName string
	CspConf *factory.FactoryOpts
}

// LoadLocalMspDir loads an MSP directory.
//
//nolint:ireturn,nolintlint // method may return any MSP implementation.
func LoadLocalMspDir(p DirLoadParameters) (MSP, error) {
	p = defaultParameters(p)
	conf, err := GetLocalMspConfig(p.MspDir, p.CspConf, p.MspName)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading local MSP configuration [%s]", p.MspDir)
	}
	return loadMSP(p.CspConf, conf)
}

// LoadVerifyingMspDir loads an MSP directory.
//
//nolint:ireturn,nolintlint // method may return any MSP implementation.
func LoadVerifyingMspDir(p DirLoadParameters) (MSP, error) {
	p = defaultParameters(p)
	conf, err := GetVerifyingMspConfig(p.MspDir, p.MspName, ProviderTypeToString(FABRIC))
	if err != nil {
		return nil, errors.Wrapf(err, "error loading verifing MSP configuration [%s]", p.MspDir)
	}
	return loadMSP(p.CspConf, conf)
}

//nolint:ireturn,nolintlint // method may return any MSP implementation.
func loadMSP(cspConfig *factory.FactoryOpts, conf *msppb.MSPConfig) (MSP, error) {
	csp, err := factory.GetBCCSPFromOpts(cspConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting BCCSP from config")
	}
	mspInst, err := New(Options[ProviderTypeToString(FABRIC)], csp)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating MSP instance")
	}
	err = mspInst.Setup(conf)
	return mspInst, errors.Wrapf(err, "error setting up MSP instance")
}

func defaultParameters(p DirLoadParameters) DirLoadParameters {
	if p.CspConf == nil {
		p.CspConf = factory.GetDefaultOpts()
	}
	if p.MspName == "" {
		p.MspName = "msp"
	}
	return p
}
