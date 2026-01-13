/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msp

import "github.com/hyperledger/fabric-x-common/msp"

//go:generate counterfeiter -o mock/msp_manager.go --fake-name MSPManager . MSPManager
type MSPManager interface {
	// IdentityDeserializer interface needs to be implemented by MSPManager
	msp.IdentityDeserializer

	// Setup the MSP manager instance according to configuration information
	Setup(msps []msp.MSP) error

	// GetMSPs Provides a list of Membership Service providers
	GetMSPs() (map[string]msp.MSP, error)
}

//go:generate counterfeiter -o mock/msp.go --fake-name MSP . MSP
type MSP interface {
	msp.MSP
}
