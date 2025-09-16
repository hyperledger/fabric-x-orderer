/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter

import (
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

//go:generate counterfeiter -o ./mocks/rule.go . Rule
type Rule interface {
	Verify(request *comm.Request) error
	Update(config FilterConfig) error
}

// FilterConfig is an interface that gives the necessary information to verify rules.
//
//go:generate counterfeiter -o ./mocks/filter_config.go . FilterConfig
type FilterConfig interface {
	GetRequestMaxBytes() uint64
	GetClientSignatureVerificationRequired() bool
	GetChannelID() string
	GetPolicyManager() policies.Manager
}

// AcceptRule - always returns nil as a result for Verify
type AcceptRule struct{}

func (a AcceptRule) Verify(request *comm.Request) error {
	return nil
}

func (a AcceptRule) Update(config FilterConfig) error {
	return nil
}
