/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"fmt"

	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

type Rule interface {
	Verify(request *comm.Request) error
}

// Verifier is a struct that holds a slice of rules, used to verify a request.
type Verifier struct {
	rules []Rule
}

// NewVerifier creates a Verifier using the provided ordered list of Rules.
func NewVerifier(rules []Rule) *Verifier {
	return &Verifier{
		rules: rules,
	}
}

// Verify checks the request against the rules in order. It returns nil if all rules pass, or an error if any rule fails.
func (vr *Verifier) Verify(request *comm.Request) error {
	for _, rule := range vr.rules {
		if err := rule.Verify(request); err != nil {
			return err
		}
	}
	return nil
}

func (vr *Verifier) AddRule(rule Rule) {
	vr.rules = append(vr.rules, rule)
}

// PayloadNotEmptyRule - checks that the payload in the request is not nil.
var PayloadNotEmptyRule = Rule(payloadNotEmptyRule{})

type payloadNotEmptyRule struct{}

func (a payloadNotEmptyRule) Verify(request *comm.Request) error {
	if request.Payload == nil {
		return fmt.Errorf("empty payload field")
	}
	return nil
}

// AcceptRule - always returns nil as a result for Apply
var AcceptRule = Rule(acceptRule{})

type acceptRule struct{}

func (a acceptRule) Verify(request *comm.Request) error {
	return nil
}

// Request Max Size rule

// MaxSizeRuleSupport is an interface that gives the necessary information to verify the size of a request.
type MaxSizeRuleSupport interface {
	GetMaxSizeBytes() (uint64, error)
}

type MaxSizeRule struct {
	support MaxSizeRuleSupport
}

func NewMaxSizeRule(support MaxSizeRuleSupport) *MaxSizeRule {
	return &MaxSizeRule{support: support}
}

// Verfiy checks that the size of the request does not exceeds the maximal size in bytes.
func (ms *MaxSizeRule) Verify(request *comm.Request) error {
	maxSize, ok := ms.support.GetMaxSizeBytes()
	if ok != nil {
		return fmt.Errorf("router node config not found")
	}
	requestSize := requestSizeInBytes(request)
	if requestSize > maxSize {
		return fmt.Errorf("the request's size exceeds the maximum size")
	}
	return nil
}

// requestSizeInBytes return the (approximated) size in bytes of a request
func requestSizeInBytes(request *comm.Request) uint64 {
	return uint64(len(request.Payload) + len(request.Signature))
}

// Signature verification rule
// ** Not Implemented **

type SigVerifier struct {
	support SigVerifySupport
}

type SigVerifySupport interface {
	// add a proper get function if needed
}

func NewSigVerifier(support SigVerifySupport) *SigVerifier {
	return &SigVerifier{support: support}
}

func (sf *SigVerifier) Verify(request *comm.Request) error {
	// add signature verification logic
	return nil
}
