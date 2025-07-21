/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"fmt"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

type Rule interface {
	Verify(request *comm.Request) error
}

// Similiarly to fabric, we define a verifier as a slice of rules, which can be verified one after the other.
type Verifier struct {
	rules []Rule
}

// NewRuleSet creates a new RuleSet with the given ordered list of Rules
func NewVerifier(rules []Rule) *Verifier {
	return &Verifier{
		rules: rules,
	}
}

// Verify verifies the rules given for this set in order, returning nil on valid or err on invalid
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

// Here is a list of possible filters.

// Not empty rule - checks that the payload in the request is not nil.
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

// Signature verification rule

type SigVerifierSupport interface {
	getSignedTransactionService() (*armageddon.SignedTransactionService, error)
}

type SigVerifier struct {
	support SigVerifierSupport
}

func NewSigVerifier(support SigVerifierSupport) *SigVerifier {
	return &SigVerifier{support: support}
}

func (sf *SigVerifier) Verify(request *comm.Request) error {
	service, ok := sf.support.getSignedTransactionService()
	if ok != nil {
		return fmt.Errorf("SignedTransactionServie not found")
	}
	id, _ := service.GetRandomTransactionIndex()
	isValid := service.VerifyTransaction(id)
	if !isValid {
		return fmt.Errorf("failed vrifying signature")
	}
	// fmt.Printf("Succefuly verified dignature of transaction id %d \n", id)
	return nil
}

// MaxSizeRule - checks that the size of the request does not exceeds the maximal size in bytes.
type MaxSizeRuleSupport interface {
	RouterNodeConfig() (*nodeconfig.RouterNodeConfig, error)
}

type MaxSizeRule struct {
	support MaxSizeRuleSupport
}

func NewMaxSizeRule(support MaxSizeRuleSupport) *MaxSizeRule {
	return &MaxSizeRule{support: support}
}

func (ms *MaxSizeRule) Verify(request *comm.Request) error {
	config, ok := ms.support.RouterNodeConfig()
	if ok != nil {
		return fmt.Errorf("router node config not found")
	}
	requestSize := requestSizeInBytes(request)
	maxSize := config.RequestMaxBytes
	if requestSize > maxSize {
		return fmt.Errorf("the request's size exceeds the maximum size")
	}
	return nil
}

// requestSizeInBytes return the (approximated) size in bytes of a request
func requestSizeInBytes(request *comm.Request) uint64 {
	return uint64(len(request.Payload) + len(request.Signature))
}
