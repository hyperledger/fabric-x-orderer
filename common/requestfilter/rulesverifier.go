/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

// Verifier is a struct that holds a slice of rules, used to verify a request.
type RulesVerifier struct {
	rules         []Rule
	structureRule StructureRule
	lock          sync.RWMutex
}

// NewRulesVerifier creates a RulesVerifier using the provided ordered list of Rules.
func NewRulesVerifier(rules []Rule) *RulesVerifier {
	return &RulesVerifier{
		rules: rules,
	}
}

func (rv *RulesVerifier) AddRule(rule Rule) {
	rv.lock.Lock()
	defer rv.lock.Unlock()

	rv.rules = append(rv.rules, rule)
}

// AddStructureRule add one structure rule to the verifier. If a rule is already set, it will replace it.
func (rv *RulesVerifier) AddStructureRule(sr StructureRule) {
	rv.lock.Lock()
	defer rv.lock.Unlock()
	rv.structureRule = sr
}

// Verify checks the request against the rules in order. It returns nil if all rules pass, or an error if any rule fails.
func (rv *RulesVerifier) Verify(request *comm.Request) error {
	rv.lock.RLock()
	defer rv.lock.RUnlock()
	for _, rule := range rv.rules {
		if err := rule.Verify(request); err != nil {
			return err
		}
	}

	return nil
}

// VerifyStructureAndClassify checks the structure of the request with the structure rule, and also returns the request's type.
func (rv *RulesVerifier) VerifyStructureAndClassify(request *comm.Request) (common.HeaderType, error) {
	rv.lock.RLock()
	defer rv.lock.RUnlock()
	var reqType common.HeaderType
	if rv.structureRule == nil {
		return reqType, fmt.Errorf("sturcture rule is not set in verifier")
	}
	return rv.structureRule.VerifyAndClassify(request)
}

// Update the rules in the RulesVerifier with the FilterConfig
func (rv *RulesVerifier) Update(config FilterConfig) error {
	rv.lock.Lock()
	defer rv.lock.Unlock()

	for _, rule := range rv.rules {
		if err := rule.Update(config); err != nil {
			return err
		}
	}

	if rv.structureRule != nil {
		if err := rv.structureRule.Update(config); err != nil {
			return err
		}
	}

	return nil
}
