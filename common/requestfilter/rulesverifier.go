/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

// Verifier is a struct that holds a slice of rules, used to verify a request.
type RulesVerifier struct {
	rules          []Rule
	structureRules []StructureRule
	lock           sync.RWMutex
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

func (rv *RulesVerifier) AddStructureRule(sr StructureRule) {
	rv.lock.Lock()
	defer rv.lock.Unlock()

	rv.structureRules = append(rv.structureRules, sr)
}

// Verify checks the request against the rules in order. It returns nil if all rules pass, or an error if any rule fails.
// In addition, if there is a structure rule the return the type of the request, verify will also return it.
func (rv *RulesVerifier) Verify(request *comm.Request) (string, error) {
	rv.lock.RLock()
	defer rv.lock.RUnlock()
	reqType := ""
	for _, rule := range rv.rules {
		if err := rule.Verify(request); err != nil {
			return reqType, err
		}
	}

	for _, rule := range rv.structureRules {
		t, err := rule.Verify(request)
		if err != nil {
			return reqType, err
		}
		if reqType == "" {
			reqType = t
		} else if reqType != t {
			return "", fmt.Errorf("bad request type: received different type from different rules")
		}
	}
	return reqType, nil
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
	return nil
}
