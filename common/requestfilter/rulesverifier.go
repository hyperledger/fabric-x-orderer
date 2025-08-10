/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package requestfilter

import (
	"sync"

	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

// Verifier is a struct that holds a slice of rules, used to verify a request.
type RulesVerifier struct {
	rules []Rule
	lock  sync.RWMutex
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
