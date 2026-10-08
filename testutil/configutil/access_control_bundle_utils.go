/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configutil

import (
	"bytes"
	"errors"

	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
)

// AdmitSigners makes every policy of the bundle admit a request signed with one of the given
// certificates, or every request when none is given. It stands in for the policies of the
// configuration in a test that runs no MSP.
func AdmitSigners(bundle *mocks.FakeConfigResources, signCerts ...[]byte) {
	policy := &policyMocks.FakePolicyEvaluator{}
	policy.EvaluateSignedDataStub = func(signedData []*protoutil.SignedData) error {
		if len(signCerts) == 0 {
			return nil
		}

		for _, data := range signedData {
			for _, signCert := range signCerts {
				if bytes.Equal(data.Identity.GetCertificate(), signCert) {
					return nil
				}
			}
		}

		return errors.New("the request is signed with no certificate the policy admits")
	}

	policyManager := &policyMocks.FakePolicyManager{}
	policyManager.GetPolicyReturns(policy, true)
	bundle.PolicyManagerReturns(policyManager)
}
