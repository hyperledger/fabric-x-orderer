/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configutil

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"

	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/pkg/errors"
)

// AdmitSigners makes every policy of the bundle admit a request signed with one of the given
// certificates, or every request when none is given. It stands in for the policies of the
// configuration in a test that runs no MSP: a request must carry one of the certificates, and its
// signature must verify against the key of that certificate.
func AdmitSigners(bundle *mocks.FakeConfigResources, signCerts ...[]byte) {
	policy := &policyMocks.FakePolicyEvaluator{}
	policy.EvaluateSignedDataStub = func(signedData []*protoutil.SignedData) error {
		if len(signCerts) == 0 {
			return nil
		}

		for _, data := range signedData {
			if signedByOneOf(data, signCerts) {
				return nil
			}
		}

		return errors.New("the request is signed with no certificate the policy admits")
	}

	policyManager := &policyMocks.FakePolicyManager{}
	policyManager.GetPolicyReturns(policy, true)
	bundle.PolicyManagerReturns(policyManager)
}

// signedByOneOf reports whether the request carries one of the certificates, and its signature
// verifies against the key of that certificate.
func signedByOneOf(data *protoutil.SignedData, signCerts [][]byte) bool {
	certPEM := data.Identity.GetCertificate()
	for _, signCert := range signCerts {
		if !bytes.Equal(certPEM, signCert) {
			continue
		}

		cert, err := utils.Parsex509Cert(signCert)
		if err != nil {
			return false
		}

		publicKey, ok := cert.PublicKey.(*ecdsa.PublicKey)
		if !ok {
			return false
		}

		digest := sha256.Sum256(data.Data)
		return ecdsa.VerifyASN1(publicKey, digest[:], data.Signature)
	}

	return false
}
