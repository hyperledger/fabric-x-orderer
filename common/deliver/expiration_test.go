/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package deliver_test

import (
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/common/crypto/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/testutil/fakes"
	"github.com/hyperledger/fabric-x-orderer/common/msputils/mock"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// Scenario:
//  1. Build the expiry check over a set of identities that resolve a request identity to an identity
//     with a known expiry.
//  2. Check an identity that carries its certificate, and expect the expiry of the identity it
//     resolves to.
//  3. Check an identity sent by certificate ID, and expect the expiry of the identity it resolves to.
//  4. Check an identity that does not resolve, and expect an error.
//  5. Build the expiry check over no identities, check an identity that carries its certificate, and
//     expect the expiry of that certificate.
func TestNewExpiresAtFuncResolvesTheRequestIdentity(t *testing.T) {
	knownExpiry := time.Now().Add(-time.Hour).Truncate(time.Second)
	knownIdentity := &fakes.SigningIdentity{}
	knownIdentity.ExpiresAtReturns(knownExpiry)

	identities := &mock.MSPManager{}
	identities.DeserializeIdentityReturns(knownIdentity, nil)
	expiresAt := deliver.NewExpiresAtFunc(identities)

	t.Run("an identity that carries its certificate", func(t *testing.T) {
		identity := msppb.NewIdentity("org", []byte("certificate"))

		expiry, err := expiresAt(identity)
		require.NoError(t, err)
		require.Equal(t, knownExpiry, expiry)
		require.Equal(t, identity, identities.DeserializeIdentityArgsForCall(identities.DeserializeIdentityCallCount()-1))
	})

	t.Run("an identity sent by certificate ID", func(t *testing.T) {
		identity := msppb.NewIdentityWithIDOfCert("org", "known-cert-id")

		expiry, err := expiresAt(identity)
		require.NoError(t, err)
		require.Equal(t, knownExpiry, expiry)
		require.Equal(t, identity, identities.DeserializeIdentityArgsForCall(identities.DeserializeIdentityCallCount()-1))
	})

	t.Run("an identity that does not resolve", func(t *testing.T) {
		unknown := &mock.MSPManager{}
		unknown.DeserializeIdentityReturns(nil, errors.New("identity is unknown"))

		_, err := deliver.NewExpiresAtFunc(unknown)(msppb.NewIdentityWithIDOfCert("org", "unknown-cert-id"))
		require.ErrorContains(t, err, "identity is unknown")
	})

	t.Run("no identities", func(t *testing.T) {
		ca, err := tlsgen.NewCA()
		require.NoError(t, err)
		cert, err := utils.Parsex509Cert(ca.CertBytes())
		require.NoError(t, err)

		expiry, err := deliver.NewExpiresAtFunc(nil)(msppb.NewIdentity("org", ca.CertBytes()))
		require.NoError(t, err)
		require.Equal(t, cert.NotAfter, expiry)
	})
}
