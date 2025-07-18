/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package csp

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// This code originates from hyperledger fabric project. Source: https://github.com/hyperledger/fabric/blob/main/internal/cryptogen/csp/csp_internal_test.go

func TestToLowS(t *testing.T) {
	curve := elliptic.P256()
	halfOrder := new(big.Int).Div(curve.Params().N, big.NewInt(2))

	for _, test := range []struct {
		name        string
		sig         ECDSASignature
		expectedSig ECDSASignature
	}{
		{
			name: "HighS",
			sig: ECDSASignature{
				R: big.NewInt(1),
				// set S to halfOrder + 1
				S: new(big.Int).Add(halfOrder, big.NewInt(1)),
			},
			// expected signature should be (sig.R, -sig.S mod N)
			expectedSig: ECDSASignature{
				R: big.NewInt(1),
				S: new(big.Int).Mod(new(big.Int).Neg(new(big.Int).Add(halfOrder, big.NewInt(1))), curve.Params().N),
			},
		},
		{
			name: "LowS",
			sig: ECDSASignature{
				R: big.NewInt(1),
				// set S to halfOrder - 1
				S: new(big.Int).Sub(halfOrder, big.NewInt(1)),
			},
			// expected signature should be sig
			expectedSig: ECDSASignature{
				R: big.NewInt(1),
				S: new(big.Int).Sub(halfOrder, big.NewInt(1)),
			},
		},
		{
			name: "HalfOrder",
			sig: ECDSASignature{
				R: big.NewInt(1),
				// set S to halfOrder
				S: halfOrder,
			},
			// expected signature should be sig
			expectedSig: ECDSASignature{
				R: big.NewInt(1),
				S: halfOrder,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			curve := elliptic.P256()
			key := ecdsa.PublicKey{
				Curve: curve,
			}
			require.Equal(t, test.expectedSig, toLowS(key, test.sig))
		})
	}
}
