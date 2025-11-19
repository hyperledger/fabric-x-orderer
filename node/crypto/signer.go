/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package crypto

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/asn1"
	"fmt"
	"math/big"

	"github.com/hyperledger/fabric-lib-go/bccsp/utils"
	"github.com/hyperledger/fabric-x-common/common/util"
)

type ECDSASigner ecdsa.PrivateKey

func (s ECDSASigner) Sign(message []byte) ([]byte, error) {
	digest := util.ComputeSHA256(message)
	sk := (ecdsa.PrivateKey)(s)
	return signECDSA(&sk, digest)
}

func (s ECDSASigner) Serialize() ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func signECDSA(k *ecdsa.PrivateKey, digest []byte) (signature []byte, err error) {
	r, s, err := ecdsa.Sign(rand.Reader, k, digest)
	if err != nil {
		return nil, err
	}

	s, err = utils.ToLowS(&k.PublicKey, s)
	if err != nil {
		return nil, err
	}

	return marshalECDSASignature(r, s)
}

func marshalECDSASignature(r, s *big.Int) ([]byte, error) {
	return asn1.Marshal(ECDSASignature{r, s})
}

type ECDSASignature struct {
	R, S *big.Int
}
