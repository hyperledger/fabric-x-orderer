/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package signutil

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

type TestSigner struct {
	ecdsaSigner crypto.ECDSASigner
	creator     *msp.SerializedIdentity
}

func NewTestSigner(keyPath, certPath, mspID string) (*TestSigner, error) {
	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key, err: %v", err)
	}

	// Create a ECDSA Singer
	privateKey, err := tx.CreateECDSAPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create ECDSA Signer, err: %v", err)
	}

	certBytes, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read certificate, err: %v", err)
	}
	sid := &msp.SerializedIdentity{
		Mspid:   mspID,
		IdBytes: certBytes,
	}

	return &TestSigner{ecdsaSigner: crypto.ECDSASigner(*privateKey), creator: sid}, nil
}

func (s TestSigner) Sign(message []byte) ([]byte, error) {
	return s.ecdsaSigner.Sign(message)
}

func (s TestSigner) Serialize() ([]byte, error) {
	return protoutil.MarshalOrPanic(s.creator), nil
}

func CreateTestSigner(t *testing.T, mspID, dir string) *TestSigner {
	keyPath := filepath.Join(dir, "crypto", "ordererOrganizations", mspID, "users", "user", "msp", "keystore", "priv_sk")
	certPath := filepath.Join(dir, "crypto", "ordererOrganizations", mspID, "users", "user", "msp", "signcerts", "sign-cert.pem")
	Signer, err := NewTestSigner(keyPath, certPath, mspID)
	require.NotNil(t, Signer)
	require.NoError(t, err)
	return Signer
}

func CreateSignerForUser(userMspDir string) protoutil.Signer {
	mspID := getMspIDfromDir(userMspDir)
	keyPath := filepath.Join(userMspDir, "keystore", "priv_sk")
	certPath := filepath.Join(userMspDir, "signcerts", "sign-cert.pem")
	signer, err := NewTestSigner(keyPath, certPath, mspID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get default signing identity: %v", err)
		os.Exit(3)
	}
	return signer
}

func getMspIDfromDir(mspDir string) string {
	re := regexp.MustCompile(`/ordererOrganizations/([^/]+)/`)
	matches := re.FindStringSubmatch(mspDir)
	if matches == nil || len(matches) > 2 {
		fmt.Fprintf(os.Stderr, "failed to extract mspID from path: %s", mspDir)
		os.Exit(3)
	}
	return matches[1]
}
