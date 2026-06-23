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
	"strings"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
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

	// Create an ECDSA Signer
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

func CreateSignerForUser(userMspDir string) (identity.SignerSerializer, error) {
	mspID, err := GetMspIDfromDir(userMspDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get mspID from user msp dir: %s, err: %v", userMspDir, err)
	}
	keyPath := filepath.Join(userMspDir, "keystore", "priv_sk")
	mspSignCertPath := filepath.Join(userMspDir, "signcerts")
	var certPath string
	files, err := os.ReadDir(mspSignCertPath)
	if err != nil {
		return nil, fmt.Errorf("error reading directory %s", mspSignCertPath)
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".pem") {
			certPath = filepath.Join(mspSignCertPath, file.Name())
			break
		}
	}
	signer, err := NewTestSigner(keyPath, certPath, mspID)
	if err != nil {
		return nil, fmt.Errorf("failed to get default signing identity: %v", err)
	}
	return signer, nil
}

func LoadCryptoMaterialForSigner(mspDir string) (*crypto.ECDSASigner, []byte, error) {
	keyBytes, err := os.ReadFile(filepath.Join(mspDir, "keystore", "priv_sk"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read private key file: %w", err)
	}
	privateKey, err := tx.CreateECDSAPrivateKey(keyBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create private key: %w", err)
	}
	certBytes, err := os.ReadFile(filepath.Join(mspDir, "signcerts", "sign-cert.pem"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read sign certificate: %w", err)
	}
	return (*crypto.ECDSASigner)(privateKey), certBytes, nil
}

func GetMspIDfromDir(mspDir string) (string, error) {
	re := regexp.MustCompile(`/ordererOrganizations/([^/]+)/`)
	matches := re.FindStringSubmatch(mspDir)
	if matches == nil || len(matches) > 2 {
		return "", fmt.Errorf("failed to extract mspID from path: %s", mspDir)
	}
	return matches[1], nil
}
