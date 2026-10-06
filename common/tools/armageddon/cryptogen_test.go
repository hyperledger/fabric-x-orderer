/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

func TestGenerateCryptoConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	networkConfig := testutil.GenerateNetworkConfig(t, "none", "none")
	_, err = armageddon.GenerateCryptoConfigWithProfile(&networkConfig, dir, true)
	require.NoError(t, err)
}

// TestCreateNewSignCertificateFromCAIsValidOrgIdentity verifies that a signing certificate issued by
// CreateNewCertificateFromCA is a valid identity of its org MSP, both when the MSP classifies identities
// by node OUs (the default) and when it conveys admin authority by admincerts.
func TestCreateNewSignCertificateFromCAIsValidOrgIdentity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		enableNodeOUs bool
	}{
		{name: "node OUs", enableNodeOUs: true},
		{name: "admincerts", enableNodeOUs: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			networkConfig := testutil.GenerateNetworkConfig(t, "none", "none")
			_, err := armageddon.GenerateCryptoConfigWithProfile(&networkConfig, dir, tc.enableNodeOUs)
			require.NoError(t, err)

			orgDir := filepath.Join(dir, "crypto", "ordererOrganizations", "org1")
			mspConfig, err := msp.GetVerifyingMspConfig(
				filepath.Join(orgDir, "msp"), "org1", msp.ProviderTypeToString(msp.FABRIC),
			)
			require.NoError(t, err)
			orgMSP, err := msp.New(&msp.BCCSPNewOpts{NewBaseOpts: msp.NewBaseOpts{Version: msp.MSPv3_0}}, factory.GetDefault())
			require.NoError(t, err)
			require.NoError(t, orgMSP.Setup(mspConfig))

			certDir := t.TempDir()
			newSignCert, err := armageddon.CreateNewCertificateFromCA(
				filepath.Join(orgDir, "ca", "org1-CA-cert.pem"), filepath.Join(orgDir, "ca", "priv_sk"), "sign",
				filepath.Join(certDir, "sign-cert.pem"), filepath.Join(certDir, "priv_sk"), []string{"127.0.0.1"},
			)
			require.NoError(t, err)

			id, err := orgMSP.DeserializeIdentity(msppb.NewIdentity("org1", newSignCert))
			require.NoError(t, err)
			require.NoError(t, id.Validate())
		})
	}
}
