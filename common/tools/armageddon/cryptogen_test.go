/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon_test

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	mspa "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
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
			orgMSP := setupOrgMSP(t, orgDir, "org1")

			certDir := t.TempDir()
			newSignCert, err := armageddon.CreateNewCertificateFromCA(
				filepath.Join(orgDir, "ca", "org1-CA-cert.pem"), filepath.Join(orgDir, "ca", "priv_sk"), "sign",
				filepath.Join(certDir, "sign-cert.pem"), filepath.Join(certDir, "priv_sk"), []string{"127.0.0.1"},
			)
			require.NoError(t, err)

			id, err := orgMSP.DeserializeIdentity(msppb.NewIdentity("org1", newSignCert))
			require.NoError(t, err)
			require.NoError(t, id.Validate())
			if tc.enableNodeOUs {
				requireRoleOU(t, orgMSP, "org1", newSignCert, "orderer", mspa.MSPRole_ORDERER)
			}
		})
	}
}

// TestGenerateCryptoConfigAssignsRoleOUs verifies that, with node OUs enabled, every certificate generated for an
// orderer org carries the OU of its role and is classified by the org MSP accordingly: the signing certificates of
// the Arma nodes carry the orderer OU, the client user carries the client OU and the admin user carries the admin
// OU. Only the admin user is an admin of the org.
func TestGenerateCryptoConfigAssignsRoleOUs(t *testing.T) {
	dir := t.TempDir()
	networkConfig := testutil.GenerateNetworkConfig(t, "none", "none")
	_, err := armageddon.GenerateCryptoConfigWithProfile(&networkConfig, dir, true)
	require.NoError(t, err)

	for _, party := range networkConfig.Parties {
		org := fmt.Sprintf("org%d", party.ID)
		orgDir := filepath.Join(dir, "crypto", "ordererOrganizations", org)
		orgMSP := setupOrgMSP(t, orgDir, org)

		nodes := []string{"router", "consenter", "assembler"}
		for i := range party.BatchersEndpoints {
			nodes = append(nodes, fmt.Sprintf("batcher%d", i+1))
		}
		partyDir := filepath.Join(orgDir, "orderers", fmt.Sprintf("party%d", party.ID))
		for _, node := range nodes {
			t.Run(fmt.Sprintf("%s %s", org, node), func(t *testing.T) {
				cert := readCert(t, filepath.Join(partyDir, node, "msp", "signcerts"), node)
				requireRoleOU(t, orgMSP, org, cert, "orderer", mspa.MSPRole_ORDERER)
			})
		}

		for _, user := range []struct {
			name string
			ou   string
			role mspa.MSPRole_MSPRoleType
		}{
			{name: "client@" + org, ou: "client", role: mspa.MSPRole_CLIENT},
			{name: "Admin@" + org, ou: "admin", role: mspa.MSPRole_ADMIN},
		} {
			t.Run(user.name, func(t *testing.T) {
				cert := readCert(t, filepath.Join(orgDir, "users", user.name, "msp", "signcerts"), user.name)
				requireRoleOU(t, orgMSP, org, cert, user.ou, user.role)
			})
		}
	}
}

// setupOrgMSP sets up the verifying MSP of an org from its generated MSP folder.
func setupOrgMSP(t *testing.T, orgDir, org string) msp.MSP {
	t.Helper()
	mspConfig, err := msp.GetVerifyingMspConfig(filepath.Join(orgDir, "msp"), org, msp.ProviderTypeToString(msp.FABRIC))
	require.NoError(t, err)
	orgMSP, err := msp.New(&msp.BCCSPNewOpts{NewBaseOpts: msp.NewBaseOpts{Version: msp.MSPv3_0}}, factory.GetDefault())
	require.NoError(t, err)
	require.NoError(t, orgMSP.Setup(mspConfig))
	return orgMSP
}

// readCert reads the PEM certificate <name>-cert.pem from dir.
func readCert(t *testing.T, dir, name string) []byte {
	t.Helper()
	cert, err := os.ReadFile(filepath.Join(dir, name+"-cert.pem"))
	require.NoError(t, err)
	return cert
}

// requireRoleOU asserts that the PEM certificate carries the given OU, that the org MSP classifies it with the given
// role, and that it is not an admin of the org unless that role is the admin role.
func requireRoleOU(t *testing.T, orgMSP msp.MSP, org string, certPEM []byte, ou string, role mspa.MSPRole_MSPRoleType) {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block, "certificate is not PEM encoded")
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	require.Contains(t, cert.Subject.OrganizationalUnit, ou)

	id, err := orgMSP.DeserializeIdentity(msppb.NewIdentity(org, certPEM))
	require.NoError(t, err)
	require.NoError(t, id.SatisfiesPrincipal(rolePrincipal(org, role)), "expected role %s", role)
	if role != mspa.MSPRole_ADMIN {
		require.Error(t, id.SatisfiesPrincipal(rolePrincipal(org, mspa.MSPRole_ADMIN)), "must not be an admin")
	}
}

func rolePrincipal(org string, role mspa.MSPRole_MSPRoleType) *mspa.MSPPrincipal {
	return &mspa.MSPPrincipal{
		PrincipalClassification: mspa.MSPPrincipal_ROLE,
		Principal:               protoutil.MarshalOrPanic(&mspa.MSPRole{MspIdentifier: org, Role: role}),
	}
}
