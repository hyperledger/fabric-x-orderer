/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/stretchr/testify/require"
)

// TestProposeConfigUpdate_AdminAuthorizedViaAdmincerts verifies that when the orderer crypto is
// generated with "--noOUs" — so node OUs are off and admin authority is conveyed by admincerts — an
// admin-signed config update still satisfies the channel Admins mod-policy and is authorized.
func TestProposeConfigUpdate_AdminAuthorizedViaAdmincerts(t *testing.T) {
	dir, _, currBundle, builder, proposer, signer, verifier := setupOrdererRulesTest(t, 1, "--noOUs")

	// The org MSP is in admincerts mode — no NodeOUs config.yaml is present and the
	// admincerts folder holds the admin certificate, so admin authority can only come from admincerts.
	orgMSP := filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "msp")
	require.NoFileExists(t, filepath.Join(orgMSP, "config.yaml"))
	adminCerts, err := filepath.Glob(filepath.Join(orgMSP, "admincerts", "*.pem"))
	require.NoError(t, err)
	require.NotEmpty(t, adminCerts, "admincerts must hold the admin certificate when node OUs are off")

	// A config update signed by the org admin is authorized via its admincerts entry.
	updatePb := builder.UpdateBatchTimeouts(t, configutil.NewBatchTimeoutsConfig(
		configutil.BatchTimeoutsConfigName.BatchCreationTimeout, "1s",
	))
	updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1}, 1, updatePb)
	req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}

	_, err = proposer.ProposeConfigUpdate(req, currBundle, signer, verifier, factory.GetDefault())
	require.NoError(t, err)
}

// TestPrepareAndAddNewParty_AddedPartyAdminIsAuthorized verifies that the admin of a party added by
// PrepareAndAddNewParty is an admin of its org, both in a network generated with node OUs (the default)
// and in one generated with "--noOUs", where admin authority is conveyed by admincerts.
func TestPrepareAndAddNewParty_AddedPartyAdminIsAuthorized(t *testing.T) {
	for _, tc := range []struct {
		name          string
		enableNodeOUs bool
		genArgs       []string
	}{
		{name: "node OUs", enableNodeOUs: true},
		{name: "admincerts", enableNodeOUs: false, genArgs: []string{"--noOUs"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bccsp := factory.GetDefault()
			dir, _, currBundle, builder, proposer, signer, verifier := setupOrdererRulesTest(t, 1, tc.genArgs...)

			addedPartyID, netInfo := builder.PrepareAndAddNewParty(t, dir, tc.enableNodeOUs)
			defer func() {
				for _, ni := range netInfo {
					if ni != nil {
						ni.Close()
					}
				}
			}()

			updateEnv := configutil.CreateConfigTX(t, dir, []types.PartyID{1}, 1, builder.ConfigUpdatePBData(t))
			req := &comm.Request{Payload: updateEnv.Payload, Signature: updateEnv.Signature}
			nextCfgEnv, err := proposer.ProposeConfigUpdate(req, currBundle, signer, verifier, bccsp)
			require.NoError(t, err)
			nextBundle, err := channelconfig.NewBundleFromEnvelope(
				&common.Envelope{Payload: nextCfgEnv.Payload, Signature: nextCfgEnv.Signature}, bccsp,
			)
			require.NoError(t, err)

			addedOrg := fmt.Sprintf("org%d", addedPartyID)
			adminCert, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", addedOrg,
				"users", "Admin@"+addedOrg, "msp", "signcerts", fmt.Sprintf("Admin@%s-cert.pem", addedOrg)))
			require.NoError(t, err)
			admin, err := nextBundle.MSPManager().DeserializeIdentity(msppb.NewIdentity(addedOrg, adminCert))
			require.NoError(t, err)
			require.NoError(t, admin.SatisfiesPrincipal(&msp.MSPPrincipal{
				PrincipalClassification: msp.MSPPrincipal_ROLE,
				Principal:               protoutil.MarshalOrPanic(&msp.MSPRole{MspIdentifier: addedOrg, Role: msp.MSPRole_ADMIN}),
			}))
		})
	}
}
