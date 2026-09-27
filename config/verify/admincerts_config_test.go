/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify_test

import (
	"path/filepath"
	"testing"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
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
