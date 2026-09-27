/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configtx

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/test/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
)

// TestSubmitConfigTxSignedByAdminUnderAdmincerts submits an admin-signed configuration transaction
// to ARMA end-to-end when the crypto is generated with "--noOUs" — node OUs are off and
// admin authority is conveyed by admincerts. It asserts the transaction is accepted and the
// resulting configuration block is committed and verifiable.
func TestSubmitConfigTxSignedByAdminUnderAdmincerts(t *testing.T) {
	armaBinaryPath, err := gexec.BuildWithEnvironment(
		"github.com/hyperledger/fabric-x-orderer/cmd/arma",
		[]string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")},
	)
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	numOfShards := 1
	numOfParties := 1

	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)
	numOfArmaNodes := len(netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--noOUs"})

	// The generated org MSP is in admincerts mode — no NodeOUs config.yaml is present
	// and the admincerts folder holds the admin certificate, so the admin below is authorized only by it.
	orgMSP := filepath.Join(dir, "crypto", "ordererOrganizations", "org1", "msp")
	require.NoFileExists(t, filepath.Join(orgMSP, "config.yaml"))
	adminCerts, err := filepath.Glob(filepath.Join(orgMSP, "admincerts", "*.pem"))
	require.NoError(t, err)
	require.NotEmpty(t, adminCerts, "admincerts must hold the admin certificate when node OUs are off")

	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()
	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	require.NoError(t, err)
	require.NotNil(t, uc)

	logger := testutil.CreateLogger(t, 0)
	verifier := utils.BuildVerifier(dir, types.PartyID(1), logger)

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	submittingPartyID := 1
	configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, genesisBlockPath)

	requestBatchMaxBytes := uint64(1048576)
	configUpdatePbData := configUpdateBuilder.UpdateSmartBFTConfig(t, configutil.NewSmartBFTConfig(
		configutil.SmartBFTConfigName.RequestBatchMaxBytes,
		strconv.FormatUint(requestBatchMaxBytes, 10),
	))
	require.NotEmpty(t, configUpdatePbData)

	// The config tx is signed by the org admin, whose authority comes solely from its admincerts entry.
	env := configutil.CreateConfigTX(t, dir, []types.PartyID{1}, submittingPartyID, configUpdatePbData)
	require.NotNil(t, env)

	// A nil error means the router accepted the broadcast with Status_SUCCESS.
	err = broadcastClient.SendTx(env)
	require.NoError(t, err)

	testutil.WaitForNetworkRelaunch(t, netInfo, 1)

	totalBlocks := 2 // genesis + config block
	statusSuccess := common.Status_UNKNOWN
	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig: uc,
		Parties:    []types.PartyID{types.PartyID(submittingPartyID)},
		StartBlock: uint64(0),
		Status:     &statusSuccess,
		Verifier:   verifier,
		Blocks:     totalBlocks,
		Timeout:    90,
		ErrString:  "cancelled pull from assembler: %d",
		LogString:  "configuration block 1 partyID %d verified with",
		Signer:     signutil.CreateTestSigner(t, "org1", dir),
	})
}
