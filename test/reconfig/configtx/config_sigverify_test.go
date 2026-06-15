/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configtx

import (
	"os"
	"path/filepath"
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

// TestSubmitReceiveAndVerifySignaturesConfigBlock tests the end-to-end process of submitting a configuration
// transaction, receiving the resulting configuration block, and verifying its signatures.
func TestSubmitReceiveAndVerifySignaturesConfigBlock(t *testing.T) {
	// 1. Builds the arma binary and creates a temporary directory for test artifacts
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	numOfShards := 1
	numOfParties := 1

	// create temp dir
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 2. Creates a network configuration with the specified number of shards and parties
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)
	numOfArmaNodes := len(netInfo)

	// 3. Generates necessary cryptographic materials and network artifacts using the arma CLI
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// 4. Starts arma nodes and waits for them to be ready
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	require.NoError(t, err)
	require.NotNil(t, uc)

	logger := testutil.CreateLogger(t, 0)

	// 5. Builds a signature verifier from the consenter information
	verifier := utils.BuildVerifier(dir, types.PartyID(1), logger)

	// 6. Creates a broadcast client to submit transactions and submits a configuration transaction based on the genesis block
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	// Create config tx
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	submittingPartyID := 1
	configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, genesisBlockPath)

	configUpdatePbData := configUpdateBuilder.UpdateBatchSizeConfig(t, configutil.NewBatchSizeConfig(configutil.BatchSizeConfigName.MaxMessageCount, 500))
	require.NotEmpty(t, configUpdatePbData)

	env := configutil.CreateConfigTX(t, dir, []types.PartyID{1}, submittingPartyID, configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTx(env)
	require.NoError(t, err)
	totalBlocks := 2 // genesis + config block

	// 7. Pulls and verifies blocks from assemblers, ensuring the configuration block's signatures are valid
	statusSuccess := common.Status_UNKNOWN
	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig: uc,
		Parties:    []types.PartyID{types.PartyID(submittingPartyID)},
		StartBlock: uint64(0),
		Status:     &statusSuccess,
		Verifier:   verifier,
		Blocks:     totalBlocks,
		ErrString:  "cancelled pull from assembler: %d",
		LogString:  "configuration block 1 partyID %d verified with",
		Signer:     signutil.CreateTestSigner(t, "org1", dir),
	})
}
