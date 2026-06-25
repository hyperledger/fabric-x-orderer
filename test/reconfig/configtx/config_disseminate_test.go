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

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/onsi/gomega/gexec"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/stretchr/testify/require"
)

func TestConfigTXDisseminationVerificationFailure(t *testing.T) {
	// Compile Arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Generate the configuration with clientSignatureVerificationRequired = True
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 2
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)
	numOfArmaNodes := len(netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	// Run Arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()
	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	// Create a broadcast client
	submittingParty := 1
	uc, err := testutil.GetUserConfig(dir, types.PartyID(submittingParty))
	require.NoError(t, err)
	require.NotNil(t, uc)
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	require.NotNil(t, signer)
	require.NotNil(t, certBytes)

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	// Create a well signed config transaction
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, genesisBlockPath)
	configUpdatePbData := configUpdateBuilder.UpdateBatchSizeConfig(t, cfgutil.NewBatchSizeConfig(cfgutil.BatchSizeConfigName.MaxMessageCount, 500))
	require.NotEmpty(t, configUpdatePbData)
	env := cfgutil.CreateConfigTX(t, dir, []types.PartyID{1, 2, 3, 4}, submittingParty, configUpdatePbData)
	require.NotNil(t, env)

	// Override signature to damage transaction
	env.Signature = []byte("signature")

	// Send the config tx and expect rejection
	err = broadcastClient.SendTx(env)
	require.Error(t, err)
	require.ErrorContains(t, err, "INTERNAL_SERVER_ERROR, Info: request structure verification error: signature did not satisfy policy /Channel/Writers")

	// Create a config tx signed by only one admin, no majority
	configUpdateBuilder = cfgutil.NewConfigUpdateBuilder(t, dir, genesisBlockPath)
	configUpdatePbData = configUpdateBuilder.UpdateBatchSizeConfig(t, cfgutil.NewBatchSizeConfig(cfgutil.BatchSizeConfigName.MaxMessageCount, 500))
	require.NotEmpty(t, configUpdatePbData)
	env = cfgutil.CreateConfigTX(t, dir, []types.PartyID{1}, submittingParty, configUpdatePbData)
	err = broadcastClient.SendTx(env)
	require.Error(t, err)
	require.ErrorContains(t, err, "1 sub-policies were satisfied, but this policy requires 3 of the 'Admins' sub-policies to be satisfied")
}
