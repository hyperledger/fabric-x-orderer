/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package params

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/test/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
)

// TestUpdateTimeoutParameters verifies that updating a party's timeout parameters via a config update succeeds,
// and that the party can continue processing transactions after the config update with the new timeout parameters.
// NOTE: as the memory pool options are not updated in dynamic reconfig, this scenario is tested as an admin action requirement.
// TODO: move to dynamic reconfig approach
func TestUpdateTimeoutParameters(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 2, "none", "none")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	userConfig, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

	txNumber := 100
	// rate limiter parameters
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	rate := 500

	capacity := rate / fillFrequency
	rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
		os.Exit(3)
	}

	broadcastClient := client.NewBroadcastTxClient(userConfig, 10*time.Second)
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, userConfig.MSPDir)
	require.NoError(t, err)

	org := fmt.Sprintf("org%d", submittingParty)

	for i := range txNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	totalTxNumber := txNumber

	var parties []types.PartyID
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	pullRequestSigner := signutil.CreateTestSigner(t, "org1", dir)

	statusUnknown := common.Status_UNKNOWN
	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// Create config update
	configUpdateBuilder, _ := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))

	autoRemoveTimeout := "100ms"
	configUpdatePbData := configUpdateBuilder.UpdateBatchTimeouts(t, configutil.NewBatchTimeoutsConfig(configutil.BatchTimeoutsConfigName.AutoRemoveTimeout, autoRemoveTimeout))

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	// Restart Arma nodes
	armaNetwork.Stop()

	armaNetwork.Restart(t, readyChan)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	// Send transactions again and verify they are processed
	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)

	for i := range txNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i+txNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	totalTxNumber += txNumber

	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber + 1, // including config update tx
		BlockHandler: &verifyTimeoutParam{
			AutoRemoveTimeout: autoRemoveTimeout,
		},
		Timeout:   60,
		ErrString: "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:    &statusUnknown,
		Signer:    pullRequestSigner,
	})
}

// TestUpdateSmartBFTParameters verifies that updating SmartBFT parameters via a config update succeeds,
// and that the network can continue processing transactions after the config update with the new parameters.
func TestUpdateSmartBFTParameters(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 2, "none", "none")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	userConfig, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

	txNumber := 100

	// rate limiter parameters
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	rate := 500

	capacity := rate / fillFrequency
	rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
		os.Exit(3)
	}

	broadcastClient := client.NewBroadcastTxClient(userConfig, 10*time.Second)
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, userConfig.MSPDir)
	require.NoError(t, err)

	org := fmt.Sprintf("org%d", submittingParty)

	for i := range txNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	totalTxNumber := txNumber

	var parties []types.PartyID
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	pullRequestSigner := signutil.CreateTestSigner(t, "org1", dir)

	statusUnknown := common.Status_UNKNOWN
	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// Create config update
	configUpdateBuilder, _ := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))

	requestBatchMaxBytes := uint64(1048576)
	configUpdatePbData := configUpdateBuilder.UpdateSmartBFTConfig(t, configutil.NewSmartBFTConfig(configutil.SmartBFTConfigName.RequestBatchMaxBytes, strconv.FormatUint(requestBatchMaxBytes, 10)))

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	t.Log("Wait for arma nodes to restart dynamically")
	testutil.WaitForNetworkRelaunch(t, netInfo, 1)

	// Send transactions again and verify they are processed
	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)

	for i := range txNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i+txNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	totalTxNumber += txNumber

	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber + 1, // including config update tx
		BlockHandler: &verifySmartBFTParam{
			RequestBatchMaxBytes: requestBatchMaxBytes,
		},
		Timeout:   60,
		ErrString: "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:    &statusUnknown,
		Signer:    pullRequestSigner,
	})
}

// TestUpdateBatchingParameters verifies that updating a party's batching parameters via a config update succeeds,
// and that the party can continue processing transactions after the config update with the new batching parameters.
// NOTE: as the memory pool options are not updated in dynamic reconfig, this scenario is tested as an admin action requirement.
// TODO: move to dynamic reconfig approach
func TestUpdateBatchingParameters(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, 2, "none", "none")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan string, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	userConfig, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

	txNumber := 100
	// rate limiter parameters
	fillInterval := 10 * time.Millisecond
	fillFrequency := 1000 / int(fillInterval.Milliseconds())
	rate := 500

	capacity := rate / fillFrequency
	rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
		os.Exit(3)
	}

	broadcastClient := client.NewBroadcastTxClient(userConfig, 10*time.Second)
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, userConfig.MSPDir)
	require.NoError(t, err)

	// Create config update
	configUpdateBuilder, _ := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))

	maxMessageCount := 2
	configUpdatePbData := configUpdateBuilder.UpdateBatchSizeConfig(t, configutil.NewBatchSizeConfig(configutil.BatchSizeConfigName.MaxMessageCount, maxMessageCount))

	var parties []types.PartyID
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	// Restart Arma nodes
	armaNetwork.Stop()

	armaNetwork.Restart(t, readyChan)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	// Send transactions again and verify they are processed
	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)

	for i := range txNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, fmt.Sprintf("org%d", submittingParty))
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	totalTxNumber := txNumber

	pullRequestSigner := signutil.CreateTestSigner(t, "org1", dir)
	statusUnknown := common.Status_UNKNOWN

	utils.PullFromAssemblers(t, &utils.BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber + 1, // including config update tx
		BlockHandler: &verifyMaxMessageCount{MaxMessageCount: maxMessageCount},
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})
}

type verifyMaxMessageCount struct {
	MaxMessageCount int
}

func (vm *verifyMaxMessageCount) HandleBlock(t *testing.T, block *common.Block) error {
	isGenesisBlock := block.Header.Number == 0 || block.Header.GetDataHash() == nil
	if isGenesisBlock {
		return nil
	}

	if protoutil.IsConfigBlock(block) {
		envelope, err := configutil.ReadConfigEnvelopeFromConfigBlock(block)
		if err != nil || envelope == nil {
			return fmt.Errorf("failed to read config envelope from config block: %w", err)
		}

		sharedConfig := configutil.GetSharedConfig(t, envelope)
		require.NotNil(t, sharedConfig)

		require.Equal(t, vm.MaxMessageCount, int(sharedConfig.BatchingConfig.BatchSize.MaxMessageCount), "MaxMessageCount in the config block does not match the expected value")
		return nil
	}

	if len(block.GetData().GetData()) > vm.MaxMessageCount {
		return fmt.Errorf("block contains %d transactions, which exceeds the max message count per block of %d", len(block.GetData().GetData()), vm.MaxMessageCount)
	}

	return nil
}

type verifyTimeoutParam struct {
	AutoRemoveTimeout string
}

func (vt *verifyTimeoutParam) HandleBlock(t *testing.T, block *common.Block) error {
	isGenesisBlock := block.Header.Number == 0 || block.Header.GetDataHash() == nil
	if isGenesisBlock {
		return nil
	}

	if protoutil.IsConfigBlock(block) {
		envelope, err := configutil.ReadConfigEnvelopeFromConfigBlock(block)
		if err != nil || envelope == nil {
			return fmt.Errorf("failed to read config envelope from config block: %w", err)
		}

		sharedConfig := configutil.GetSharedConfig(t, envelope)
		require.NotNil(t, sharedConfig)

		if vt.AutoRemoveTimeout != sharedConfig.BatchingConfig.BatchTimeouts.AutoRemoveTimeout {
			return fmt.Errorf("AutoRemoveTimeout in the config block does not match the expected value. Expected: %s, Got: %s", vt.AutoRemoveTimeout, sharedConfig.BatchingConfig.BatchTimeouts.AutoRemoveTimeout)
		}
		return nil
	}

	return nil
}

type verifySmartBFTParam struct {
	RequestBatchMaxBytes uint64
}

func (vt *verifySmartBFTParam) HandleBlock(t *testing.T, block *common.Block) error {
	isGenesisBlock := block.Header.Number == 0 || block.Header.GetDataHash() == nil
	if isGenesisBlock {
		return nil
	}

	if protoutil.IsConfigBlock(block) {
		envelope, err := configutil.ReadConfigEnvelopeFromConfigBlock(block)
		if err != nil || envelope == nil {
			return fmt.Errorf("failed to read config envelope from config block: %w", err)
		}

		sharedConfig := configutil.GetSharedConfig(t, envelope)
		require.NotNil(t, sharedConfig)

		if vt.RequestBatchMaxBytes != sharedConfig.ConsensusConfig.SmartBFTConfig.RequestBatchMaxBytes {
			return fmt.Errorf("RequestBatchMaxBytes in the config block does not match the expected value. Expected: %d, Got: %d", vt.RequestBatchMaxBytes, sharedConfig.ConsensusConfig.SmartBFTConfig.RequestBatchMaxBytes)
		}
		return nil
	}

	return nil
}
