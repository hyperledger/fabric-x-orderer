/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestUpdatePartyRouterEndpoint(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 2
	numOfShards := 2
	submittingParty := types.PartyID(1)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "none", "none")
	require.NotNil(t, netInfo)
	require.NoError(t, err)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan struct{}, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	userConfig, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

	totalTxNumber := 100
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

	for i := range totalTxNumber {
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

	var parties []types.PartyID
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	statusUknown := common.Status_UNKNOWN
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUknown,
	})

	// Create config update
	configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()

	partyToUpdate := types.PartyID(submittingParty)
	routerIP := strings.Split(userConfig.RouterEndpoints[partyToUpdate-1], ":")[0] // extract IP from the user config router endpoint
	availablePort, newListener := testutil.GetAvailablePort(t)
	newPort, err := strconv.Atoi(availablePort)
	require.NoError(t, err)
	routerToMonitor := armaNetwork.GetRouter(t, submittingParty)
	routerToMonitor.Listener = newListener

	configUpdatePbData := configUpdateBuilder.UpdateRouterEndpoint(t, partyToUpdate, routerIP, newPort)

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, []types.PartyID{1, 2}, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	// Pull blocks to verify all transactions are included
	userBlockHandler := &verifyRouterEndpointUpdate{updatedParty: partyToUpdate, routerIP: routerIP, newPort: newPort}
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber + 1, // including config update tx
		BlockHandler: userBlockHandler,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUknown,
	})

	require.True(t, userBlockHandler.RouterEndpointUpdated.Load(), "Router endpoint was not updated in the config update")

	// Restart Arma nodes
	armaNetwork.Stop()

	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyToUpdate), "local_config_router.yaml")

	// Verify the router node config stored in the router ledger is updated
	cfg, _, err := config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)
	require.True(t, cfg.SharedConfig.GetPartiesConfig()[partyToUpdate-1].RouterConfig.Host == routerIP &&
		cfg.SharedConfig.GetPartiesConfig()[partyToUpdate-1].RouterConfig.Port == uint32(newPort), "Shared config was not updated with the new router endpoint")

	// Update the router node local config with the new endpoint to allow it to start
	localConfig, _, err := config.LoadLocalConfig(routerNodeConfigPath)
	require.NoError(t, err)
	localConfig.NodeLocalConfig.GeneralConfig.ListenAddress = routerIP
	localConfig.NodeLocalConfig.GeneralConfig.ListenPort = uint32(newPort)
	utils.WriteToYAML(localConfig.NodeLocalConfig, routerNodeConfigPath)

	armaNetwork.Restart(t, readyChan)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	// Send transactions again and verify they are processed

	// Update the user config with the new router endpoint
	userConfig.RouterEndpoints[0] = fmt.Sprintf("%s:%d", routerIP, newPort)
	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)

	for i := range totalTxNumber {
		status := rl.GetToken()
		if !status {
			fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
			os.Exit(3)
		}
		txContent := tx.PrepareTxWithTimestamp(i+totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	// Pull blocks to verify all transactions are included
	userBlockHandler = &verifyRouterEndpointUpdate{updatedParty: partyToUpdate, routerIP: routerIP, newPort: newPort}
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   userConfig,
		Parties:      parties,
		Transactions: totalTxNumber*2 + 1, // including config update tx
		BlockHandler: userBlockHandler,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUknown,
	})

	require.True(t, userBlockHandler.RouterEndpointUpdated.Load(), "Router endpoint was not updated in the config update")
}

// Verify that the config update is applied by checking the router endpoint in the config update block
type verifyRouterEndpointUpdate struct {
	updatedParty          types.PartyID
	RouterEndpointUpdated atomic.Bool
	routerIP              string
	newPort               int
}

func (v *verifyRouterEndpointUpdate) HandleBlock(t *testing.T, block *common.Block) error {
	if protoutil.IsConfigBlock(block) {
		envelope, err := configutil.ReadConfigEnvelopeFromConfigBlock(block)
		if err != nil || envelope == nil {
			return fmt.Errorf("failed to read config envelope from config block: %w", err)
		}

		partyConfig := configutil.GetPartyConfig(t, envelope, v.updatedParty)
		if partyConfig == nil {
			return fmt.Errorf("party config for party %d not found in the config block", v.updatedParty)
		}

		v.RouterEndpointUpdated.Store(partyConfig.RouterConfig.Host == v.routerIP && partyConfig.RouterConfig.Port == uint32(v.newPort))
	}

	return nil
}

func TestAddNewParty(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 3
	numOfShards := 1

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	require.NotNil(t, netInfo)
	require.NoError(t, err)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Start Arma nodes
	numOfArmaNodes := len(netInfo)
	readyChan := make(chan struct{}, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	addedNetInfo, addedPartyConfig := testutil.ExtendNetwork(t, configPath)
	testutil.ExtendConfigAndCrypto(addedPartyConfig, dir, true)

	// Create config update
	configUpdateBuilder, cleanUp := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()

	submittingParty := types.PartyID(1)
	addedPartyId := types.PartyID(addedPartyConfig.Parties[0].ID)
	addedParty := fmt.Sprintf("party%d", addedPartyId)
	addedOrg := fmt.Sprintf("org%d", addedPartyId)

	consenterConfig, _, err := config.LoadLocalConfig(filepath.Join(dir, "config", addedParty, "local_config_consenter.yaml"))
	require.NoError(t, err)
	consenterTlsCert, err := os.ReadFile(consenterConfig.NodeLocalConfig.GeneralConfig.TLSConfig.Certificate)
	require.NoError(t, err)
	routerLocalConfig, _, err := config.LoadLocalConfig(filepath.Join(dir, "config", addedParty, "local_config_router.yaml"))
	require.NoError(t, err)
	routerTlsCert, err := os.ReadFile(routerLocalConfig.NodeLocalConfig.GeneralConfig.TLSConfig.Certificate)
	require.NoError(t, err)
	assemblerConfig, _, err := config.LoadLocalConfig(filepath.Join(dir, "config", addedParty, "local_config_assembler.yaml"))
	require.NoError(t, err)
	assemblerTlsCert, err := os.ReadFile(assemblerConfig.NodeLocalConfig.GeneralConfig.TLSConfig.Certificate)
	require.NoError(t, err)

	batchersConfig := make([]*protos.BatcherNodeConfig, len(addedPartyConfig.Parties[0].BatchersEndpoints))

	for i := range addedPartyConfig.Parties[0].BatchersEndpoints {
		batcherNodeConfig, _, err := config.LoadLocalConfig(filepath.Join(dir, "config", addedParty, fmt.Sprintf("local_config_batcher%d.yaml", i+1)))
		require.NoError(t, err)
		batcherTlsCert, err := os.ReadFile(batcherNodeConfig.NodeLocalConfig.GeneralConfig.TLSConfig.Certificate)
		require.NoError(t, err)
		batcherSignCert, err := os.ReadFile(filepath.Join(batcherNodeConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "signcerts", "sign-cert.pem"))
		require.NoError(t, err)

		batchersConfig[i] = &protos.BatcherNodeConfig{
			ShardID:  uint32(i + 1),
			Host:     batcherNodeConfig.NodeLocalConfig.GeneralConfig.ListenAddress,
			Port:     batcherNodeConfig.NodeLocalConfig.GeneralConfig.ListenPort,
			TlsCert:  batcherTlsCert,
			SignCert: batcherSignCert,
		}
	}

	caCerts, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", addedOrg, "msp", "cacerts", "ca-cert.pem"))
	require.NoError(t, err)
	tlsCACerts, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", addedOrg, "msp", "tlscacerts", "tlsca-cert.pem"))
	require.NoError(t, err)
	consenterSignCert, err := os.ReadFile(filepath.Join(consenterConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "signcerts", "sign-cert.pem"))
	require.NoError(t, err)

	configUpdatePbData := configUpdateBuilder.AddNewParty(t, &protos.PartyConfig{
		CACerts:    [][]byte{caCerts},
		TLSCACerts: [][]byte{tlsCACerts},
		ConsenterConfig: &protos.ConsenterNodeConfig{
			Host:     consenterConfig.NodeLocalConfig.GeneralConfig.ListenAddress,
			Port:     consenterConfig.NodeLocalConfig.GeneralConfig.ListenPort,
			SignCert: consenterSignCert,
			TlsCert:  consenterTlsCert,
		},
		RouterConfig: &protos.RouterNodeConfig{
			Host:    routerLocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress,
			Port:    routerLocalConfig.NodeLocalConfig.GeneralConfig.ListenPort,
			TlsCert: routerTlsCert,
		},
		AssemblerConfig: &protos.AssemblerNodeConfig{
			Host:    assemblerConfig.NodeLocalConfig.GeneralConfig.ListenAddress,
			Port:    assemblerConfig.NodeLocalConfig.GeneralConfig.ListenPort,
			TlsCert: assemblerTlsCert,
		},
		BatchersConfig: batchersConfig,
	})

	env := configutil.CreateConfigTX(t, dir, numOfParties, int(submittingParty), configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	userConfig, err := testutil.GetUserConfig(dir, types.PartyID(submittingParty))
	require.NoError(t, err)

	broadcastClient := client.NewBroadcastTxClient(userConfig, 10*time.Second)
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	// Wait for Arma nodes to stop
	testutil.WaitSoftStopped(t, netInfo)

	armaNetwork.Stop()

	configBlockStoreDir := t.TempDir()
	defer os.RemoveAll(configBlockStoreDir)

	newConfigBlockPath := filepath.Join(configBlockStoreDir, "config.block")
	_, lastConfigBlock, err := config.ReadConfig(filepath.Join(dir, "config", fmt.Sprintf("party%d", submittingParty), "local_config_assembler.yaml"), flogging.MustGetLogger("TestAddNewParty"))
	require.NoError(t, err)
	configBlock := &common.Block{Header: lastConfigBlock.GetHeader(), Data: lastConfigBlock.GetData(), Metadata: lastConfigBlock.GetMetadata()}
	err = configtxgen.WriteOutputBlock(configBlock, newConfigBlockPath)
	require.NoError(t, err)

	for _, netNode := range addedNetInfo {
		netNode.ConfigBlockPath = newConfigBlockPath
	}

	addedPartyUserConfig, err := testutil.GetUserConfig(dir, addedPartyId)
	require.NoError(t, err)

	var routerEndpoints, assemblerEndpoints []string
	var tlsCACertsBytesPartiesCollection [][]byte

	routerEndpoints = append(routerEndpoints, userConfig.RouterEndpoints...)
	routerEndpoints = append(routerEndpoints, addedPartyUserConfig.RouterEndpoints...)
	assemblerEndpoints = append(assemblerEndpoints, userConfig.AssemblerEndpoints...)
	assemblerEndpoints = append(assemblerEndpoints, addedPartyUserConfig.AssemblerEndpoints...)
	// the added party TLS CA certs already has all the existing parties TLS CA certs
	tlsCACertsBytesPartiesCollection = append(tlsCACertsBytesPartiesCollection, addedPartyUserConfig.TLSCACerts...)

	numOfParties++

	for i := range numOfParties {
		uc, err := testutil.GetUserConfig(dir, types.PartyID(i+1))
		require.NoError(t, err)

		uc.RouterEndpoints = routerEndpoints
		uc.AssemblerEndpoints = assemblerEndpoints
		uc.TLSCACerts = tlsCACertsBytesPartiesCollection

		err = utils.WriteToYAML(uc, filepath.Join(dir, "config", fmt.Sprintf("party%d", i+1), "user_config.yaml"))
		require.NoError(t, err)
	}

	maps.Copy(netInfo, addedNetInfo)
	numOfArmaNodes = len(netInfo)
	readyChan = make(chan struct{}, numOfArmaNodes)

	armaNetwork = testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	userConfig, err = testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)

	broadcastClient = client.NewBroadcastTxClient(userConfig, 10*time.Second)
	defer broadcastClient.Stop()

	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, userConfig.MSPDir)
	require.NoError(t, err)
	org := fmt.Sprintf("org%d", submittingParty)

	txContent := tx.PrepareTxWithTimestamp(0, 64, []byte("sessionNumber"))
	env = tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
	err = broadcastClient.SendTxTo(env, addedPartyId)
	require.NoError(t, err)

	// totalTxNumber := 100
	// // rate limiter parameters
	// fillInterval := 10 * time.Millisecond
	// fillFrequency := 1000 / int(fillInterval.Milliseconds())
	// rate := 500

	// capacity := rate / fillFrequency
	// rl, err := armageddon.NewRateLimiter(rate, fillInterval, capacity)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "failed to start a rate limiter")
	// 	os.Exit(3)
	// }

	// for i := range totalTxNumber {
	// 	status := rl.GetToken()
	// 	if !status {
	// 		fmt.Fprintf(os.Stderr, "failed to send tx %d", i+1)
	// 		os.Exit(3)
	// 	}
	// 	txContent := tx.PrepareTxWithTimestamp(i, 64, []byte("sessionNumber"))
	// 	env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, org)
	// 	err = broadcastClient.SendTx(env)
	// 	require.NoError(t, err)
	// }

	// var parties []types.PartyID
	// for i := 1; i <= numOfParties; i++ {
	// 	parties = append(parties, types.PartyID(i))
	// }

	// statusUknown := common.Status_UNKNOWN
	// PullFromAssemblers(t, &BlockPullerOptions{
	// 	UserConfig:   userConfig,
	// 	Parties:      parties,
	// 	Transactions: totalTxNumber,
	// 	Timeout:      120,
	// 	ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
	// 	Status:       &statusUknown,
	// })
}

func mergeUniqueCerts(certs1, certs2 [][]byte) [][]byte {
	uniqueMap := make(map[string]struct{})

	for _, item := range certs1 {
		uniqueMap[string(item)] = struct{}{}
	}

	for _, item := range certs2 {
		uniqueMap[string(item)] = struct{}{}
	}

	mergedSlice := make([][]byte, 0, len(uniqueMap))
	for key := range uniqueMap {
		mergedSlice = append(mergedSlice, []byte(key))
	}

	return mergedSlice
}
