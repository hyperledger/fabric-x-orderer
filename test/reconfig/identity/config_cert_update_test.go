/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package identity

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/tools/configtxgen"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	test_utils "github.com/hyperledger/fabric-x-orderer/test/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var caFolders = map[string]struct{}{
	"ca":         {},
	"tlsca":      {},
	"cacerts":    {},
	"tlscacerts": {},
}

// TestChangePartyCertificates verifies that updating a party's certificates via a config update succeeds,
// and that the party can continue processing transactions after the config update with the new certificates.
func TestChangePartyCertificates(t *testing.T) {
	// Prepare Arma config and crypto and get the genesis block
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 2
	submittingParty := types.PartyID(2)
	submittingOrg := fmt.Sprintf("org%d", submittingParty)
	partyToUpdate := types.PartyID(1)
	updateOrg := fmt.Sprintf("org%d", partyToUpdate)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	configFilePath := filepath.Join(dir, fmt.Sprintf("config/party%d/local_config_router.yaml", types.PartyID(submittingParty)))
	conf, _, err := config.LoadLocalConfig(configFilePath)
	require.NoError(t, err)

	// Modify the router configuration to require client signature verification.
	conf.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = true
	utils.WriteToYAML(conf.NodeLocalConfig, configFilePath)

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

	parties := make([]types.PartyID, 0, numOfParties)
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	nonUpdatedParties := make([]types.PartyID, 0, len(parties)-1)
	for _, party := range parties {
		if party != partyToUpdate {
			nonUpdatedParties = append(nonUpdatedParties, party)
		}
	}

	uc, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)

	totalTxNumber := 10
	// Send transactions to all parties to ensure network is operational before config update
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)

	for i := range totalTxNumber {
		txContent := tx.PrepareTxWithTimestamp(i+totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}
	pullRequestSigner := signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown := common.Status_UNKNOWN
	// Pull blocks to verify all transactions are included
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      120,
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// Create config update to change a party's certificates
	configUpdateBuilder, _ := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))

	nodesIPs := testutil.GetNodesIPsFromNetInfo(netInfo)
	require.NotNil(t, nodesIPs)

	tlsCACertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "tlsca", "tlsca-cert.pem")
	tlsCAPrivKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "tlsca", "priv_sk")

	signCACertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "ca", "ca-cert.pem")
	signCAPrivKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "ca", "priv_sk")

	// Update the router TLS certs in the config
	newRouterTlsCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "router", "tls")
	newRouterTlsKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "router", "tls", "key.pem")
	newRouterTlsCertBytes, err := armageddon.CreateNewCertificateFromCA(tlsCACertPath, tlsCAPrivKeyPath, "tls", newRouterTlsCertPath, newRouterTlsKeyPath, nodesIPs)
	require.NoError(t, err)
	configUpdateBuilder.UpdateRouterTLSCert(t, partyToUpdate, newRouterTlsCertBytes)

	// Update the batchers TLS certs and signing certs in the config
	for shardToUpdate := types.ShardID(1); int(shardToUpdate) <= numOfShards; shardToUpdate++ {
		newBatcherTlsCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "tls")
		newBatcherTlsKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "tls", "key.pem")
		newBatcherTlsCertBytes, err := armageddon.CreateNewCertificateFromCA(tlsCACertPath, tlsCAPrivKeyPath, "tls", newBatcherTlsCertPath, newBatcherTlsKeyPath, nodesIPs)
		require.NoError(t, err)
		configUpdateBuilder.UpdateBatcherTLSCert(t, partyToUpdate, shardToUpdate, newBatcherTlsCertBytes)

		newBatcherSignCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "msp", "signcerts")
		newBatcherSignKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "msp", "keystore", "priv_sk")
		newBatcherSignCertBytes, err := armageddon.CreateNewCertificateFromCA(signCACertPath, signCAPrivKeyPath, "sign", newBatcherSignCertPath, newBatcherSignKeyPath, nodesIPs)
		require.NoError(t, err)
		configUpdateBuilder.UpdateBatcherSignCert(t, partyToUpdate, shardToUpdate, newBatcherSignCertBytes)
	}

	// Update the assembler TLS certs in the config
	newAssemblerTlsCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "assembler", "tls")
	newAssemblerTlsKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "assembler", "tls", "key.pem")
	newAssemblerTlsCertBytes, err := armageddon.CreateNewCertificateFromCA(tlsCACertPath, tlsCAPrivKeyPath, "tls", newAssemblerTlsCertPath, newAssemblerTlsKeyPath, nodesIPs)
	require.NoError(t, err)
	configUpdateBuilder.UpdateAssemblerTLSCert(t, partyToUpdate, newAssemblerTlsCertBytes)

	// Update the consenter TLS certs in the config
	newConsenterTlsCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "tls")
	newConsenterTlsKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "tls", "key.pem")
	newConsenterTlsCertBytes, err := armageddon.CreateNewCertificateFromCA(tlsCACertPath, tlsCAPrivKeyPath, "tls", newConsenterTlsCertPath, newConsenterTlsKeyPath, nodesIPs)
	require.NoError(t, err)
	configUpdateBuilder.UpdateConsensusTLSCert(t, partyToUpdate, newConsenterTlsCertBytes)

	// Update the consenter signing certs in the config
	newConsenterSignCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "msp", "signcerts")
	newConsenterSignKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "msp", "keystore", "priv_sk")
	newConsenterSignCertBytes, err := armageddon.CreateNewCertificateFromCA(signCACertPath, signCAPrivKeyPath, "sign", newConsenterSignCertPath, newConsenterSignKeyPath, nodesIPs)
	require.NoError(t, err)
	configUpdateBuilder.UpdateConsenterSignCert(t, partyToUpdate, newConsenterSignCertBytes)

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdateBuilder.ConfigUpdatePBData(t))
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	broadcastClient.Stop()

	t.Logf("Wait for the party %d nodes to enter pending admin state and then stop it", partyToUpdate)
	testutil.WaitForPendingAdminByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, []types.PartyID{partyToUpdate})
	armaNetwork.StopParties([]types.PartyID{partyToUpdate})

	t.Log("Wait for arma nodes to restart dynamically")
	testutil.WaitForRelaunchByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, nonUpdatedParties, 1)

	t.Logf("Restart party %d", partyToUpdate)
	armaNetwork.RestartParties(t, []types.PartyID{partyToUpdate}, readyChan)
	testutil.WaitReady(t, readyChan, 3+numOfShards, 10)

	// Verify that the party's certificates are updated by checking the router's shared config
	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyToUpdate), "local_config_router.yaml")
	routerConfig, _, err := config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)

	var updatedPartyConfig *protos.PartyConfig
	for _, partyConfig := range routerConfig.SharedConfig.GetPartiesConfig() {
		if partyConfig.PartyID == uint32(partyToUpdate) {
			updatedPartyConfig = partyConfig
			break
		}
	}
	require.NotNil(t, updatedPartyConfig, "Updated party config not found in the config")

	newTlsCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "router", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	// Verify that the router TLS cert path is updated in the config
	require.Equal(t, newTlsCertBytes, updatedPartyConfig.RouterConfig.GetTlsCert(), "Certificate path was not updated in the config")

	// Verify that the batcher TLS certs path are updated in the config
	for _, shardConfig := range updatedPartyConfig.BatchersConfig {
		newBatcherTlsCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardConfig.ShardID), "tls")
		newTlsCertBytes, err = os.ReadFile(filepath.Join(newBatcherTlsCertPath, "tls-cert.pem"))
		require.NoError(t, err)
		newBatcherSignCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardConfig.ShardID), "msp", "signcerts")
		newSignCertBytes, err := os.ReadFile(filepath.Join(newBatcherSignCertPath, "sign-cert.pem"))
		require.NoError(t, err)
		require.Equal(t, newTlsCertBytes, shardConfig.GetTlsCert(), "Batcher certificate path was not updated in the config")
		require.Equal(t, newSignCertBytes, shardConfig.GetSignCert(), "Batcher signing certificate path was not updated in the config")
	}

	newTlsCertBytes, err = os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "assembler", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	// Verify that the assembler TLS cert path is updated in the config
	require.Equal(t, newTlsCertBytes, updatedPartyConfig.AssemblerConfig.GetTlsCert(), "Certificate path was not updated in the config")

	newTlsCertBytes, err = os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	newSignCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "msp", "signcerts", "sign-cert.pem"))
	require.NoError(t, err)
	// Verify that the consenter TLS and signing cert paths are updated in the config
	require.Equal(t, newTlsCertBytes, updatedPartyConfig.ConsenterConfig.GetTlsCert(), "Consenter certificate path was not updated in the config")
	require.Equal(t, newSignCertBytes, updatedPartyConfig.ConsenterConfig.GetSignCert(), "Consenter signing certificate path was not updated in the config")

	updatedRouterInfo := armaNetwork.GetRouter(t, partyToUpdate)
	// Verify that the updated router TLS connection to a consenter node is successful by checking for successful pulls,
	// if the TLS cert was not updated correctly, the router would fail to establish connections to the batchers and the pull would fail with TLS errors
	updatedRouterInfo.RunInfo.Session.Err.Detect("pullAndProcessDecisions -> Pulled config block number")

	for shardToUpdate := types.ShardID(1); int(shardToUpdate) <= numOfShards; shardToUpdate++ {
		updatedBatcherInfo := armaNetwork.GetBatcher(t, partyToUpdate, shardToUpdate)
		// Verify that the updated batcher TLS connection to a consenter is successful by checking for successful pulls,
		// if the TLS cert was not updated correctly, the batcher would fail to establish connections to the consenter and the pull would fail with TLS errors
		updatedBatcherInfo.RunInfo.Session.Err.Detect("replicateDecision -> Got config block number")

		assemblerInfo := armaNetwork.GetAssembler(t, partyToUpdate)
		// Verify that the assembler can pull blocks successfully with the updated certificates
		assemblerInfo.RunInfo.Session.Err.Detect("pullBlocks -> Started pulling blocks from: shard%dparty%d", shardToUpdate, partyToUpdate)
	}

	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)
	signer, certBytes, err = testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)

	for i := range totalTxNumber {
		txContent := tx.PrepareTxWithTimestamp(i+totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
	}

	broadcastClient.Stop()

	pullRequestSigner = signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown = common.Status_UNKNOWN
	// Pull blocks to verify all transactions are included
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber*2 + 1, // including config update tx
		Timeout:      120,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})
}

// TestChangePartyCACertificates tests a party's CA change.
// Scenario:
// 1. Run an arma network of 4 parties, single shard.
// 2. Send txs and pull blocks to ensure network is operational.
// 3. Create a new TLS and signing CA for party 1.
// 4. Send a config tx that appends each CA to the current CA list (TLS and signing) of party 1.
// 5. Wait for dynamic restart of all nodes.
// 6. Send more txs and pull blocks to verify the network is operational again.
// 7. Update party 1 crypto material (TLS and signing certificates) on disk with new certificates.
// 8. Send a config tx that updates all node-level TLS and signing certificates, issued by the new CA's, for party 1.
// 9. Wait for the nodes of party 1 to enter a pending admin state and stop party 1.
// 10. Restart party 1 and wait for dynamic restart of the non-updated parties
// 11. Extend client trust with the new TLS CA and send more txs and pull blocks to verify the network is operational again.
// 12. Update party 1 crypto material on disk with new CAs.
// 13. Send a config tx that updates the CA's list to include only the new CAs (i.e., remove old CAs).
// 14. Wait for dynamic restart of all nodes.
// 15. Send more txs and pull blocks to verify the network is operational again.
func TestChangePartyCACertificates(t *testing.T) {
	// 1.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 1
	submittingParty := types.PartyID(2)

	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	defer netInfo.CleanUp()
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	configFilePath := filepath.Join(dir, fmt.Sprintf("config/party%d/local_config_router.yaml", types.PartyID(submittingParty)))
	conf, _, err := config.LoadLocalConfig(configFilePath)
	require.NoError(t, err)

	// Modify the router configuration to require client signature verification.
	conf.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = true
	utils.WriteToYAML(conf.NodeLocalConfig, configFilePath)

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

	parties := make([]types.PartyID, 0, numOfParties)
	for i := 1; i <= numOfParties; i++ {
		parties = append(parties, types.PartyID(i))
	}

	uc, err := testutil.GetUserConfig(dir, submittingParty)
	require.NoError(t, err)

	// 2.
	txNumber := 10
	totalTxNumber := 0
	signer, certBytes, err := testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)
	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	submittingOrg := fmt.Sprintf("org%d", submittingParty)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}

	// Pull blocks to verify all transactions are included
	pullRequestSigner := signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown := common.Status_UNKNOWN
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Timeout:      120,
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// 3.
	configUpdateBuilder, _ := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))

	partyToUpdate := types.PartyID(1)
	nonUpdatedParties := slices.DeleteFunc(slices.Clone(parties), func(partyID types.PartyID) bool {
		return partyID == partyToUpdate
	})

	nodesIPs := testutil.GetNodesIPsFromNetInfo(netInfo)
	require.NotNil(t, nodesIPs)

	var batchersEndpoints []string
	for shardID := types.ShardID(1); int(shardID) <= numOfShards; shardID++ {
		batchersEndpoints = append(batchersEndpoints, netInfo[testutil.NodeName{PartyID: partyToUpdate, NodeType: testutil.Batcher, ShardID: shardID}].Listener.Addr().String())
	}

	networkConfig := &generate.Network{
		Parties: []generate.Party{
			{
				ID:                partyToUpdate,
				RouterEndpoint:    netInfo[testutil.NodeName{PartyID: partyToUpdate, NodeType: testutil.Router}].Listener.Addr().String(),
				ConsenterEndpoint: netInfo[testutil.NodeName{PartyID: partyToUpdate, NodeType: testutil.Consensus}].Listener.Addr().String(),
				BatchersEndpoints: batchersEndpoints,
				AssemblerEndpoint: netInfo[testutil.NodeName{PartyID: partyToUpdate, NodeType: testutil.Assembler}].Listener.Addr().String(),
			},
		},
	}

	updateOrg := fmt.Sprintf("org%d", partyToUpdate)
	configUpdateDir := filepath.Join(dir, "config_update")
	err = os.MkdirAll(configUpdateDir, 0o755)
	require.NoError(t, err, "failed to create config update directory")
	defer os.RemoveAll(configUpdateDir)

	err = armageddon.GenerateCryptoConfig(networkConfig, configUpdateDir)
	require.NoError(t, err, "failed to regenerate crypto config with Armageddon")

	// merge the new crypto config for the updated party to the existing crypto config directory so that the config update builder can pick up the new certs
	copyDir(
		filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", updateOrg),
		filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg),
		copyCAFilesPredicate,
		true,
	)

	// Append the new TLS CA cert and Sign CA cert to the config update builder
	newTlsCACertPath := filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", updateOrg, "msp", "tlscacerts", "tlsca-cert.pem")
	newTlsCACertBytes, err := os.ReadFile(newTlsCACertPath)
	require.NoError(t, err)
	configUpdateBuilder.AppendPartyTLSCACerts(t, partyToUpdate, [][]byte{newTlsCACertBytes})

	newSignCACertPath := filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", updateOrg, "msp", "cacerts", "ca-cert.pem")
	newSignCACertBytes, err := os.ReadFile(newSignCACertPath)
	require.NoError(t, err)
	configUpdateBuilder.AppendPartyCACerts(t, partyToUpdate, [][]byte{newSignCACertBytes})

	// Submit config update
	env := configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdateBuilder.ConfigUpdatePBData(t))
	require.NotNil(t, env)

	// 4.
	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	totalTxNumber++

	broadcastClient.Stop()

	// 5.
	t.Log("Wait for arma nodes to restart dynamically")
	testutil.WaitForNetworkRelaunch(t, netInfo, 1)

	// Verify that the party's CA certificates are updated by checking the router's shared config
	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyToUpdate), "local_config_router.yaml")
	routerConfig, lastConfigBlock, err := config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)

	routerNodeConfig := routerConfig.ExtractRouterConfig(lastConfigBlock)
	ordererConfig, ok := routerNodeConfig.Bundle.OrdererConfig()
	require.True(t, ok, "failed to extract orderer config from the last config block")

	routerSharedConfig := protos.SharedConfig{}
	err = proto.Unmarshal(ordererConfig.ConsensusMetadata(), &routerSharedConfig)
	require.NoError(t, err)

	var updatedPartyConfig *protos.PartyConfig
	for _, partyConfig := range routerSharedConfig.GetPartiesConfig() {
		if partyConfig.PartyID == uint32(partyToUpdate) {
			updatedPartyConfig = partyConfig
			break
		}
	}
	require.NotNil(t, updatedPartyConfig, "Updated party config not found in the config")

	require.True(t, func() bool {
		for _, cert := range updatedPartyConfig.GetCACerts() {
			if bytes.Equal(cert, newSignCACertBytes) {
				return true
			}
		}
		return false
	}(), "Signing CA certs were not updated in the config")

	require.True(t, func() bool {
		for _, cert := range updatedPartyConfig.TLSCACerts {
			if bytes.Equal(cert, newTlsCACertBytes) {
				return true
			}
		}
		return false
	}(), "TLS CA certs were not updated in the config")

	// 6.
	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}

	// Pull blocks to verify all transactions are included
	pullRequestSigner = signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown = common.Status_UNKNOWN
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	copyDir(
		filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", updateOrg), filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg),
		copyNonCAFilesPredicate, false,
	)

	// 7.
	newConfigBlockPath := filepath.Join(dir, "config1.block")
	err = configtxgen.WriteOutputBlock(lastConfigBlock, newConfigBlockPath)
	require.NoError(t, err)

	configUpdateBuilder, _ = configutil.NewConfigUpdateBuilder(t, dir, newConfigBlockPath)

	// Update the router TLS certs in the config
	newRouterTlsCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "router", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdateRouterTLSCert(t, partyToUpdate, newRouterTlsCertBytes)

	// Update the batchers TLS certs and signing certs in the config
	for shardToUpdate := types.ShardID(1); int(shardToUpdate) <= numOfShards; shardToUpdate++ {
		newBatcherTlsCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "tls", "tls-cert.pem"))
		require.NoError(t, err)
		configUpdateBuilder.UpdateBatcherTLSCert(t, partyToUpdate, shardToUpdate, newBatcherTlsCertBytes)

		newBatcherSignCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), fmt.Sprintf("batcher%d", shardToUpdate), "msp", "signcerts", "sign-cert.pem"))
		require.NoError(t, err)
		configUpdateBuilder.UpdateBatcherSignCert(t, partyToUpdate, shardToUpdate, newBatcherSignCertBytes)
	}

	// Update the assembler TLS certs in the config
	newAssemblerTlsCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "assembler", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdateAssemblerTLSCert(t, partyToUpdate, newAssemblerTlsCertBytes)

	// Update the consenter TLS certs in the config
	newConsenterTlsCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "tls", "tls-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdateConsensusTLSCert(t, partyToUpdate, newConsenterTlsCertBytes)

	// Update the consenter signing certs in the config
	newConsenterSignCertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "orderers", fmt.Sprintf("party%d", partyToUpdate), "consenter", "msp", "signcerts", "sign-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdateConsenterSignCert(t, partyToUpdate, newConsenterSignCertBytes)

	// Submit config update
	env = configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdateBuilder.ConfigUpdatePBData(t))
	require.NotNil(t, env)

	// 8.
	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	totalTxNumber++

	broadcastClient.Stop()

	// 9.
	t.Logf("Wait for party %d to enter pending admin state and then stop it", partyToUpdate)
	testutil.WaitForPendingAdminByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, []types.PartyID{partyToUpdate})
	armaNetwork.StopParties([]types.PartyID{partyToUpdate})

	// 10.
	// Restart the updated party
	armaNetwork.RestartParties(t, []types.PartyID{partyToUpdate}, readyChan)
	testutil.WaitReady(t, readyChan, 3+numOfShards, 10)

	t.Log("Wait for arma nodes to restart dynamically")
	testutil.WaitForRelaunchByTypeAndParty(t, netInfo, []testutil.NodeType{testutil.Consensus, testutil.Assembler, testutil.Batcher, testutil.Router}, nonUpdatedParties, 2)

	// Get the config block from router's config store and write it to a temp location
	_, lastConfigBlock, err = config.ReadConfig(routerNodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigRouter", zap.DebugLevel))
	require.NoError(t, err)
	newConfigBlockPath = filepath.Join(dir, "config2.block")
	err = configtxgen.WriteOutputBlock(lastConfigBlock, newConfigBlockPath)
	require.NoError(t, err)

	// 11.
	// Update the TLS CA certs
	uc.TLSCACerts = append(uc.TLSCACerts, newTlsCACertBytes)

	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)
	signer, certBytes, err = testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}

	// Pull blocks to verify all transactions are included
	pullRequestSigner = signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown = common.Status_UNKNOWN
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// 12.
	configUpdateBuilder, _ = configutil.NewConfigUpdateBuilder(t, dir, newConfigBlockPath)

	oldUC, err := testutil.GetUserConfig(dir, partyToUpdate)
	require.NoError(t, err)
	oldSigner, oldCertBytes, err := testutil.LoadCryptoMaterialsFromDir(t, oldUC.MSPDir)
	require.NoError(t, err)

	// Override the party's crypto materials with the new ones regenerated
	dstDir := filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg)
	err = os.RemoveAll(dstDir)
	require.NoError(t, err, "failed to remove directory %s", dstDir)
	copyDir(filepath.Join(configUpdateDir, "crypto", "ordererOrganizations", updateOrg), dstDir, copyAllPredicate, false)

	// Set the new TLS CA cert and Sign CA cert to the config update builder
	tlsCACertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "msp", "tlscacerts", "tlsca-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdatePartyTLSCACerts(t, partyToUpdate, [][]byte{tlsCACertBytes})

	signCACertBytes, err := os.ReadFile(filepath.Join(dir, "crypto", "ordererOrganizations", updateOrg, "msp", "cacerts", "ca-cert.pem"))
	require.NoError(t, err)
	configUpdateBuilder.UpdatePartyCACerts(t, partyToUpdate, [][]byte{signCACertBytes})

	// Submit a new config update
	env = configutil.CreateConfigTX(t, dir, parties, int(submittingParty), configUpdateBuilder.ConfigUpdatePBData(t))
	require.NotNil(t, env)

	// 13.
	// Send the config tx
	err = broadcastClient.SendTxTo(env, submittingParty)
	require.NoError(t, err)

	totalTxNumber++

	broadcastClient.Stop()

	// 14.
	testutil.WaitForNetworkRelaunch(t, netInfo, 3)

	// 15.
	broadcastClient = client.NewBroadcastTxClient(uc, 10*time.Second)
	signer, certBytes, err = testutil.LoadCryptoMaterialsFromDir(t, uc.MSPDir)
	require.NoError(t, err)

	for range txNumber {
		txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
		env := tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, submittingOrg)
		err = broadcastClient.SendTx(env)
		require.NoError(t, err)
		totalTxNumber++
	}

	// Pull blocks to verify all transactions are included
	pullRequestSigner = signutil.CreateTestSigner(t, submittingOrg, dir)
	statusUnknown = common.Status_UNKNOWN
	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		Transactions: totalTxNumber,
		Timeout:      60,
		ErrString:    "cancelled pull from assembler: %d; pull ended: failed to receive a deliver response: rpc error: code = Canceled desc = grpc: the client connection is closing",
		Status:       &statusUnknown,
		Signer:       pullRequestSigner,
	})

	// Try sending a transaction with the old certificate which should fail as the old cert is no longer trusted by the network
	txContent := tx.PrepareTxWithTimestamp(totalTxNumber, 64, []byte("sessionNumber"))
	env = tx.CreateSignedStructuredEnvelope(txContent, oldSigner, oldCertBytes, fmt.Sprintf("org%d", partyToUpdate))
	err = broadcastClient.SendTx(env)
	require.ErrorContains(t, err, "signature did not satisfy policy", "expected error when sending transaction with old certificate after CA rotation, but got no error")

	broadcastClient.Stop()
}

func copyAllPredicate(_ string, d os.DirEntry) bool {
	return true
}

func copyDir(src, dst string, pred copyPredicate, backUpOriginal bool) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if pred != nil && !pred(path, d) {
			return nil
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		target := filepath.Join(dst, relPath)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if backUpOriginal {
			target = uniqueFileName(target)
		}

		err = armageddon.CopyFile(path, target)
		return err
	})
}

func uniqueFileName(path string) string {
	base := path
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]
	for i := 1; ; i++ {
		if _, err := os.Stat(base); os.IsNotExist(err) {
			return base
		}
		base = name + "-bak" + strconv.Itoa(i) + ext
	}
}

type copyPredicate func(path string, d os.DirEntry) bool

func copyNonCAFilesPredicate(path string, d os.DirEntry) bool {
	if d.IsDir() {
		return false
	}

	dir := filepath.Dir(path)

	if _, ok := caFolders[filepath.Base(dir)]; ok {
		return false
	}
	return true
}

func copyCAFilesPredicate(path string, d os.DirEntry) bool {
	if d.IsDir() {
		return false
	}

	dir := filepath.Dir(path)

	if _, ok := caFolders[filepath.Base(dir)]; ok {
		return true
	}
	return false
}
