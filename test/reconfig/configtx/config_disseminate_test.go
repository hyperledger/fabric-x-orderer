/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configtx

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/configrequest/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/signutil"
	"github.com/onsi/gomega/gexec"

	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	fabricx_config "github.com/hyperledger/fabric-x-orderer/config"
	ordererRulesMocks "github.com/hyperledger/fabric-x-orderer/config/verify/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	test_utils "github.com/hyperledger/fabric-x-orderer/test/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

// TODO: remove this test since its written in a unit-test style with dummy config block and mocked verification path
// TestConfigTXDisseminationWithVerification is implemented instead.
func TestConfigDisseminate(t *testing.T) {
	t.Skip()
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	numParties := 4

	batcherNodes, batcherInfos := test_utils.CreateBatcherNodesAndInfo(t, ca, numParties)
	consenterNodes, consenterInfos := test_utils.CreateConsenterNodesAndInfo(t, ca, numParties)

	shards := []config.ShardInfo{{ShardId: 1, Batchers: batcherInfos}}

	genesisBlock := utils.EmptyGenesisBlock("arma")

	consenters, consentersConfigs, consentersLoggers, _ := test_utils.CreateConsenters(t, numParties, consenterNodes, consenterInfos, shards, genesisBlock)

	batchers, batchersConfigs, batchersLoggers, _ := test_utils.CreateBatchersForShard(t, numParties, batcherNodes, shards, consenterInfos, shards[0].ShardId, genesisBlock)

	routers, certs, routersConfigs, routersLoggers := test_utils.CreateRouters(t, numParties, batcherInfos, ca, shards[0].ShardId, []string{consenterNodes[0].Address(), consenterNodes[1].Address(), consenterNodes[2].Address(), consenterNodes[3].Address()}, genesisBlock)

	for i := range routers {
		routers[i].StartRouterService()
	}

	assemblers, assemblersDir, assemblersConfigs, assemblersLoggers, _ := test_utils.CreateAssemblers(t, numParties, ca, shards, consenterInfos, genesisBlock)

	// update consensus router config
	for i := range consenters {
		consenters[i].Config.Router = config.RouterInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   routers[i].Address(),
			TLSCACerts: nil,
			TLSCert:    certs[i],
		}
	}

	// update mock config update proposer
	payloadBytes := []byte{1}
	for i := range consenters {
		configRequestEnvelope := tx.CreateStructuredConfigEnvelope(payloadBytes)
		configRequest := &protos.Request{
			Payload:   configRequestEnvelope.Payload,
			Signature: configRequestEnvelope.Signature,
		}
		mockConfigUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
		mockConfigUpdateProposer.ProposeConfigUpdateReturns(configRequest, nil)
		consenters[i].ConfigUpdateProposer = mockConfigUpdateProposer
		mockConfigRequestValidator := &mocks.FakeConfigRequestValidator{}
		mockConfigRequestValidator.ValidateConfigRequestReturns(nil)
		consenters[i].ConfigRequestValidator = mockConfigRequestValidator
		mockConfigRulesVerifier := &ordererRulesMocks.FakeOrdererRules{}
		mockConfigRulesVerifier.ValidateNewConfigReturns(nil)
		mockConfigRulesVerifier.ValidateTransitionReturns(nil)
		consenters[i].ConfigRulesVerifier = mockConfigRulesVerifier
	}

	// submit data txs and make sure the assembler got them
	test_utils.SendTransactions(t, routers, assemblers[0])

	// check the init size of the config store
	for i := range routers {
		require.Equal(t, 1, routers[i].GetConfigStoreSize())
		numbers, err := batchers[i].ConfigStore.ListBlockNumbers()
		require.NoError(t, err)
		require.Len(t, numbers, 1)
	}

	// create a config request and submit
	for i := range routers {
		configReq := tx.CreateStructuredConfigUpdateRequest(payloadBytes)
		routers[i].Submit(context.Background(), configReq)
	}

	// check config store size in routers and batchers
	for i := range routers {
		require.Eventually(t, func() bool {
			routerConfigCount := routers[i].GetConfigStoreSize()

			batcherConfigCount, err := batchers[i].ConfigStore.ListBlockNumbers()
			require.NoError(t, err)

			return routerConfigCount == 2 && len(batcherConfigCount) == 2
		}, 10*time.Second, 100*time.Millisecond)
	}

	// make sure router said it is stopping
	req := tx.CreateStructuredRequest([]byte{2})
	require.Eventually(t, func() bool {
		resp, err := routers[0].Submit(context.Background(), req)
		return err == nil && strings.Contains(resp.Error, "router is stopping, cannot process request")
	}, 10*time.Second, 100*time.Millisecond)

	// make sure batcher said it is stopping
	batchers[0].Submit(context.Background(), req)
	require.Eventually(t, func() bool {
		_, err := batchers[0].Submit(context.Background(), req)
		return err != nil && strings.Contains(err.Error(), "batcher is stopped")
	}, 10*time.Second, 100*time.Millisecond)

	// make sure consenter said it is stopping
	require.Eventually(t, func() bool {
		baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{2}, types.PartyID(1), 0, 0)
		ce := &state.ControlEvent{BAF: baf}
		err := consenters[0].SubmitRequest(ce.Bytes())
		return err != nil && strings.Contains(err.Error(), "consensus is soft-stopped")
	}, 10*time.Second, 100*time.Millisecond)

	// check all assemblers appended to the ledger a config block
	time.Sleep(1 * time.Second)
	var lastBlock *common.Block
	for i := range assemblers {
		assemblers[i].Stop()

		al, err := ledger.NewAssemblerLedger(assemblersLoggers[i], assemblersDir[i])
		require.NoError(t, err)

		h := al.LedgerReader().Height()
		require.GreaterOrEqual(t, h, uint64(3)) // genesis block + at least one data block + config block

		lastBlock, err = al.LedgerReader().RetrieveBlockByNumber(h - 1)
		require.NoError(t, err)
		require.True(t, protoutil.IsConfigBlock(lastBlock))

		al.Close()
	}

	// stop all nodes and recover them
	for i := 0; i < numParties; i++ {
		routers[i].Stop()
		batchers[i].Stop()
		consenters[i].Stop()
	}

	for i := 0; i < numParties; i++ {
		batchers[i] = test_utils.RecoverBatcher(t, ca, batchersConfigs[i], batcherNodes[i], batchersLoggers[i])
		consenters[i] = test_utils.RecoverConsenter(t, ca, consentersConfigs[i], consenterNodes[i], consentersLoggers[i], lastBlock)
		assemblers[i] = test_utils.RecoverAssembler(t, assemblersConfigs[i], assemblersLoggers[i], lastBlock)
		routers[i] = test_utils.RecoverRouter(routersConfigs[i], routersLoggers[i])
	}
	time.Sleep(5 * time.Second)

	// check router and batcher config store after recovery
	for i := range routers {
		routerConfigCount := routers[i].GetConfigStoreSize()
		require.Equal(t, routerConfigCount, 2)

		batcherConfigCount, err := batchers[i].ConfigStore.ListBlockNumbers()
		require.NoError(t, err)
		require.Len(t, batcherConfigCount, 2)
	}

	// submit data txs and make sure the assembler receives them after recovery
	test_utils.SendTransactions(t, routers, assemblers[0])

	// verify last block points to the last config block
	for i := range assemblers {
		assemblers[i].Stop()

		al, err := ledger.NewAssemblerLedger(assemblersLoggers[i], assemblersDir[i])
		require.NoError(t, err)

		lastConfigIndex, err := ledger.GetLastConfigIndexFromAssemblerLedger(al)
		require.NoError(t, err)

		require.Equal(t, lastBlock.Header.Number, lastConfigIndex)

		al.Close()
	}

	for i := 0; i < numParties; i++ {
		routers[i].Stop()
		batchers[i].Stop()
		consenters[i].Stop()
	}
}

// TestConfigTXDisseminationWithVerification verifies that a config tx modifying a SmartBFT parameter is properly disseminated across all nodes.
// All nodes are expected to receive the tx and append it to the respective config store (routers and batchers) and ledgers (consenters and assemblers),
// The network then undergoes a dynamic reconfiguration process after which it becomes available again.
func TestConfigTXDisseminationWithVerification(t *testing.T) {
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

	// Prepare a Config TX, i.e. an envelope signed by an admin of org1
	// the envelope.Payload contains marshaled bytes of configUpdateEnvelope, which is an envelope with Header.Type = HeaderType_CONFIG_UPDATE, signed by majority of admins

	// Create the config transaction
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	configUpdateBuilder := cfgutil.NewConfigUpdateBuilder(t, dir, genesisBlockPath)

	requestBatchMaxBytes := uint64(1048576)
	configUpdatePbData := configUpdateBuilder.UpdateSmartBFTConfig(t, cfgutil.NewSmartBFTConfig(cfgutil.SmartBFTConfigName.RequestBatchMaxBytes, strconv.FormatUint(requestBatchMaxBytes, 10)))
	require.NotEmpty(t, configUpdatePbData)

	env := cfgutil.CreateConfigTX(t, dir, []types.PartyID{1, 2, 3, 4}, submittingParty, configUpdatePbData)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTx(env)
	require.NoError(t, err)

	testutil.WaitForNetworkRelaunch(t, netInfo, 1)

	// Pull from assembler
	parties := []types.PartyID{}
	for partyID := 1; partyID <= numOfParties; partyID++ {
		parties = append(parties, types.PartyID(partyID))
	}

	startBlock := uint64(0)
	endBlock := uint64(1)
	pullRequestSigner := signutil.CreateTestSigner(t, "org1", dir)

	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: startBlock,
		EndBlock:   endBlock,
		Blocks:     2,
		ErrString:  "cancelled pull from assembler: %d",
		Signer:     pullRequestSigner,
	})

	// Check config store size of routers
	for i := 0; i < numOfParties; i++ {
		localConfigPath := armaNetwork.GetRouter(t, types.PartyID(i+1)).RunInfo.NodeConfigPath
		localConfig := testutil.ReadNodeConfigFromYaml(t, localConfigPath)
		require.Eventually(t, func() bool {
			configStore, err := configstore.NewStore(localConfig.FileStore.Path)
			require.NoError(t, err)
			listBlockNumbers, err := configStore.ListBlockNumbers()
			require.NoError(t, err)
			routerConfigCount := len(listBlockNumbers)
			return routerConfigCount == 2
		}, 60*time.Second, 100*time.Millisecond)
	}

	// Check config store size of batchers
	for i := 0; i < numOfParties; i++ {
		for j := 0; j < numOfShards; j++ {
			batcher := armaNetwork.GetBatcher(t, types.PartyID(i+1), types.ShardID(j+1))
			localConfigPath := batcher.RunInfo.NodeConfigPath
			localConfig := testutil.ReadNodeConfigFromYaml(t, localConfigPath)
			require.Eventually(t, func() bool {
				configStore, err := configstore.NewStore(localConfig.FileStore.Path)
				require.NoError(t, err)
				listBlockNumbers, err := configStore.ListBlockNumbers()
				require.NoError(t, err)
				batcherConfigCount := len(listBlockNumbers)
				return batcherConfigCount == 2
			}, 60*time.Second, 100*time.Millisecond)
		}
	}

	// Initialize a new broadcast client
	broadcastClient2 := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient2.Stop()

	// Send a data tx that is not well signed, i.e. signature did not satisfy the policy /Channel/Writers
	txContent := tx.PrepareTxWithTimestamp(2, 64, []byte("dataTX"))
	env = tx.CreateStructuredEnvelope(txContent)
	err = broadcastClient2.SendTx(env)
	require.Error(t, err)
	require.ErrorContains(t, err, "INTERNAL_SERVER_ERROR, Info: request structure verification error: signature did not satisfy policy /Channel/Writers")

	// Send one more well signed data tx
	env = tx.CreateSignedStructuredEnvelope(txContent, signer, certBytes, fmt.Sprintf("org%d", submittingParty))
	err = broadcastClient2.SendTx(env)
	require.NoError(t, err)

	test_utils.PullFromAssemblers(t, &test_utils.BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: startBlock,
		EndBlock:   uint64(2),
		Blocks:     3,
		ErrString:  "cancelled pull from assembler: %d",
		Signer:     pullRequestSigner,
	})

	// Check that router and batcher config store keep the same size
	// Check config store size of routers
	for i := 0; i < numOfParties; i++ {
		localConfigPath := armaNetwork.GetRouter(t, types.PartyID(i+1)).RunInfo.NodeConfigPath
		localConfig := testutil.ReadNodeConfigFromYaml(t, localConfigPath)
		configStore, err := configstore.NewStore(localConfig.FileStore.Path)
		require.NoError(t, err)
		listBlockNumbers, err := configStore.ListBlockNumbers()
		require.NoError(t, err)
		routerConfigCount := len(listBlockNumbers)
		require.Equal(t, routerConfigCount, 2)
	}

	// Check config store size of batchers
	for i := 0; i < numOfParties; i++ {
		for j := 0; j < numOfShards; j++ {
			batcher := armaNetwork.GetBatcher(t, types.PartyID(i+1), types.ShardID(j+1))
			localConfigPath := batcher.RunInfo.NodeConfigPath
			localConfig := testutil.ReadNodeConfigFromYaml(t, localConfigPath)
			configStore, err := configstore.NewStore(localConfig.FileStore.Path)
			require.NoError(t, err)
			listBlockNumbers, err := configStore.ListBlockNumbers()
			require.NoError(t, err)
			batcherConfigCount := len(listBlockNumbers)
			require.Equal(t, batcherConfigCount, 2)
		}
	}

	armaNetwork.Stop()

	// Verify last block in assembler ledger points to the last config block
	for i := 0; i < numOfParties; i++ {
		assemblerNode := armaNetwork.GetAssembler(t, types.PartyID(i+1))
		localConfigPath := assemblerNode.RunInfo.NodeConfigPath
		localConfig := testutil.ReadNodeConfigFromYaml(t, localConfigPath)
		logger := flogging.MustGetLogger("assembler")
		al, err := ledger.NewAssemblerLedger(logger, localConfig.FileStore.Path)
		require.NoError(t, err)

		ledgerHeight := al.LedgerReader().Height()
		require.Equal(t, ledgerHeight, uint64(3))
		lastBlock, err := al.LedgerReader().RetrieveBlockByNumber(ledgerHeight - 1)
		require.NoError(t, err)
		require.False(t, protoutil.IsConfigBlock(lastBlock))

		lastConfigIndex, err := ledger.GetLastConfigIndexFromAssemblerLedger(al)
		require.NoError(t, err)

		require.Equal(t, uint64(1), lastConfigIndex)

		al.Close()
	}

	// Verify last block in consensus ledger points to the last config block
	for i := 0; i < numOfParties; i++ {
		consenterNode := armaNetwork.GetConsenter(t, types.PartyID(i+1))
		localConfigPath := consenterNode.RunInfo.NodeConfigPath
		localConfig := testutil.ReadNodeConfigFromYaml(t, localConfigPath)
		logger := flogging.MustGetLogger("consensus")
		consensusLedger, err := ledger.NewConsensusLedger(localConfig.FileStore.Path)
		require.NoError(t, err)

		lastConfigBlock, err := fabricx_config.GetLastConfigBlockFromConsensusLedger(consensusLedger, logger)
		require.NoError(t, err)

		require.Equal(t, uint64(1), lastConfigBlock.Header.Number)

		consensusLedger.Close()
	}
}

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
