/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/configrequest/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"

	"github.com/hyperledger/fabric-x-common/protoutil"
	configRulesMocks "github.com/hyperledger/fabric-x-orderer/common/configrulesverifier/mocks"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	fabricx_config "github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil/stub"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

// TestRouterSendConfigUpdateToConsenterStub tests the end-to-end flow of sending a configuration
// update transaction through the router to a consenter stub.
func TestRouterSendConfigUpdateToConsenterStub(t *testing.T) {
	// 1. Compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// 2. Create a temporary directory for the test.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	numOfParties := 1
	numOfShards := 1
	submittingParty := 1

	// 3. Create a config YAML file in the temporary directory.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "TLS", "none")
	require.NoError(t, err)

	// 4. Generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	configStoreDir := t.TempDir()
	defer os.RemoveAll(configStoreDir)

	// 5. Launch the batcher node stub
	batcher := stub.NewStubBatcherFromConfig(t, configStoreDir, filepath.Join(dir, "config", "party1", "local_config_batcher1.yaml"), netInfo["Party13batcher1"].Listener)
	batcher.Start()
	defer batcher.Stop()

	// 6. Launch the router node
	readyChan := make(chan struct{}, 1)

	routerNodeInfo := netInfo["Party14router"]
	routerNodeConfigPath := filepath.Join(dir, "config", "party1", "local_config_router.yaml")
	localConfig, _, err := fabricx_config.LoadLocalConfig(routerNodeConfigPath)
	require.NoError(t, err)

	localConfig.NodeLocalConfig.FileStore.Path = configStoreDir
	localConfig.NodeLocalConfig.GeneralConfig.ClientSignatureVerificationRequired = true
	utils.WriteToYAML(localConfig.NodeLocalConfig, routerNodeConfigPath)

	testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, map[string]*testutil.ArmaNodeInfo{"Party14router": routerNodeInfo})
	testutil.WaitReady(t, readyChan, 1, 10)

	// 7. Launch the consenter node stub
	consenterStub := stub.NewStubConsenterFromConfig(t, configStoreDir, filepath.Join(dir, "config", "party1", "local_config_consenter.yaml"), netInfo["Party11consensus"].Listener)
	consenterStub.Start()
	defer consenterStub.Stop()

	// 8. Create a broadcast client
	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	// 9. Prepare a Config TX, i.e. an envelope signed by an admin of org1
	// the envelope.Payload contains marshaled bytes of configUpdateEnvelope, which is an envelope with Header.Type = HeaderType_CONFIG_UPDATE, signed by majority of admins
	// Create the config transaction
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	env := configutil.CreateConfigTX(t, dir, numOfParties, genesisBlockPath, submittingParty)
	require.NotNil(t, env)

	// 10. Send the config tx
	err = broadcastClient.SendTx(env)
	require.ErrorContains(t, err, "INTERNAL_SERVER_ERROR, Info: dummy submit config")

	require.Eventually(t, func() bool {
		return consenterStub.ReceivedMessageCount() == 1
	}, 10*time.Second, 100*time.Millisecond)
}

func TestConfigDisseminate(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	numParties := 4

	batcherNodes, batcherInfos := createBatcherNodesAndInfo(t, ca, numParties)
	consenterNodes, consenterInfos := createConsenterNodesAndInfo(t, ca, numParties)

	shards := []config.ShardInfo{{ShardId: 1, Batchers: batcherInfos}}

	genesisBlock := utils.EmptyGenesisBlock("arma")

	consenters, consentersConfigs, consentersLoggers, _ := createConsenters(t, numParties, consenterNodes, consenterInfos, shards, genesisBlock)

	batchers, batchersConfigs, batchersLoggers, _ := createBatchersForShard(t, numParties, batcherNodes, shards, consenterInfos, shards[0].ShardId, genesisBlock)

	routers, certs, routersConfigs, routersLoggers := createRouters(t, numParties, batcherInfos, ca, shards[0].ShardId, consenterNodes[0].Address(), genesisBlock)

	for i := range routers {
		routers[i].StartRouterService()
	}

	assemblers, assemblersDir, assemblersConfigs, assemblersLoggers, _ := createAssemblers(t, numParties, ca, shards, consenterInfos, genesisBlock)

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
		mockConfigRulesVerifier := &configRulesMocks.FakeConfigRolesVerifier{}
		mockConfigRulesVerifier.ValidateNewConfigReturns(nil)
		consenters[i].ConfigRulesVerifier = mockConfigRulesVerifier
	}

	// submit data txs and make sure the assembler got them
	sendTransactions(t, routers, assemblers[0])

	// check the init size of the config store
	for i := range routers {
		require.Equal(t, 1, routers[i].GetConfigStoreSize())
		numbers, err := batchers[i].ConfigStore.ListBlockNumbers()
		require.NoError(t, err)
		require.Len(t, numbers, 1)
	}

	// create a config request and submit
	configReq := tx.CreateStructuredConfigUpdateRequest(payloadBytes)
	for i := range routers {
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
		require.NoError(t, err)
		return strings.Contains(resp.Error, "router is stopping, cannot process request")
	}, 10*time.Second, 100*time.Millisecond)

	// make sure batcher said it is stopping
	batchers[0].Submit(context.Background(), req)
	require.Eventually(t, func() bool {
		_, err := batchers[0].Submit(context.Background(), req)
		require.Error(t, err)
		return strings.Contains(err.Error(), "batcher is stopped")
	}, 10*time.Second, 100*time.Millisecond)

	// make sure consenter said it is stopping
	require.Eventually(t, func() bool {
		baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{2}, types.PartyID(1), 0)
		ce := &state.ControlEvent{BAF: baf}
		err := consenters[0].SubmitRequest(ce.Bytes())
		require.Error(t, err)
		return strings.Contains(err.Error(), "consensus is soft-stopped")
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
		batchers[i] = recoverBatcher(t, ca, batchersConfigs[i], batcherNodes[i], batchersLoggers[i])
		consenters[i] = recoverConsenter(t, ca, consentersConfigs[i], consenterNodes[i], consentersLoggers[i], lastBlock)
		assemblers[i] = recoverAssembler(t, assemblersConfigs[i], assemblersLoggers[i])
		routers[i] = recoverRouter(routersConfigs[i], routersLoggers[i])
	}

	// check router and batcher config store after recovery
	for i := range routers {
		routerConfigCount := routers[i].GetConfigStoreSize()
		require.Equal(t, routerConfigCount, 2)

		batcherConfigCount, err := batchers[i].ConfigStore.ListBlockNumbers()
		require.NoError(t, err)
		require.Len(t, batcherConfigCount, 2)
	}

	// submit data txs and make sure the assembler receives them after recovery
	sendTransactions(t, routers, assemblers[0])

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
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	// Run Arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	// Create a broadcast client
	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	// Prepare a Config TX, i.e. an envelope signed by an admin of org1
	// the envelope.Payload contains marshaled bytes of configUpdateEnvelope, which is an envelope with Header.Type = HeaderType_CONFIG_UPDATE, signed by majority of admins

	// Create the config transaction
	submittingParty := 1
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	env := configutil.CreateConfigTX(t, dir, numOfParties, genesisBlockPath, submittingParty)
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTx(env)
	require.NoError(t, err)

	// Pull from assembler
	parties := []types.PartyID{}
	for partyID := 1; partyID <= numOfParties; partyID++ {
		parties = append(parties, types.PartyID(partyID))
	}

	startBlock := uint64(0)
	endBlock := uint64(1)

	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: startBlock,
		EndBlock:   endBlock,
		Blocks:     2,
		ErrString:  "cancelled pull from assembler: %d",
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

	armaNetwork.Stop()

	// Check ledger height of consenters
	for i := 0; i < numOfParties; i++ {
		localConfigPath := armaNetwork.GetConsenter(t, types.PartyID(i+1)).RunInfo.NodeConfigPath
		localConfig := testutil.ReadNodeConfigFromYaml(t, localConfigPath)
		require.Eventually(t, func() bool {
			consensusLedger, err := ledger.NewConsensusLedger(localConfig.FileStore.Path)
			require.NoError(t, err)
			defer consensusLedger.Close()
			consensusLedgerCount := consensusLedger.Height()
			return consensusLedgerCount == 2
		}, 60*time.Second, 100*time.Millisecond)
	}
}
