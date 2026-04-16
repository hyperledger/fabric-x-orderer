/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/hyperledger/fabric-x-orderer/config/verify"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"

	"github.com/hyperledger/fabric-x-orderer/node/router"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	cfgutil "github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/hyperledger/fabric-x-orderer/testutil/stub"
	"go.uber.org/zap"

	"github.com/stretchr/testify/require"
)

// Scenario:
// 1. Start a router, stub batcher, and stub consenter with the initial config.
// 2. Wait for the router to be running with config sequence 0.
// 3. Build a valid config update that changes a batching timeout.
// 4. Deliver the config block through the consenter path.
// 5. Verify the router applies the new config and advances to config sequence 1.
func TestSendConfigUpdate(t *testing.T) {
	partyId := types.PartyID(1)
	parties := []types.PartyID{partyId}

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId)
	testSetup.Start()
	defer testSetup.Stop()

	// wait for the router to be running.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	// create the config request.
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()
	configUpdatePbData := configUpdateBuilder.UpdateBatchTimeouts(t, cfgutil.NewBatchTimeoutsConfig(cfgutil.BatchTimeoutsConfigName.AutoRemoveTimeout, "15ms"))
	require.NotNil(t, configUpdatePbData)
	testSetup.SendConfigUpdate(t, parties, configUpdatePbData, dir, 1)

	// check that the router has applied the new config and is running with the new config
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 1
	}, 20*time.Second, 100*time.Millisecond)
}

// Scenario:
// 1. Start a router, stub batcher, and stub consenter with the initial config.
// 2. Wait for the router to be running with config sequence 0.
// 3. Build a config update that removes the router's party.
// 4. Deliver the config block through the consenter path.
// 5. Verify the router detects eviction and transitions to pending admin.
func TestPartyEvicted(t *testing.T) {
	partyId := types.PartyID(1)
	partyToRemove := partyId
	parties := []types.PartyID{partyId}

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId)
	testSetup.Start()
	defer testSetup.Stop()

	// wait for the router to be running.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	// create the config request.
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()
	configUpdatePbData := configUpdateBuilder.RemoveParty(t, partyToRemove)
	require.NotNil(t, configUpdatePbData)
	testSetup.SendConfigUpdate(t, parties, configUpdatePbData, dir, 1)

	// check that the router detected that it got evicted and is pending admin.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StatePendingAdmin && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)
}

// Scenario:
// 1. Start a router, stub batcher, and stub consenter with the initial config.
// 2. Wait for the router to be running with config sequence 0.
// 3. Build a config update that changes the router's endpoint.
// 4. Deliver the config block through the consenter path.
// 5. Verify the router detects endpoint change and transitions to pending admin.
func TestUpdateRouterEndpoint(t *testing.T) {
	partyId := types.PartyID(1)
	partyToUpdate := partyId
	parties := []types.PartyID{partyId}

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId)
	testSetup.Start()
	defer testSetup.Stop()

	// wait for the router to be running.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	// create the config request.
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()
	configUpdatePbData := configUpdateBuilder.UpdateRouterEndpoint(t, partyToUpdate, "9.9.9.9", 9999)
	require.NotNil(t, configUpdatePbData)
	testSetup.SendConfigUpdate(t, parties, configUpdatePbData, dir, 1)

	// check that the router detected that its endpoint got changed and is pending admin.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StatePendingAdmin && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)
}

// Scenario:
// 1. Start a router, stub batcher, and stub consenter with the initial config.
// 2. Wait for the router to be running with config sequence 0.
// 3. Generate a new TLS certificate for the router and build a config update that sets it.
// 4. Deliver the config block through the consenter path.
// 5. Verify the router detects certificate change and transitions to pending admin.
func TestUpdateRouterCert(t *testing.T) {
	partyId := types.PartyID(1)
	parties := []types.PartyID{partyId}

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId)
	testSetup.Start()
	defer testSetup.Stop()

	// wait for the router to be running.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	// create and deliver a config update that changes the router TLS certificate.
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()

	nodesIPs := testutil.GetNodesIPsFromNetInfo(testSetup.netInfo)
	require.NotNil(t, nodesIPs)

	tlsCACertPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyId), "tlsca", "tlsca-cert.pem")
	tlsCAPrivKeyPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyId), "tlsca", "priv_sk")
	newRouterTLSCertPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyId), "orderers", fmt.Sprintf("party%d", partyId), "router", "tls")
	newRouterTLSKeyPath := filepath.Join(newRouterTLSCertPath, "key.pem")

	newRouterTLSCert, err := armageddon.CreateNewCertificateFromCA(tlsCACertPath, tlsCAPrivKeyPath, "tls", newRouterTLSCertPath, newRouterTLSKeyPath, nodesIPs)
	require.NoError(t, err)
	require.NotNil(t, newRouterTLSCert)

	configUpdatePbData := configUpdateBuilder.UpdateRouterTLSCert(t, partyId, newRouterTLSCert)
	require.NotNil(t, configUpdatePbData)
	testSetup.SendConfigUpdate(t, parties, configUpdatePbData, dir, 1)

	// check that the router detected its cert changed and moved to pending admin.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StatePendingAdmin && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)
}

// Scenario:
// 1. Start a router, stub batcher, and stub consenter with the initial config.
// 2. Wait for the router to be running and connected to the batcher.
// 3. Submit a small request and verify success, then submit an oversized request and verify rejection.
// 4. Deliver a config block that increases RequestMaxBytes.
// 5. Wait for the router to apply the new config and reconnect.
// 6. Verify a request larger than the old limit succeeds, and one larger than the new limit is rejected.
func TestUpdateRouterMaxRequestBytes(t *testing.T) {
	partyId := types.PartyID(1)
	parties := []types.PartyID{partyId}

	dir := t.TempDir()
	testSetup := createReconfigTestSetup(t, dir, partyId)
	testSetup.Start()
	defer testSetup.Stop()

	// wait for the router to be running.
	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 0
	}, 20*time.Second, 100*time.Millisecond)

	clientConn, err := testSetup.CreateRouterClient()
	require.NoError(t, err)
	require.NotNil(t, clientConn)
	defer clientConn.Close()

	// make sure router is connected to the batcher before submitting requests
	require.Eventually(t, func() bool {
		return testSetup.routerNode.IsAllStreamsOK()
	}, 20*time.Second, 100*time.Millisecond)

	requestMaxBytes := int(testSetup.sharedConfig.GetBatchingConfig().GetRequestMaxBytes())
	err = submitRequestToRouter(clientConn, requestMaxBytes/2) // submit a request smaller than the current max size and expect success
	require.NoError(t, err)

	err = submitRequestToRouter(clientConn, 2*requestMaxBytes) // submit a request larger than the current max size and expect failure
	require.ErrorContains(t, err, "request verification error: the request's size exceeds the maximum size")

	// create and deliver a config update that increases RequestMaxBytes.
	configUpdateBuilder, cleanUp := cfgutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	defer cleanUp()
	configUpdatePbData := configUpdateBuilder.UpdateBatchRequestMaxBytes(t, 3*requestMaxBytes)
	require.NotNil(t, configUpdatePbData)

	testSetup.SendConfigUpdate(t, parties, configUpdatePbData, dir, 1)

	// check that the router has applied the new config and is running with the new config

	require.Eventually(t, func() bool {
		status := testSetup.routerNode.GetStatus()
		return status.State == node_utils.StateRunning && status.ConfigSequenceNumber == 1
	}, 20*time.Second, 100*time.Millisecond)

	// make sure router is connected to the batcher with the new config before submitting requests
	require.Eventually(t, func() bool {
		return testSetup.routerNode.IsAllStreamsOK()
	}, 20*time.Second, 100*time.Millisecond)

	// submit a request larger than the old max size but smaller than the new max size and expect success.
	err = submitRequestToRouter(clientConn, 2*requestMaxBytes)
	require.NoError(t, err)

	// submit a request larger than the new max size and expect failure.
	err = submitRequestToRouter(clientConn, 4*requestMaxBytes)
	require.ErrorContains(t, err, "request verification error: the request's size exceeds the maximum size")
}

type reconfigTestSetup struct {
	routerNode         *router.Router
	routerFileStore    string
	routerListener     net.Listener
	stubConsenter      *stub.StubConsenter
	consenterFileStore string
	stubBatcher        *stub.StubBatcher
	batcherFileStore   string
	genesisBlock       *common.Block
	bundle             channelconfig.Resources
	netInfo            map[testutil.NodeName]*testutil.ArmaNodeInfo

	userConfig   *armageddon.UserConfig
	sharedConfig *config_protos.SharedConfig
}

func (s *reconfigTestSetup) CreateRouterClient() (*grpc.ClientConn, error) {
	cc := comm.ClientConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:        true,
			ServerRootCAs: s.userConfig.TLSCACerts,
		},
		DialTimeout:  time.Second,
		AsyncConnect: false,
	}

	clientConn, err := cc.Dial(s.routerNode.Address())
	return clientConn, err
}

func submitRequestToRouter(clientConn *grpc.ClientConn, requestSize int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	buff := make([]byte, requestSize)
	binary.BigEndian.PutUint32(buff, uint32(12345))
	req := tx.CreateStructuredRequest(buff)
	cl := protos.NewRequestTransmitClient(clientConn)
	resp, err := cl.Submit(ctx, req)
	if err != nil {
		return fmt.Errorf("error receiving response: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("received response with error: %s", resp.Error)
	}

	return nil
}

func (s *reconfigTestSetup) Start() {
	s.stubConsenter.Start()
	s.stubBatcher.Start()
	if s.routerListener != nil {
		s.routerListener.Close()
	}
	s.routerNode.StartRouterService()
}

func (s *reconfigTestSetup) Stop() {
	s.routerNode.Stop()
	s.stubBatcher.Stop()
	s.stubConsenter.Stop()
}

func (s *reconfigTestSetup) SendConfigUpdate(t *testing.T, parties []types.PartyID, configUpdatePbData []byte, dir string, decisionNum types.DecisionNum) {
	configUpdateEnvelope := cfgutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
	configEnvelope, err := s.bundle.ConfigtxValidator().ProposeConfigUpdate(configUpdateEnvelope)
	require.NoError(t, err)
	env, err := protoutil.CreateSignedEnvelope(common.HeaderType_CONFIG, s.bundle.ConfigtxValidator().ChannelID(), nil, configEnvelope, int32(0), 0)
	require.NoError(t, err)
	require.NotNil(t, env)
	configReq, err := protoutil.Marshal(env)
	require.NoError(t, err)
	require.NotNil(t, configReq)

	prevHash := protoutil.BlockHeaderHash(s.genesisBlock.Header)
	configBlock, err := consensus.CreateConfigCommonBlock(s.genesisBlock.GetHeader().GetNumber()+1, prevHash, 1, decisionNum, 1, 0, configReq)
	require.NoError(t, err)
	shardID := types.ShardID(1)
	header := &state.Header{
		Num:                          decisionNum,
		DecisionNumOfLastConfigBlock: decisionNum,
		AvailableCommonBlocks:        []*common.Block{configBlock},
		State:                        &state.State{N: uint16(len(parties)), ShardCount: 1, Shards: []state.ShardTerm{{Shard: shardID, Term: 0}}},
	}
	err = s.stubConsenter.DeliverDecisionFromHeader(header)
	require.NoError(t, err)
}

func createReconfigTestSetup(t *testing.T, dir string, partyId types.PartyID) *reconfigTestSetup {
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 1, 1, "TLS", "none")
	require.NotNil(t, netInfo)
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	consenterFileStore := t.TempDir()
	consenterListener := netInfo[testutil.NodeName{PartyID: partyId, NodeType: testutil.Consensus}].Listener
	consenterNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyId), "local_config_consenter.yaml")
	stubConsenter := stub.NewStubConsenterFromConfig(t, consenterFileStore, consenterNodeConfigPath, consenterListener)

	batcherFileStore := t.TempDir()
	batcherListener := netInfo[testutil.NodeName{PartyID: partyId, NodeType: testutil.Batcher, ShardID: types.ShardID(1)}].Listener
	batcherNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyId), fmt.Sprintf("local_config_batcher%d.yaml", 1))
	stubBatcher := stub.NewStubBatcherFromConfig(t, batcherFileStore, batcherNodeConfigPath, batcherListener)

	routerFileStore := t.TempDir()
	routerListener := netInfo[testutil.NodeName{PartyID: partyId, NodeType: testutil.Router}].Listener
	routerNodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", partyId), "local_config_router.yaml")
	routerNode, genesisBlock, routerBundle := createRealRouterFromConfig(t, partyId, routerFileStore, routerNodeConfigPath)

	userConfig, err := testutil.GetUserConfig(dir, partyId)
	require.NoError(t, err)
	require.NotNil(t, userConfig)

	ordererConfig, ok := routerBundle.OrdererConfig()
	require.True(t, ok)
	sharedConfig := config_protos.SharedConfig{}
	err = proto.Unmarshal(ordererConfig.ConsensusMetadata(), &sharedConfig)
	require.NoError(t, err)

	return &reconfigTestSetup{
		stubConsenter:      stubConsenter,
		consenterFileStore: consenterFileStore,
		stubBatcher:        &stubBatcher,
		batcherFileStore:   batcherFileStore,
		routerNode:         routerNode,
		routerFileStore:    routerFileStore,
		routerListener:     routerListener,
		genesisBlock:       genesisBlock,
		bundle:             routerBundle,
		userConfig:         userConfig,
		sharedConfig:       &sharedConfig,
		netInfo:            netInfo,
	}
}

func createRealRouterFromConfig(t *testing.T, partyID types.PartyID, fileStoreDir string, nodeConfigPath string) (*router.Router, *common.Block, channelconfig.Resources) {
	if fileStoreDir != "" {
		localConfig, _, err := config.LoadLocalConfig(nodeConfigPath)
		require.NoError(t, err)
		localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
		err = utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)
		require.NoError(t, err)
	}

	config, lastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, fmt.Sprintf("ReadConfigRouter%d", partyID), zap.DebugLevel))
	require.NoError(t, err)
	routerConfig := config.ExtractRouterConfig(lastConfigBlock)
	require.NotNil(t, routerConfig)
	_, signer, err := testutil.BuildTestLocalMSP(config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID)
	require.NoError(t, err)
	require.NotNil(t, signer)
	routerLogger := testutil.CreateLogger(t, int(partyID))
	router := router.NewRouter(routerConfig, config, routerLogger, signer, make(chan struct{}), &policy.DefaultConfigUpdateProposer{}, &verify.DefaultOrdererRules{})
	return router, lastConfigBlock, routerConfig.Bundle
}
