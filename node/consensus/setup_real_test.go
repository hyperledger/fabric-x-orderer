/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	consensus_node "github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func createTestSetupReal(t *testing.T, dir string, parties []types.PartyID, numOfShards int) (*common.Block, consensusTestSetup) {
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetworkWithPortAllocator(t, configPath, len(parties), numOfShards, "TLS", "none", testutil.SharedTestPortAllocator())
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStoreAndMonitoringPort(t, dir, netInfo)

	netInfo.CleanUp()
	consensusNodes, servers, genesisBlock := createConsensusNodesAndGRPCServers(t, dir, parties)
	ledgerListeners := startConsensusNodesAndRegisterGRPCServers(t, parties, consensusNodes, servers)

	setup := consensusTestSetup{}
	setup.consensusNodes = append(setup.consensusNodes, consensusNodes...)
	setup.listeners = ledgerListeners
	setup.batcherNodes = createBatcherNodesInfo(t, dir, parties)
	return genesisBlock, setup
}

func createBatcherNodesInfo(t *testing.T, dir string, parties []types.PartyID) (batcherNodes []*node) {
	batcherNodes = make([]*node, 0, len(parties))

	t.Logf(">>> createBatcherNodesInfo dir: %s", dir)

	for _, i := range parties {

		nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_batcher1.yaml")
		t.Logf(">>> nodeConfigPath: %s", nodeConfigPath)
		configContent, lastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigBatcher", zap.DebugLevel))
		require.NoError(t, err)
		require.NotNil(t, lastConfigBlock)
		require.NotNil(t, configContent)
		batcherConfig := configContent.ExtractBatcherConfig(lastConfigBlock)
		require.NotNil(t, batcherConfig)
		require.NotNil(t, batcherConfig.SigningPrivateKey)

		batcherSK, err := tx.CreateECDSAPrivateKey(batcherConfig.SigningPrivateKey)
		require.NoError(t, err)
		batcherNodes = append(batcherNodes,
			&node{sk: batcherSK},
		)

	}

	return batcherNodes
}

func recoverNodeReal(t *testing.T, dir string, nodeIndex int, expectedConfigBlock *common.Block, setup consensusTestSetup) {
	recoveredConsensusNodes, recoveredServers, recoveredConfigBlock := createConsensusNodesAndGRPCServers(t, dir, []types.PartyID{types.PartyID(nodeIndex + 1)})
	require.Len(t, recoveredConsensusNodes, 1)
	require.Len(t, recoveredServers, 1)
	require.NotNil(t, recoveredConfigBlock)
	require.Equal(t, expectedConfigBlock.GetHeader().Number, recoveredConfigBlock.GetHeader().Number)

	recoveredLedgerListeners := startConsensusNodesAndRegisterGRPCServers(t, []types.PartyID{types.PartyID(nodeIndex + 1)}, recoveredConsensusNodes, recoveredServers)
	require.Len(t, recoveredLedgerListeners, 1)

	setup.consensusNodes[nodeIndex] = recoveredConsensusNodes[0]
	setup.listeners[nodeIndex] = recoveredLedgerListeners[0]
}

func createConsensusNodesAndGRPCServers(t *testing.T, dir string, parties []types.PartyID) ([]*consensus_node.Consensus, []*comm.GRPCServer, *common.Block) {
	t.Logf(">>> createConsensusNodesAndGRPCServers: %v", parties)
	consensusNodes := make([]*consensus_node.Consensus, 0, len(parties))
	servers := make([]*comm.GRPCServer, 0, len(parties))
	var lastConfigBlock *common.Block
	for _, i := range parties {
		t.Logf(">>> creating party: %d", i)
		nodeConfigPath := filepath.Join(dir, "config", fmt.Sprintf("party%d", i), "local_config_consenter.yaml")
		configContent, nodeLastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigConsensus", zap.DebugLevel))
		require.NoError(t, err)
		consenterConfig := configContent.ExtractConsenterConfig(nodeLastConfigBlock)
		require.NotNil(t, consenterConfig)
		_, signer, err := testutil.BuildTestLocalMSP(configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID)
		require.NoError(t, err)
		require.NotNil(t, signer)
		consenterLogger := testutil.CreateLogger(t, int(i))
		server := node_utils.CreateGRPCConsensus(consenterConfig)
		servers = append(servers, server)
		consensus := consensus_node.CreateConsensus(consenterConfig, configContent, nodeLastConfigBlock, consenterLogger, make(chan struct{}), signer, &policy.DefaultConfigUpdateProposer{})
		consensus.Net = server
		consensusNodes = append(consensusNodes, consensus)
		lastConfigBlock = nodeLastConfigBlock
		t.Logf(">>> created party: %+v", consensus.Config.PartyId)
	}
	return consensusNodes, servers, lastConfigBlock
}

func startConsensusNodesAndRegisterGRPCServers(t *testing.T, parties []types.PartyID, consensusNodes []*consensus_node.Consensus, servers []*comm.GRPCServer) []*storageListener {
	for i, consensusNode := range consensusNodes {
		protos.RegisterConsensusServer(servers[i].Server(), consensusNode)
		orderer.RegisterAtomicBroadcastServer(servers[i].Server(), consensusNode.DeliverService)
		orderer.RegisterClusterNodeServiceServer(servers[i].Server(), consensusNode)
		srv := servers[i]
		go func(s *comm.GRPCServer) {
			if err := s.Start(); err != nil {
				panic(fmt.Sprintf("failed to start gRPC server: %v", err))
			}
		}(srv)
		t.Logf(">>> startConsensusNodesAndRegisterGRPCServers started grpc server: party: %d at: %s", consensusNode.Config.PartyId, servers[i].Address())
	}

	ledgerListeners := make([]*storageListener, 0, len(parties))
	for _, consensusNode := range consensusNodes {
		if err := consensusNode.Start(); err != nil {
			panic(fmt.Sprintf("failed to start consensus node: %v", err))
		}
		ledgerListener := &storageListener{c: make(chan *common.Block, 100)}
		consensusNode.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(ledgerListener)
		ledgerListeners = append(ledgerListeners, ledgerListener)
		t.Logf(">>> startConsensusNodesAndRegisterGRPCServers registered ledger listener: party: %d", consensusNode.Config.PartyId)
	}

	time.Sleep(5 * time.Second)

	return ledgerListeners
}

func updateFileStoreAndMonitoringPort(t *testing.T, dir string, netInfo testutil.ArmaNodesInfoMap) {
	for _, nodeInfo := range netInfo {
		var nodeConfigPath string
		switch nodeInfo.NodeType {
		case testutil.Consensus:
			nodeConfigPath = filepath.Join(dir, "config", fmt.Sprintf("party%d", nodeInfo.PartyId), "local_config_consenter.yaml")
		case testutil.Batcher:
			nodeConfigPath = filepath.Join(dir, "config", fmt.Sprintf("party%d", nodeInfo.PartyId), fmt.Sprintf("local_config_batcher%d.yaml", nodeInfo.ShardId))
		default:
			continue
		}
		storagePath := t.TempDir()
		monitoringPort := uint32(nodeInfo.MonitoringListener.Addr().(*net.TCPAddr).Port)
		testutil.EditDirectoryInNodeConfigYAML(t, nodeConfigPath, storagePath, nodeInfo.ConfigBlockPath, monitoringPort)
	}
}
