/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	genconfig "github.com/hyperledger/fabric-x-orderer/config/generate"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type NodeType string

const (
	Router    NodeType = "router"
	Batcher   NodeType = "batcher"
	Consensus NodeType = "consensus"
	Assembler NodeType = "assembler"
)

func (n NodeType) String() string {
	return string(n)
}

// EditDirectoryInNodeConfigYAML modifies the node configuration YAML file with updated directory paths and monitoring settings.
// It reads the existing node configuration from the specified path, updates the file storage path,
// monitoring listen port, and optionally sets the bootstrap file path if provided.
// The modified configuration is then written back to the YAML file.
// Parameters:
//   - t: *testing.T for test assertions and error handling
//   - path: string representing the file path to the node configuration YAML file
//   - storagePath: string representing the new file storage directory path
//   - bootstrapFilePath: string representing the bootstrap file path (optional, only set if non-empty)
//   - monitoringListenPort: uint32 representing the monitoring server listen port
func EditDirectoryInNodeConfigYAML(t *testing.T, path string, storagePath string, bootstrapFilePath string, monitoringListenPort uint32) {
	nodeConfig := ReadNodeConfigFromYaml(t, path)
	nodeConfig.FileStore.Path = storagePath
	nodeConfig.GeneralConfig.MonitoringListenPort = monitoringListenPort
	if bootstrapFilePath != "" {
		nodeConfig.GeneralConfig.Bootstrap.File = bootstrapFilePath
	}
	err := nodeconfig.NodeConfigToYAML(nodeConfig, path)
	require.NoError(t, err)
}

// EditLocalMSPDirForNode overrides the local msp directory of node.
// This override is used in tests where nodes are running in the same process and a shared default BCCSP variable (global variable) is built.
// This variable holds the key store path which is the local msp path and is initialized once with the local msp of the first node.
// To avoid conflicts and access to wrong directories, we can override the local msp field to be the same.
func EditLocalMSPDirForNode(t *testing.T, path string, localMSPPath string) {
	nodeConfig := ReadNodeConfigFromYaml(t, path)
	nodeConfig.GeneralConfig.LocalMSPDir = localMSPPath
	err := nodeconfig.NodeConfigToYAML(nodeConfig, path)
	require.NoError(t, err)
}

func ReadNodeConfigFromYaml(t *testing.T, path string) *config.NodeLocalConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	config := config.NodeLocalConfig{}
	err = yaml.Unmarshal(configBytes, &config)
	require.NoError(t, err)
	return &config
}

var defaultPortAllocator PortAllocator = &DefaultPortAllocator{}

func allocatePort(t *testing.T, allocator PortAllocator) (string, net.Listener) {
	if allocator == nil {
		allocator = defaultPortAllocator
	}
	return allocator.Allocate(t)
}

// CreateNetwork creates a config.yaml file with the network configuration. This file is the input for armageddon generate command.
func CreateNetwork(t *testing.T, configPath string, numOfParties int, numOfBatcherShards int, useTLSRouter string, useTLSAssembler string) ArmaNodesInfoMap {
	return CreateNetworkWithPortAllocator(t, configPath, numOfParties, numOfBatcherShards, useTLSRouter, useTLSAssembler, SharedTestPortAllocator())
}

// CreateNetworkWithPortAllocator creates a config.yaml file with a caller-provided port allocator.
func CreateNetworkWithPortAllocator(t *testing.T, configPath string, numOfParties int, numOfBatcherShards int, useTLSRouter string, useTLSAssembler string, allocator PortAllocator) ArmaNodesInfoMap {
	var parties []genconfig.Party
	netInfo := *NewArmaNodesInfoMap()
	var maxPartyID types.PartyID

	for i := range numOfParties {
		assemblerPort, assemblerListener := allocatePort(t, allocator)
		_, assemblerMonitoringListener := allocatePort(t, allocator)
		consenterPort, consenterListener := allocatePort(t, allocator)
		_, consenterMonitoringListener := allocatePort(t, allocator)
		routerPort, routerListener := allocatePort(t, allocator)
		_, routerMonitoringListener := allocatePort(t, allocator)
		var batchersListenersInShard []net.Listener
		var batchersMonitoringListenersInShard []net.Listener
		var batchersEndpoints []string
		for range numOfBatcherShards {
			batcherPort, batcherListener := allocatePort(t, allocator)
			_, batcherMonitoringListener := allocatePort(t, allocator)
			batchersListenersInShard = append(batchersListenersInShard, batcherListener)
			batchersMonitoringListenersInShard = append(batchersMonitoringListenersInShard, batcherMonitoringListener)
			batchersEndpoints = append(batchersEndpoints, "127.0.0.1:"+batcherPort)
		}

		partyID := types.PartyID(i + 1)
		party := genconfig.Party{
			ID:                partyID,
			AssemblerEndpoint: "127.0.0.1:" + assemblerPort,
			ConsenterEndpoint: "127.0.0.1:" + consenterPort,
			RouterEndpoint:    "127.0.0.1:" + routerPort,
			BatchersEndpoints: batchersEndpoints,
		}

		parties = append(parties, party)

		if partyID > maxPartyID {
			maxPartyID = partyID
		}

		nodeName := NodeName{PartyID: types.PartyID(i + 1), NodeType: Router}
		netInfo[nodeName] = &ArmaNodeInfo{Listener: routerListener, NodeType: Router, PartyId: types.PartyID(i + 1), MonitoringListener: routerMonitoringListener}

		for j, b := range batchersListenersInShard {
			nodeName = NodeName{PartyID: types.PartyID(i + 1), NodeType: Batcher, ShardID: types.ShardID(j + 1)}
			netInfo[nodeName] = &ArmaNodeInfo{Listener: b, NodeType: Batcher, PartyId: types.PartyID(i + 1), ShardId: types.ShardID(j + 1), MonitoringListener: batchersMonitoringListenersInShard[j]}
		}

		nodeName = NodeName{PartyID: types.PartyID(i + 1), NodeType: Consensus}
		netInfo[nodeName] = &ArmaNodeInfo{Listener: consenterListener, NodeType: Consensus, PartyId: types.PartyID(i + 1), MonitoringListener: consenterMonitoringListener}

		nodeName = NodeName{PartyID: types.PartyID(i + 1), NodeType: Assembler}
		netInfo[nodeName] = &ArmaNodeInfo{Listener: assemblerListener, NodeType: Assembler, PartyId: types.PartyID(i + 1), MonitoringListener: assemblerMonitoringListener}
	}

	network := genconfig.Network{
		Parties:         parties,
		UseTLSRouter:    useTLSRouter,
		UseTLSAssembler: useTLSAssembler,
		MaxPartyID:      maxPartyID,
	}

	err := utils.WriteToYAML(network, configPath)
	require.NoError(t, err)

	return netInfo
}

type NodeName struct {
	PartyID  types.PartyID
	NodeType NodeType
	ShardID  types.ShardID
}

// ExtendNetwork extends an existing network configuration by adding a party to it.
// It updates the config file with the new party and returns the new party's information and config only
func ExtendNetwork(t *testing.T, configPath string) (ArmaNodesInfoMap, *genconfig.Network) {
	return ExtendNetworkWithPortAllocator(t, configPath, SharedTestPortAllocator())
}

// ExtendNetworkWithPortAllocator extends the network and uses a caller-provided allocator for new ports.
func ExtendNetworkWithPortAllocator(t *testing.T, configPath string, allocator PortAllocator) (ArmaNodesInfoMap, *genconfig.Network) {
	netInfo := *NewArmaNodesInfoMap()

	networkConfig := genconfig.Network{}
	err := utils.ReadFromYAML(&networkConfig, configPath)
	require.NoError(t, err, "failed to read network config file")

	networkConfig.MaxPartyID++

	numOfBatcherShards := len(networkConfig.Parties[0].BatchersEndpoints)

	assemblerPort, assemblerListener := allocatePort(t, allocator)
	_, assemblerMonitoringListener := allocatePort(t, allocator)
	consenterPort, consenterListener := allocatePort(t, allocator)
	_, consenterMonitoringListener := allocatePort(t, allocator)
	routerPort, routerListener := allocatePort(t, allocator)
	_, routerMonitoringListener := allocatePort(t, allocator)
	var batchersListenersInShard []net.Listener
	var batchersMonitoringListenersInShard []net.Listener
	var batchersEndpoints []string

	for range numOfBatcherShards {
		batcherPort, batcherListener := allocatePort(t, allocator)
		_, batcherMonitoringListener := allocatePort(t, allocator)
		batchersListenersInShard = append(batchersListenersInShard, batcherListener)
		batchersMonitoringListenersInShard = append(batchersMonitoringListenersInShard, batcherMonitoringListener)
		batchersEndpoints = append(batchersEndpoints, "127.0.0.1:"+batcherPort)
	}

	newPartyConfig := genconfig.Party{
		ID:                networkConfig.MaxPartyID,
		AssemblerEndpoint: "127.0.0.1:" + assemblerPort,
		ConsenterEndpoint: "127.0.0.1:" + consenterPort,
		RouterEndpoint:    "127.0.0.1:" + routerPort,
		BatchersEndpoints: batchersEndpoints,
	}

	networkConfig.Parties = append(networkConfig.Parties, newPartyConfig)

	nodeName := NodeName{PartyID: networkConfig.MaxPartyID, NodeType: Router}
	netInfo[nodeName] = &ArmaNodeInfo{Listener: routerListener, NodeType: Router, PartyId: types.PartyID(networkConfig.MaxPartyID), MonitoringListener: routerMonitoringListener}

	for j, b := range batchersListenersInShard {
		nodeName = NodeName{PartyID: networkConfig.MaxPartyID, NodeType: Batcher, ShardID: types.ShardID(j + 1)}
		netInfo[nodeName] = &ArmaNodeInfo{Listener: b, NodeType: Batcher, PartyId: types.PartyID(networkConfig.MaxPartyID), ShardId: types.ShardID(j + 1), MonitoringListener: batchersMonitoringListenersInShard[j]}
	}

	nodeName = NodeName{PartyID: networkConfig.MaxPartyID, NodeType: Consensus}
	netInfo[nodeName] = &ArmaNodeInfo{Listener: consenterListener, NodeType: Consensus, PartyId: types.PartyID(networkConfig.MaxPartyID), MonitoringListener: consenterMonitoringListener}
	nodeName = NodeName{PartyID: networkConfig.MaxPartyID, NodeType: Assembler}
	netInfo[nodeName] = &ArmaNodeInfo{Listener: assemblerListener, NodeType: Assembler, PartyId: types.PartyID(networkConfig.MaxPartyID), MonitoringListener: assemblerMonitoringListener}

	err = utils.WriteToYAML(networkConfig, configPath)
	require.NoError(t, err)

	return netInfo, &genconfig.Network{Parties: []genconfig.Party{newPartyConfig}, UseTLSRouter: networkConfig.UseTLSRouter, UseTLSAssembler: networkConfig.UseTLSAssembler}
}

// ExtendConfigAndCrypto generates crypto materials for the network, extends the config with the new party and writes the updated config to a file.
func ExtendConfigAndCrypto(networkConfig *genconfig.Network, outputDir string, clientSignatureVerificationRequired bool) {
	// generate crypto material
	err := armageddon.GenerateCryptoConfig(networkConfig, outputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating crypto config: %s", err)
		os.Exit(-1)
	}

	// generate local config yaml files
	networkLocalConfig, err := genconfig.CreateArmaLocalConfig(*networkConfig, outputDir, outputDir, clientSignatureVerificationRequired)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating local config: %s", err)
		os.Exit(-1)
	}

	// generate shared config yaml file
	_, err = genconfig.ExtendArmaSharedConfig(*networkConfig, networkLocalConfig, outputDir, outputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating shared config: %s", err)
		os.Exit(-1)
	}

	sharedConfig, _, err := config.LoadSharedConfig(filepath.Join(outputDir, "bootstrap", "shared_config.yaml"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading shared config: %s", err)
		os.Exit(-1)
	}

	// generate user config yaml file for each party
	// user will be able to connect to each of the routers and assemblers only if it receives for each router the CA that signed the certificate of that router.
	// therefore, the CA created per party must be collected for each party, to which the router is associated.
	var tlsCACertsBytesPartiesCollection [][]byte
	for _, party := range sharedConfig.PartiesConfig {
		tlsCACertsBytesPartiesCollection = append(tlsCACertsBytesPartiesCollection, party.TLSCACerts...)
	}

	for _, party := range networkConfig.Parties {
		userTLSPrivateKeyPath := filepath.Join(outputDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "users", "user", "tls", "user-key.pem")
		userTLSCertPath := filepath.Join(outputDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "users", "user", "tls", "user-tls-cert.pem")
		mspDir := filepath.Join(outputDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "users", "user", "msp")

		userConfig, err := armageddon.NewUserConfig(mspDir, userTLSPrivateKeyPath, userTLSCertPath, tlsCACertsBytesPartiesCollection, networkConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating user config: %s", err)
			os.Exit(-1)
		}

		err = utils.WriteToYAML(userConfig, filepath.Join(outputDir, "config", fmt.Sprintf("party%d", party.ID), "user_config.yaml"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error generating user config yaml: %s", err)
			os.Exit(-1)
		}
	}
}

// PrepareSharedConfigBinary generates a shared configuration and writes the encoded configuration to a file.
// The function returns the path to the file and the shared config in the yaml format.
// This function is used in testing only.
func PrepareSharedConfigBinary(t *testing.T, dir string) (*config.SharedConfigYaml, string) {
	networkConfig := GenerateNetworkConfig(t, "none", "none")
	return PrepareSharedConfigBinaryFromNetwork(t, networkConfig, dir)
}

// PrepareSharedConfigBinaryFromNetwork generates a shared configuration from a network definition and writes the encoded configuration to a file.
// The function returns the path to the file and the shared config in the yaml format.
// This function is used in testing only.
func PrepareSharedConfigBinaryFromNetwork(t *testing.T, networkConfig genconfig.Network, dir string) (*config.SharedConfigYaml, string) {
	err := armageddon.GenerateCryptoConfig(&networkConfig, dir)
	require.NoError(t, err)

	networkLocalConfig, err := genconfig.CreateArmaLocalConfig(networkConfig, dir, dir, false)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)

	// 3.
	networkSharedConfig, err := genconfig.CreateArmaSharedConfig(networkConfig, networkLocalConfig, dir, dir)
	require.NoError(t, err)
	require.NotNil(t, networkSharedConfig)

	sharedConfig, _, err := config.LoadSharedConfig(filepath.Join(dir, "bootstrap", "shared_config.yaml"))
	require.NoError(t, err)
	require.NotNil(t, sharedConfig)
	require.NotNil(t, sharedConfig.BatchingConfig)
	require.NotNil(t, sharedConfig.ConsensusConfig)
	require.NotNil(t, sharedConfig.PartiesConfig)
	require.Equal(t, len(sharedConfig.PartiesConfig), len(networkConfig.Parties))

	sharedConfigBytes, err := proto.Marshal(sharedConfig)
	require.NoError(t, err)
	sharedConfigPath := filepath.Join(dir, "bootstrap", "shared_config.bin")
	err = os.WriteFile(sharedConfigPath, sharedConfigBytes, 0o644)
	require.NoError(t, err)

	return networkSharedConfig, sharedConfigPath
}

func runNode(t *testing.T, node *ArmaNodeInfo, readyChan chan string) *gexec.Session {
	node.Close()
	cmd := exec.Command(node.RunInfo.ArmaBinaryPath, node.NodeType.String(), "--config", node.RunInfo.NodeConfigPath)
	require.NotNil(t, cmd)

	sess, err := gexec.Start(cmd, os.Stdout, os.Stderr)
	require.NoError(t, err)

	select {
	case <-time.After(60 * time.Second):
		require.Fail(t, fmt.Sprintf("Timed out waiting for Arma node %s_%d_%d to start", node.NodeType.String(), node.PartyId, node.ShardId))
	case <-sess.Err.Detect("panic"):
		readyChan <- fmt.Sprintf("%s_%d_%d_panic", node.NodeType.String(), node.PartyId, node.ShardId)
	case <-sess.Err.Detect("listening on"):
		readyChan <- fmt.Sprintf("%s_%d_%d_listening", node.NodeType.String(), node.PartyId, node.ShardId)
	}

	return sess
}

func RunArmaNodes(t *testing.T, dir string, armaBinaryPath string, readyChan chan string, netInfo map[NodeName]*ArmaNodeInfo) *ArmaNetwork {
	armaNetwork := ArmaNetwork{
		armaNodes: map[NodeType][][]*ArmaNodeInfo{
			Router:    {},
			Batcher:   {},
			Consensus: {},
			Assembler: {},
		},
	}

	armaNetwork.AddAndStartNodes(t, dir, armaBinaryPath, readyChan, netInfo)

	return &armaNetwork
}

func WaitReady(t *testing.T, readyChan chan string, waitFor int, duration time.Duration) {
	timeout := time.After(duration * time.Second)
	listening := []string{}

	for range waitFor {
		select {
		case msg := <-readyChan:
			if strings.Contains(msg, "listening") {
				listening = append(listening, msg)
			}
			if len(listening) == waitFor {
				return
			}
		case <-timeout:
			require.Fail(t, fmt.Sprintf("expected %d arma nodes to start successfully, but got %d listening logs: %v", waitFor, len(listening), listening))
		}
	}
}

func WaitPanic(t *testing.T, readyChan chan string, waitFor int, duration time.Duration) {
	timeout := time.After(duration * time.Second)
	panic := []string{}

	for {
		select {
		case msg := <-readyChan:
			if strings.Contains(msg, "panic") {
				panic = append(panic, msg)
			}
			if waitFor == len(panic) {
				return
			}
		case <-timeout:
			require.Fail(t, fmt.Sprintf("expected %d arma nodes to panic during startup, but got %d: %v", waitFor, len(panic), panic))
		}
	}
}

func WaitSoftStoppedByType(t *testing.T, netInfo map[NodeName]*ArmaNodeInfo, nodeTypes []NodeType) {
	// Propagate errors from worker goroutines via a channel so that
	// `require.Fail` is only invoked from the main goroutine.
	errCh := make(chan error, len(netInfo))
	done := make(chan struct{})

	go func() {
		defer close(done)
		var wg sync.WaitGroup

		for _, n := range netInfo {
			if !containsNodeType(nodeTypes, n.NodeType) {
				continue
			}
			wg.Add(1)
			go func(n *ArmaNodeInfo) {
				defer wg.Done()
				select {
				case <-n.RunInfo.Session.Err.Detect("Soft stop"):
					return
				case <-n.RunInfo.Session.Err.Detect("soft stop"):
					return
				case <-time.After(45 * time.Second):
					errCh <- fmt.Errorf("timed out waiting for Arma node %s_%d_%d to stop", n.NodeType.String(), n.PartyId, n.ShardId)
				}
			}(n)
		}

		wg.Wait()
	}()

	select {
	case <-done:
		select {
		case err := <-errCh:
			require.Fail(t, err.Error())
		default:
			return
		}
	case <-time.After(60 * time.Second):
		select {
		case err := <-errCh:
			require.Fail(t, err.Error())
		default:
			require.Fail(t, "Timed out waiting for Arma nodes to stop")
		}
	}
}

func WaitForRelaunchByType(t *testing.T, netInfo map[NodeName]*ArmaNodeInfo, nodeTypes []NodeType, configSeq uint64) {
	errCh := make(chan error, len(netInfo))
	done := make(chan struct{})

	go func() {
		defer close(done)
		var wg sync.WaitGroup

		for _, n := range netInfo {
			if !containsNodeType(nodeTypes, n.NodeType) {
				continue
			}
			wg.Add(1)
			go func(n *ArmaNodeInfo) {
				defer wg.Done()
				detectCh := n.RunInfo.Session.Err.Detect("started with new config sequence %d", configSeq)
				defer n.RunInfo.Session.Err.CancelDetects()
				select {
				case <-detectCh:
					return
				case <-time.After(120 * time.Second):
					errCh <- fmt.Errorf("timed out waiting for node %s_%d_%d to launch", n.NodeType.String(), n.PartyId, n.ShardId)
				}
			}(n)
		}

		wg.Wait()
	}()

	select {
	case <-done:
		select {
		case err := <-errCh:
			require.Fail(t, err.Error())
		default:
			return
		}
	case <-time.After(180 * time.Second):
		select {
		case err := <-errCh:
			require.Fail(t, err.Error())
		default:
			require.Fail(t, "Timed out waiting for required nodes: %v to launch", nodeTypes)
		}
	}
}

func WaitForNetworkRelaunch(t *testing.T, netInfo map[NodeName]*ArmaNodeInfo, configSeq uint64) {
	WaitForRelaunchByType(t, netInfo, []NodeType{Consensus, Assembler, Batcher, Router}, configSeq)

	time.Sleep(time.Minute) // wait for consenters.
}

// WaitForRelaunchByTypeAndParty waits for specific nodes to relaunch with a new configuration sequence.
// This function is used in tests to verify that nodes of specified types and parties have successfully
// restarted after a configuration update.
func WaitForRelaunchByTypeAndParty(t *testing.T, netInfo map[NodeName]*ArmaNodeInfo, nodeTypes []NodeType, parties []types.PartyID, configSeq uint64) {
	errCh := make(chan error, len(netInfo))
	done := make(chan struct{})

	go func() {
		defer close(done)
		var wg sync.WaitGroup

		for _, n := range netInfo {
			if !containsNodeType(nodeTypes, n.NodeType) {
				continue
			}
			if !containsParty(parties, n.PartyId) {
				continue
			}
			wg.Add(1)
			go func(n *ArmaNodeInfo) {
				defer wg.Done()
				detectCh := n.RunInfo.Session.Err.Detect("started with new config sequence %d", configSeq)
				defer n.RunInfo.Session.Err.CancelDetects()
				select {
				case <-detectCh:
					return
				case <-time.After(120 * time.Second):
					errCh <- fmt.Errorf("timed out waiting for node %s_%d_%d to launch", n.NodeType.String(), n.PartyId, n.ShardId)
				}
			}(n)
		}

		wg.Wait()
	}()

	select {
	case <-done:
		select {
		case err := <-errCh:
			require.Fail(t, err.Error())
		default:
			return
		}
	case <-time.After(180 * time.Second):
		select {
		case err := <-errCh:
			require.Fail(t, err.Error())
		default:
			require.Fail(t, "Timed out waiting for required nodes: %v from parties: %v to launch", nodeTypes, parties)
		}
	}
}

// WaitForPendingAdminByTypeAndParty waits for specific nodes to enter pending admin state.
// This function is used in tests to verify that nodes of specified types and parties have successfully
// entered pending admin state and wait for admin action.
func WaitForPendingAdminByTypeAndParty(t *testing.T, netInfo map[NodeName]*ArmaNodeInfo, nodeTypes []NodeType, parties []types.PartyID) {
	errCh := make(chan error, len(netInfo))
	done := make(chan struct{})

	go func() {
		defer close(done)
		var wg sync.WaitGroup

		for _, n := range netInfo {
			if !containsNodeType(nodeTypes, n.NodeType) {
				continue
			}
			if !containsParty(parties, n.PartyId) {
				continue
			}
			wg.Add(1)
			go func(n *ArmaNodeInfo) {
				defer wg.Done()
				detectCh := n.RunInfo.Session.Err.Detect("Pending admin action to apply new config")
				defer n.RunInfo.Session.Err.CancelDetects()
				select {
				case <-detectCh:
					return
				case <-time.After(120 * time.Second):
					errCh <- fmt.Errorf("timed out waiting for node %s_%d_%d to enter pending admin state", n.NodeType.String(), n.PartyId, n.ShardId)
				}
			}(n)
		}

		wg.Wait()
	}()

	select {
	case <-done:
		select {
		case err := <-errCh:
			require.Fail(t, err.Error())
		default:
			return
		}
	case <-time.After(180 * time.Second):
		select {
		case err := <-errCh:
			require.Fail(t, err.Error())
		default:
			require.Fail(t, "Timed out waiting for required nodes to enter pending admin state")
		}
	}
}

func containsNodeType(nodeTypes []NodeType, nodeType NodeType) bool {
	return slices.Contains(nodeTypes, nodeType)
}

func containsParty(parties []types.PartyID, nodeParty types.PartyID) bool {
	return slices.Contains(parties, nodeParty)
}

func WaitSoftStopped(t *testing.T, netInfo map[NodeName]*ArmaNodeInfo) {
	require.NotNil(t, netInfo)

	timeOut, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	g, ctx := errgroup.WithContext(timeOut)

	timeOutChan := make(chan *ArmaNodeInfo, len(netInfo))

	for _, n := range netInfo {
		require.NotNil(t, n.RunInfo, fmt.Sprintf("RunInfo is nil for node %s_%d_%d", n.NodeType.String(), n.PartyId, n.ShardId))
		ni := n
		g.Go(func() error {
			defer ni.RunInfo.Session.Err.CancelDetects() // ensure that detects are cancelled when done
			select {
			case <-ctx.Done():
				// return an error to indicate that the wait timed out
				return ctx.Err()
			case <-ni.RunInfo.Session.Err.Detect("Soft stop"):
			case <-ni.RunInfo.Session.Err.Detect("soft stop"):
			case <-time.After(120 * time.Second):
				timeOutChan <- ni
			}
			return nil
		})
	}

	// wait for all goroutines to finish and check if any of them returned an error (indicating a timeout)
	if err := g.Wait(); err != nil {
		require.Fail(t, fmt.Sprintf("Timed out waiting for Arma nodes to stop: %s", err))
	}
	close(timeOutChan)

	timedOutNodes := make([]string, 0, len(netInfo))
	for n := range timeOutChan {
		timedOutNodes = append(timedOutNodes, fmt.Sprintf("%s_%d_%d", n.NodeType.String(), n.PartyId, n.ShardId))
	}

	if len(timedOutNodes) > 0 {
		require.Fail(t, fmt.Sprintf("Arma nodes did not stop within the expected time: %s", strings.Join(timedOutNodes, ", ")))
	}
}

func sortArmaNodeInfo(infos []*ArmaNodeInfo) func(i, j int) bool {
	runningOrder := map[NodeType]int{Consensus: 1, Assembler: 2, Batcher: 3, Router: 4}

	return func(i, j int) bool {
		if infos[i].PartyId < infos[j].PartyId {
			return true
		}
		if infos[i].PartyId == infos[j].PartyId {
			if infos[i].NodeType == infos[j].NodeType {
				return infos[i].ShardId < infos[j].ShardId
			}
			return runningOrder[infos[i].NodeType] < runningOrder[infos[j].NodeType]
		}
		return false
	}
}

func LoadCryptoMaterialsFromDir(t *testing.T, mspDir string) (*crypto.ECDSASigner, []byte, error) {
	keyBytes, err := os.ReadFile(filepath.Join(mspDir, "keystore", "priv_sk"))
	require.NoError(t, err, "failed to read private key file")

	privateKey, err := tx.CreateECDSAPrivateKey(keyBytes)
	require.NoError(t, err, "failed to create private key")

	certBytes, err := os.ReadFile(filepath.Join(mspDir, "signcerts", "sign-cert.pem"))
	require.NoError(t, err, "failed to read sign certificate file")

	return (*crypto.ECDSASigner)(privateKey), certBytes, nil
}

func CreateAssemblerBundleForTest(sequence uint64) channelconfig.Resources {
	bundle := &configMocks.FakeConfigResources{}
	configtxValidator := &policyMocks.FakeConfigtxValidator{}
	configtxValidator.ChannelIDReturns("arma")
	configEnvelope := &common.ConfigEnvelope{
		Config:     nil,
		LastUpdate: nil,
	}
	configtxValidator.ProposeConfigUpdateReturns(configEnvelope, nil)
	configtxValidator.SequenceReturns(sequence)
	bundle.ConfigtxValidatorReturns(configtxValidator)

	policy := &policyMocks.FakePolicyEvaluator{}
	policy.EvaluateSignedDataReturns(nil)
	policyManager := &policyMocks.FakePolicyManager{}
	policyManager.GetPolicyReturns(policy, true)
	bundle.PolicyManagerReturns(policyManager)

	return bundle
}

func GetNodesIPsFromNetInfo(netInfo map[NodeName]*ArmaNodeInfo) []string {
	var ips []string
	for _, val := range netInfo {
		ips = append(ips, utils.TrimPortFromEndpoint(val.Listener.Addr().String()))
	}
	return ips
}

func StopAndRestartArmaNetwork(t *testing.T, armaNetwork *ArmaNetwork) {
	armaNetwork.Stop()
	totalNodes := armaNetwork.Len()
	require.NotZero(t, totalNodes, "expected at least one Arma node to restart")
	readyChan := make(chan string, totalNodes)
	armaNetwork.Restart(t, readyChan)
	WaitReady(t, readyChan, totalNodes, 10)
}
