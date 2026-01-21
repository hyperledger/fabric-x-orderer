/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
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

// EditDirectoryInNodeConfigYAML updates a node YAML config file at the given path.
// It sets the FileStore.Path to storagePath, clears the monitoring listen port
func EditDirectoryInNodeConfigYAML(t *testing.T, path string, storagePath string) {
	nodeConfig := ReadNodeConfigFromYaml(t, path)
	nodeConfig.FileStore.Path = storagePath
	nodeConfig.GeneralConfig.MonitoringListenPort = 0
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

// CreateNetwork creates a config.yaml file with the network configuration. This file is the input for armageddon generate command.
func CreateNetwork(t *testing.T, configPath string, numOfParties int, numOfBatcherShards int, useTLSRouter string, useTLSAssembler string) map[NodeName]*ArmaNodeInfo {
	var parties []genconfig.Party
	netInfo := make(map[NodeName]*ArmaNodeInfo)
	var maxPartyID types.PartyID

	for i := range numOfParties {
		assemblerPort, lla := GetAvailablePort(t)
		consenterPort, llc := GetAvailablePort(t)
		routerPort, llr := GetAvailablePort(t)
		var llbs []net.Listener
		var batchersEndpoints []string

		for range numOfBatcherShards {
			batcherPort, llb := GetAvailablePort(t)
			llbs = append(llbs, llb)
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
		netInfo[nodeName] = &ArmaNodeInfo{Listener: llr, NodeType: Router, PartyId: types.PartyID(i + 1)}

		for j, b := range llbs {
			nodeName = NodeName{PartyID: types.PartyID(i + 1), NodeType: Batcher, ShardID: types.ShardID(j + 1)}
			netInfo[nodeName] = &ArmaNodeInfo{Listener: b, NodeType: Batcher, PartyId: types.PartyID(i + 1), ShardId: types.ShardID(j + 1)}
		}

		nodeName = NodeName{PartyID: types.PartyID(i + 1), NodeType: Consensus}
		netInfo[nodeName] = &ArmaNodeInfo{Listener: llc, NodeType: Consensus, PartyId: types.PartyID(i + 1)}

		nodeName = NodeName{PartyID: types.PartyID(i + 1), NodeType: Assembler}
		netInfo[nodeName] = &ArmaNodeInfo{Listener: lla, NodeType: Assembler, PartyId: types.PartyID(i + 1)}
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

// PrepareSharedConfigBinary generates a shared configuration and writes the encoded configuration to a file.
// The function returns the path to the file and the shared config in the yaml format.
// This function is used in testing only.
func PrepareSharedConfigBinary(t *testing.T, dir string) (*config.SharedConfigYaml, string) {
	networkConfig := GenerateNetworkConfig(t, "none", "none")
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

func runNode(t *testing.T, name string, armaBinaryPath string, nodeConfigPath string, readyChan chan string, listener net.Listener) *gexec.Session {
	listener.Close()
	cmd := exec.Command(armaBinaryPath, name, "--config", nodeConfigPath)
	require.NotNil(t, cmd)

	sess, err := gexec.Start(cmd, os.Stdout, os.Stderr)
	require.NoError(t, err)

	select {
	case <-time.After(60 * time.Second):
		require.Fail(t, fmt.Sprintf("Timed out waiting for Arma node %s to start", name))
	case <-sess.Err.Detect("panic"):
		readyChan <- fmt.Sprintf("%s_panic", name)
	case <-sess.Err.Detect("listening on"):
		readyChan <- fmt.Sprintf("%s_listening", name)
	}

	return sess
}

func RunArmaNodes(t *testing.T, dir string, armaBinaryPath string, readyChan chan string, netInfo map[NodeName]*ArmaNodeInfo) *ArmaNetwork {
	nodes := map[NodeType]string{
		Router:    "local_config_router",
		Batcher:   "local_config_batcher",
		Consensus: "local_config_consenter",
		Assembler: "local_config_assembler",
	}

	nodeInfos := make([]*ArmaNodeInfo, 0, len(netInfo))
	numOfParties := 0
	for n := range netInfo {
		nodeInfos = append(nodeInfos, netInfo[n])
		if n.NodeType == Consensus {
			numOfParties++
		}
	}

	sort.Slice(nodeInfos, sortArmaNodeInfo(nodeInfos))

	armaNetwork := ArmaNetwork{
		armaNodes: map[NodeType][][]*ArmaNodeInfo{
			Router:    {},
			Batcher:   {},
			Consensus: {},
			Assembler: {},
		},
	}

	for _, netNode := range nodeInfos {
		shardId := ""
		if netNode.ShardId != 0 {
			shardId = strconv.FormatUint(uint64(netNode.ShardId), 10)
		}

		partyId := fmt.Sprintf("party%d", netNode.PartyId)

		partyDir := path.Join(dir, "config", partyId)
		nodeConfigPath := path.Join(partyDir, nodes[netNode.NodeType]+shardId+".yaml")

		storagePath := path.Join(dir, "storage", partyId, fmt.Sprintf("%s%s", netNode.NodeType.String(), shardId))
		err := os.MkdirAll(storagePath, 0o755)
		require.NoError(t, err)

		EditDirectoryInNodeConfigYAML(t, nodeConfigPath, storagePath)
		sess := runNode(t, netNode.NodeType.String(), armaBinaryPath, nodeConfigPath, readyChan, netNode.Listener)
		netNode.RunInfo = &ArmaNodeRunInfo{Session: sess, ArmaBinaryPath: armaBinaryPath, NodeConfigPath: nodeConfigPath}
		armaNetwork.AddArmaNode(netNode.NodeType, int(netNode.PartyId)-1, netNode)
	}

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
			require.Fail(t, fmt.Sprintf("expected %d arma nodes to start successfully, but got %d panics: %v", waitFor, len(listening), listening))
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

func WaitSoftStopped(t *testing.T, netInfo map[NodeName]*ArmaNodeInfo) {
	stopChan := make(chan struct{})

	go func() {
		defer close(stopChan)
		wg := sync.WaitGroup{}

		for _, n := range netInfo {
			wg.Go(func() {
				select {
				case <-n.RunInfo.Session.Err.Detect("Soft stop"):
				case <-n.RunInfo.Session.Err.Detect("soft stop"):
				case <-time.After(45 * time.Second):
					require.Fail(t, fmt.Sprintf("Timed out waiting for Arma node %s to stop", n.NodeType))
				}
			})
		}
		wg.Wait()
	}()

	select {
	case <-stopChan:
	case <-time.After(60 * time.Second):
		require.Fail(t, "Timed out waiting for Arma nodes to stop")
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
