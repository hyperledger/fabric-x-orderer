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
	"testing"
	"time"

	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/common/tools/armageddon"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/common/utils"
	"github.ibm.com/decentralized-trust-research/arma/config"
	genconfig "github.ibm.com/decentralized-trust-research/arma/config/generate"
	nodeconfig "github.ibm.com/decentralized-trust-research/arma/node/config"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

const (
	Router    string = "router"
	Batcher   string = "batcher"
	Consensus string = "consensus"
	Assembler string = "assembler"
)

// EditDirectoryInNodeConfigYAML fill the Directory field in all relevant config structures. This must be done before running Arma nodes
func EditDirectoryInNodeConfigYAML(t *testing.T, path string, storagePath string) {
	nodeConfig := readNodeConfigFromYaml(t, path)
	nodeConfig.FileStore.Path = storagePath
	err := nodeconfig.NodeConfigToYAML(nodeConfig, path)
	require.NoError(t, err)
}

func readNodeConfigFromYaml(t *testing.T, path string) *config.NodeLocalConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	config := config.NodeLocalConfig{}
	err = yaml.Unmarshal(configBytes, &config)
	require.NoError(t, err)
	return &config
}

// CreateNetwork creates a config.yaml file with the network configuration. This file is the input for armageddon generate command.
func CreateNetwork(t *testing.T, path string, numOfParties int, numOfBatcherShards int, useTLSRouter string, useTLSAssembler string) map[string]*ArmaNodeInfo {
	var parties []genconfig.Party
	netInfo := make(map[string]*ArmaNodeInfo)
	for i := 0; i < numOfParties; i++ {
		assemblerPort, lla := GetAvailablePort(t)
		consenterPort, llc := GetAvailablePort(t)
		routerPort, llr := GetAvailablePort(t)
		var llbs []net.Listener
		var batchersEndpoints []string

		for n := 0; n < numOfBatcherShards; n++ {
			batcherPort, llb := GetAvailablePort(t)
			llbs = append(llbs, llb)
			batchersEndpoints = append(batchersEndpoints, "127.0.0.1:"+batcherPort)
		}

		party := genconfig.Party{
			ID:                types.PartyID(i + 1),
			AssemblerEndpoint: "127.0.0.1:" + assemblerPort,
			ConsenterEndpoint: "127.0.0.1:" + consenterPort,
			RouterEndpoint:    "127.0.0.1:" + routerPort,
			BatchersEndpoints: batchersEndpoints,
		}

		parties = append(parties, party)

		nodeName := fmt.Sprintf("Party%drouter", i+1)
		netInfo[nodeName] = &ArmaNodeInfo{Listener: llr, NodeType: Router, PartyId: types.PartyID(i + 1)}

		for j, b := range llbs {
			nodeName = fmt.Sprintf("Party%dbatcher%d", i+1, j+1)
			netInfo[nodeName] = &ArmaNodeInfo{Listener: b, NodeType: Batcher, PartyId: types.PartyID(i + 1), ShardId: types.ShardID(j + 1)}
		}

		nodeName = fmt.Sprintf("Party%dconsensus", i+1)
		netInfo[nodeName] = &ArmaNodeInfo{Listener: llc, NodeType: Consensus, PartyId: types.PartyID(i + 1)}

		nodeName = fmt.Sprintf("Party%dassembler", i+1)
		netInfo[nodeName] = &ArmaNodeInfo{Listener: lla, NodeType: Assembler, PartyId: types.PartyID(i + 1)}
	}

	network := genconfig.Network{
		Parties:         parties,
		UseTLSRouter:    useTLSRouter,
		UseTLSAssembler: useTLSAssembler,
	}

	err := utils.WriteToYAML(network, path)
	require.NoError(t, err)

	return netInfo
}

// PrepareSharedConfigBinary generates a shared configuration and writes the encoded configuration to a file.
// The function return the path to the file and the shared config in the yaml format.
// This function is used in testing only.
func PrepareSharedConfigBinary(t *testing.T, dir string) (*config.SharedConfigYaml, string) {
	networkConfig := GenerateNetworkConfig(t, "none", "none")
	err := armageddon.GenerateCryptoConfig(&networkConfig, dir)
	require.NoError(t, err)

	networkLocalConfig, err := genconfig.CreateArmaLocalConfig(networkConfig, dir, dir)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)

	// 3.
	networkSharedConfig, err := genconfig.CreateArmaSharedConfig(networkConfig, networkLocalConfig, dir, dir)
	require.NoError(t, err)
	require.NotNil(t, networkSharedConfig)

	sharedConfig, err := config.LoadSharedConfig(filepath.Join(dir, "bootstrap", "shared_config.yaml"))
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

func runNode(t *testing.T, name string, armaBinaryPath string, nodeConfigPath string, readyChan chan struct{}, listener net.Listener) *gexec.Session {
	listener.Close()
	cmd := exec.Command(armaBinaryPath, name, "--config", nodeConfigPath)
	require.NotNil(t, cmd)

	sess, err := gexec.Start(cmd, os.Stdout, os.Stderr)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		match, err := gbytes.Say("listening on").Match(sess.Err)
		require.NoError(t, err)
		return match
	}, 60*time.Second, 10*time.Millisecond)

	readyChan <- struct{}{}
	return sess
}

func RunArmaNodes(t *testing.T, dir string, armaBinaryPath string, readyChan chan struct{}, netInfo map[string]*ArmaNodeInfo) *ArmaNetwork {
	nodes := map[string]string{
		Router:    "local_config_router",
		Batcher:   "local_config_batcher",
		Consensus: "local_config_consenter",
		Assembler: "local_config_assembler",
	}

	nodeNames := make([]string, 0, len(netInfo))
	for n := range netInfo {
		nodeNames = append(nodeNames, n)
	}

	sort.Strings(nodeNames)

	armaNetwork := ArmaNetwork{
		armaNodes: map[string][][]*ArmaNodeInfo{
			Router:    {},
			Batcher:   {},
			Consensus: {},
			Assembler: {},
		},
	}

	for _, nodeName := range nodeNames {
		netNode := netInfo[nodeName]

		shardId := ""
		if netNode.ShardId != 0 {
			shardId = strconv.FormatUint(uint64(netNode.ShardId), 10)
		}

		partyId := fmt.Sprintf("party%d", netNode.PartyId)

		partyDir := path.Join(dir, "config", partyId)
		nodeConfigPath := path.Join(partyDir, nodes[netNode.NodeType]+shardId+".yaml")

		storagePath := path.Join(dir, "storage", partyId, netNode.NodeType+shardId)
		err := os.MkdirAll(storagePath, 0o755)
		require.NoError(t, err)

		EditDirectoryInNodeConfigYAML(t, nodeConfigPath, storagePath)
		sess := runNode(t, netNode.NodeType, armaBinaryPath, nodeConfigPath, readyChan, netNode.Listener)
		netNode.RunInfo = &ArmaNodeRunInfo{Session: sess, ArmaBinaryPath: armaBinaryPath, NodeConfigPath: nodeConfigPath}
		armaNetwork.AddArmaNode(netNode.NodeType, int(netNode.PartyId)-1, netNode)
	}

	return &armaNetwork
}

func WaitReady(t *testing.T, readyChan chan struct{}, waitFor int, duration time.Duration) {
	startTimeout := time.After(duration * time.Second)
	for i := 0; i < waitFor; i++ {
		select {
		case <-readyChan:
		case <-startTimeout:
			require.Fail(t, "arma nodes failed to start in time")
		}
	}
}
