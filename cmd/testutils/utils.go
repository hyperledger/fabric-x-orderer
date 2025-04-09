package testutils

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/cmd/armageddon"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/common/utils"
	"github.ibm.com/decentralized-trust-research/arma/config"
	genconfig "github.ibm.com/decentralized-trust-research/arma/config/generate"
	nodeconfig "github.ibm.com/decentralized-trust-research/arma/node/config"
	"github.ibm.com/decentralized-trust-research/arma/testutil"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
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
func CreateNetwork(t *testing.T, path string, numOfParties int, useTLSRouter string, useTLSAssembler string) map[string]net.Listener {
	var parties []genconfig.Party
	listeners := make(map[string]net.Listener)
	for i := 0; i < numOfParties; i++ {
		assemblerPort, lla := testutil.GetAvailablePort(t)
		consenterPort, llc := testutil.GetAvailablePort(t)
		routerPort, llr := testutil.GetAvailablePort(t)
		batcher1Port, llb1 := testutil.GetAvailablePort(t)
		batcher2Port, llb2 := testutil.GetAvailablePort(t)

		party := genconfig.Party{
			ID:                types.PartyID(i + 1),
			AssemblerEndpoint: "127.0.0.1:" + assemblerPort,
			ConsenterEndpoint: "127.0.0.1:" + consenterPort,
			RouterEndpoint:    "127.0.0.1:" + routerPort,
			BatchersEndpoints: []string{"127.0.0.1:" + batcher1Port, "127.0.0.1:" + batcher2Port},
		}

		parties = append(parties, party)
		listeners[fmt.Sprintf("Party%drouter", i+1)] = llr
		listeners[fmt.Sprintf("Party%dbatcher1", i+1)] = llb1
		listeners[fmt.Sprintf("Party%dbatcher2", i+1)] = llb2
		listeners[fmt.Sprintf("Party%dconsensus", i+1)] = llc
		listeners[fmt.Sprintf("Party%dassembler", i+1)] = lla
	}

	network := genconfig.Network{
		Parties:         parties,
		UseTLSRouter:    useTLSRouter,
		UseTLSAssembler: useTLSAssembler,
	}

	err := utils.WriteToYAML(network, path)
	require.NoError(t, err)

	return listeners
}

// PrepareSharedConfigBinary generates a shared configuration and writes the encoded configuration to a file.
// The function return the path to the file and the shared config in the yaml format.
// This function is used in testing only.
func PrepareSharedConfigBinary(t *testing.T, dir string) (*config.SharedConfigYaml, string) {
	networkConfig := testutil.GenerateNetworkConfig(t, "none", "none")
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

func RunArmaNodes(t *testing.T, dir string, armaBinaryPath string, readyChan chan struct{}, listeners map[string]net.Listener) []*gexec.Session {
	nodes := map[string][]string{
		"router":    {"local_config_router.yaml"},
		"batcher":   {"local_config_batcher1.yaml", "local_config_batcher2.yaml"},
		"consensus": {"local_config_consenter.yaml"},
		"assembler": {"local_config_assembler.yaml"},
	}

	var sessions []*gexec.Session
	for _, nodeType := range []string{"consensus", "batcher", "router", "assembler"} {
		for i := 0; i < 4; i++ {
			partyDir := path.Join(dir, "config", fmt.Sprintf("party%d", i+1))
			for j := 0; j < len(nodes[nodeType]); j++ {
				nodeConfigPath := path.Join(partyDir, nodes[nodeType][j])
				var nodeTypeL string
				if nodeType == "batcher" {
					nodeTypeL = fmt.Sprintf("batcher%d", j+1)
				} else {
					nodeTypeL = nodeType
				}

				storagePath := path.Join(dir, "storage", fmt.Sprintf("party%d", i+1), nodeTypeL)
				err := os.MkdirAll(storagePath, 0o755)
				require.NoError(t, err)

				EditDirectoryInNodeConfigYAML(t, nodeConfigPath, storagePath)

				listener := listeners[fmt.Sprintf("Party%d"+nodeTypeL, i+1)]
				sess := runNode(t, nodeType, armaBinaryPath, nodeConfigPath, readyChan, listener)
				sessions = append(sessions, sess)
			}
		}
	}
	return sessions
}
