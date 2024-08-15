package armageddon

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"
	"time"

	"arma/core"

	"arma/node/config"

	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// Scenario:
// 1. Create a config YAML file to be an input to armageddon
// 2. Run armageddon generate command to create config files in a folder structure
// 3. Run arma with the generated config files to run each of the nodes for all parties
// 4. Run armageddon submit command to make 1000 txs, send txs to all routers at a specified rate and pull blocks from some assembler to observe that txs appear in some block
func TestArmageddon(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	generateInputConfigFileForArmageddon(t, configPath)

	// 2.
	armageddon := NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("arma/node/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	runArmaNodes(t, dir, armaBinaryPath, readyChan)
	startTimeout := time.After(10 * time.Second)
	for i := 0; i < 20; i++ {
		select {
		case <-readyChan:
		case <-startTimeout:
			require.Fail(t, "arma nodes failed to start in time")
		}
	}

	// 4.
	userConfigPath := path.Join(dir, fmt.Sprintf("Party%d", 1), "user_config.yaml")
	rate := "500"
	txs := "1000"
	armageddon.Run([]string{"submit", "--config", userConfigPath, "--transactions", txs, "--rate", rate})
}

func runArmaNodes(t *testing.T, dir string, armaBinaryPath string, readyChan chan struct{}) {
	nodes := map[string][]string{
		"router":    {"router_node_config.yaml"},
		"batcher":   {"batcher_node_1_config.yaml", "batcher_node_2_config.yaml"},
		"consensus": {"consenter_node_config.yaml"},
		"assembler": {"assembler_node_config.yaml"},
	}

	for _, nodeType := range []string{"consensus", "batcher", "router", "assembler"} {
		for i := 0; i < 4; i++ {
			partyDir := path.Join(dir, fmt.Sprintf("Party%d", i+1))
			for j := 0; j < len(nodes[nodeType]); j++ {
				nodeConfigPath := path.Join(partyDir, nodes[nodeType][j])
				editDirectoryInNodeConfigYAML(t, nodeType, nodeConfigPath)
				runNode(t, nodeType, armaBinaryPath, nodeConfigPath, readyChan)
			}
		}
	}
}

func runNode(t *testing.T, name string, armaBinaryPath string, nodeConfigPath string, readyChan chan struct{}) {
	cmd := exec.Command(armaBinaryPath, name, "--config", nodeConfigPath)
	require.NotNil(t, cmd)

	sess, err := gexec.Start(cmd, os.Stdout, os.Stderr)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		match, err := gbytes.Say("listening on").Match(sess.Err)
		require.NoError(t, err)
		return match
	}, 10*time.Second, 10*time.Millisecond)

	readyChan <- struct{}{}
}

// generateInputConfigFileForArmageddon create a config.yaml file which is the input to armageddon generate command.
// the configuration includes 4 parties and 2 batchers for each party.
func generateInputConfigFileForArmageddon(t *testing.T, path string) {
	var parties []Party
	var listeners []net.Listener
	for i := 0; i < 4; i++ {
		assemblerPort, lla := getAvailablePort(t)
		consenterPort, llc := getAvailablePort(t)
		routerPort, llr := getAvailablePort(t)
		batcher1Port, llb1 := getAvailablePort(t)
		batcher2Port, llb2 := getAvailablePort(t)

		party := Party{
			ID:                core.PartyID(i + 1),
			AssemblerEndpoint: "127.0.0.1:" + assemblerPort,
			ConsenterEndpoint: "127.0.0.1:" + consenterPort,
			RouterEndpoint:    "127.0.0.1:" + routerPort,
			BatchersEndpoints: []string{"127.0.0.1:" + batcher1Port, "127.0.0.1:" + batcher2Port},
		}

		parties = append(parties, party)
		listeners = append(listeners, lla, llc, llr, llb1, llb2)
	}

	network := Network{
		Parties: parties,
	}

	err := writeToYAML(network, path)
	require.NoError(t, err)

	for _, ll := range listeners {
		require.NoError(t, ll.Close())
	}
}

func getAvailablePort(t *testing.T) (port string, ll net.Listener) {
	ll, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := ll.Addr().String()
	_, portS, err := net.SplitHostPort(endpoint)
	require.NoError(t, err)
	return portS, ll
}

func writeToYAML(config interface{}, path string) error {
	rnc, err := yaml.Marshal(&config)
	if err != nil {
		return err
	}

	err = os.WriteFile(path, rnc, 0o644)
	if err != nil {
		return err
	}

	return nil
}

// func readRouterNodeConfigFromYaml(t *testing.T, path string) *config.RouterNodeConfig {
// 	configBytes, err := os.ReadFile(path)
// 	require.NoError(t, err)
// 	routerConfig := config.RouterNodeConfig{}
// 	err = yaml.Unmarshal(configBytes, &routerConfig)
// 	require.NoError(t, err)
// 	return &routerConfig
// }

func readAssemblerNodeConfigFromYaml(t *testing.T, path string) *config.AssemblerNodeConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	assemblerConfig := config.AssemblerNodeConfig{}
	err = yaml.Unmarshal(configBytes, &assemblerConfig)
	require.NoError(t, err)
	return &assemblerConfig
}

func readConsenterNodeConfigFromYaml(t *testing.T, path string) *config.ConsenterNodeConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	consenterConfig := config.ConsenterNodeConfig{}
	err = yaml.Unmarshal(configBytes, &consenterConfig)
	require.NoError(t, err)
	return &consenterConfig
}

func readBatcherNodeConfigFromYaml(t *testing.T, path string) *config.BatcherNodeConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	batcherConfig := config.BatcherNodeConfig{}
	err = yaml.Unmarshal(configBytes, &batcherConfig)
	require.NoError(t, err)
	return &batcherConfig
}

// editDirectoryInNodeConfigYAML fill the Directory field in all relevant config structures. This must be done before running Arma nodes
func editDirectoryInNodeConfigYAML(t *testing.T, name string, path string) {
	dir, err := os.MkdirTemp("", "Directory_"+fmt.Sprint(name))
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	switch name {
	case "batcher":
		batcherConfig := readBatcherNodeConfigFromYaml(t, path)
		batcherConfig.Directory = dir
		config.NodeConfigToYAML(batcherConfig, path)
	case "consensus":
		consenterConfig := readConsenterNodeConfigFromYaml(t, path)
		consenterConfig.Directory = dir
		config.NodeConfigToYAML(consenterConfig, path)
	case "assembler":
		assemblerConfig := readAssemblerNodeConfigFromYaml(t, path)
		assemblerConfig.Directory = dir
		config.NodeConfigToYAML(assemblerConfig, path)
	}
}
