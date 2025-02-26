package armageddon

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/utils"
	genconfig "github.ibm.com/decentralized-trust-research/arma/config/generate"

	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	nodeconfig "github.ibm.com/decentralized-trust-research/arma/node/config"

	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// Scenario:
// 1. Create a config YAML file to be an input to armageddon
// 2. Run armageddon generate command to create config files in a folder structure with a TLS connection between client and router and assembler
// 3. Run arma with the generated config files to run each of the nodes for all parties
// 4. Run armageddon submit command to make 1000 txs, send txs to all routers at a specified rate and pull blocks from some assembler to observe that txs appear in some block
func TestArmageddonWithTLS(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	listeners := generateInputConfigFileForArmageddon(t, configPath)

	// 2.
	armageddon := NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--useTLS", "--version", "1"})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	sessions := runArmaNodes(t, dir, armaBinaryPath, readyChan, listeners)
	defer func() {
		for i := range sessions {
			sessions[i].Kill()
		}
	}()

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
	txSize := "32"
	armageddon.Run([]string{"submit", "--config", userConfigPath, "--transactions", txs, "--rate", rate, "--txSize", txSize})
}

// Scenario:
//  1. Create a config YAML file to be an input to armageddon
//  2. Run armageddon generate command to create config files in a folder structure
//  3. Run arma with the generated config files to run each of the nodes for all parties
//  4. Run armageddon load command to make 10000 txs and send them to all routers at multiple rates (5000 for each rate)
//  5. In parallel, run armageddon receive command to pull blocks from the assembler and report results , number of txs should be 40000
func TestLoadStepsAndReceive(t *testing.T) {
	t.Skip()
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	listeners := generateInputConfigFileForArmageddon(t, configPath)

	// 2.
	armageddon := NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--useTLS", "--version", "1"})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	sessions := runArmaNodes(t, dir, armaBinaryPath, readyChan, listeners)
	defer func() {
		for i := range sessions {
			sessions[i].Kill()
		}
	}()

	startTimeout := time.After(10 * time.Second)
	for i := 0; i < 20; i++ {
		select {
		case <-readyChan:
		case <-startTimeout:
			require.Fail(t, "arma nodes failed to start in time")
		}
	}

	// 4. + 5.
	userConfigPath := path.Join(dir, fmt.Sprintf("Party%d", 1), "user_config.yaml")
	rates := "500 1000"
	txsSent := "5000"
	txsRec := "10000"
	txSize := "64"

	var waitForTxToBeSentAndReceived sync.WaitGroup
	waitForTxToBeSentAndReceived.Add(2)
	go func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", txsSent, "--rate", rates, "--txSize", txSize})
		waitForTxToBeSentAndReceived.Done()
	}()

	go func() {
		armageddon.Run([]string{"receive", "--config", userConfigPath, "--pullFromPartyId", "1", "--expectedTxs", txsRec, "--output", dir})
		waitForTxToBeSentAndReceived.Done()
	}()
	waitForTxToBeSentAndReceived.Wait()
}

// Scenario:
//  1. Create a config YAML file to be an input to armageddon
//  2. Run armageddon generate command to create config files in a folder structure
//  3. Run arma with the generated config files to run each of the nodes for all parties
//     4.+5. Compile armageddon and run armageddon load command with invalid rate (string which cannot be converted to integer), expect to get an error
func TestLoadStepsFails(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	listeners := generateInputConfigFileForArmageddon(t, configPath)

	// 2.
	armageddon := NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--useTLS"})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	sessions := runArmaNodes(t, dir, armaBinaryPath, readyChan, listeners)
	defer func() {
		for i := range sessions {
			sessions[i].Kill()
		}
	}()

	startTimeout := time.After(10 * time.Second)
	for i := 0; i < 20; i++ {
		select {
		case <-readyChan:
		case <-startTimeout:
			require.Fail(t, "arma nodes failed to start in time")
		}
	}

	// 4. + 5.
	userConfigPath := path.Join(dir, fmt.Sprintf("Party%d", 1), "user_config.yaml")
	rates := "BOOM"
	txsSent := "10000"
	txSize := "64"

	armageddonBinary, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/armageddon/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armageddonBinary)
	cmd := exec.Command(armageddonBinary, "load", "--config", userConfigPath, "--transactions", txsSent, "--rate", rates, "--txSize", txSize)
	require.NotNil(t, cmd)
	stdout, err := cmd.Output()
	// Check if the command returned an error and the output contains the expected error message
	require.Contains(t, string(stdout), "BOOM")
	require.Contains(t, err.Error(), "exit status")
}

// Scenario:
//  1. Create a config YAML file to be an input to armageddon
//  2. Run armageddon generate command to create config files in a folder structure
//  3. Run arma with the generated config files to run each of the nodes for all parties
//  4. Run armageddon load command to make 10000 txs and send them to all routers at a specified rate
//  5. In parallel, run armageddon receive command to pull blocks from the assembler and report results
func TestLoadAndReceive(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	listeners := generateInputConfigFileForArmageddon(t, configPath)

	// 2.
	armageddon := NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--useTLS"})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	sessions := runArmaNodes(t, dir, armaBinaryPath, readyChan, listeners)
	defer func() {
		for i := range sessions {
			sessions[i].Kill()
		}
	}()

	startTimeout := time.After(10 * time.Second)
	for i := 0; i < 20; i++ {
		select {
		case <-readyChan:
		case <-startTimeout:
			require.Fail(t, "arma nodes failed to start in time")
		}
	}

	// 4. + 5.
	userConfigPath := path.Join(dir, fmt.Sprintf("Party%d", 1), "user_config.yaml")
	rate := "500"
	txs := "10000"
	txSize := "64"

	var waitForTxToBeSentAndReceived sync.WaitGroup
	waitForTxToBeSentAndReceived.Add(2)
	go func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", txs, "--rate", rate, "--txSize", txSize})
		waitForTxToBeSentAndReceived.Done()
	}()

	go func() {
		armageddon.Run([]string{"receive", "--config", userConfigPath, "--pullFromPartyId", "1", "--expectedTxs", txs, "--output", dir})
		waitForTxToBeSentAndReceived.Done()
	}()
	waitForTxToBeSentAndReceived.Wait()
}

func runArmaNodes(t *testing.T, dir string, armaBinaryPath string, readyChan chan struct{}, listeners map[string]net.Listener) []*gexec.Session {
	nodes := map[string][]string{
		"router":    {"router_node_config.yaml"},
		"batcher":   {"batcher_node_1_config.yaml", "batcher_node_2_config.yaml"},
		"consensus": {"consenter_node_config.yaml"},
		"assembler": {"assembler_node_config.yaml"},
	}

	var sessions []*gexec.Session
	for _, nodeType := range []string{"consensus", "batcher", "router", "assembler"} {
		for i := 0; i < 4; i++ {
			partyDir := path.Join(dir, fmt.Sprintf("Party%d", i+1))
			for j := 0; j < len(nodes[nodeType]); j++ {
				nodeConfigPath := path.Join(partyDir, nodes[nodeType][j])
				editDirectoryInNodeConfigYAML(t, nodeType, nodeConfigPath)
				var nodeTypeL string
				if nodeType == "batcher" {
					nodeTypeL = fmt.Sprintf("batcher%d", j+1)
				} else {
					nodeTypeL = nodeType
				}
				listener := listeners[fmt.Sprintf("Party%d"+nodeTypeL, i+1)]
				sess := runNode(t, nodeType, armaBinaryPath, nodeConfigPath, readyChan, listener)
				sessions = append(sessions, sess)
			}
		}
	}
	return sessions
}

// Scenario:
// 1. Create a config YAML file to be an input to armageddon
// 2. Run armageddon generate command to create config files in a folder structure, with non TLS connection between client and router and assembler
// 3. Run arma with the generated config files to run each of the nodes for all parties
// 4. Run armageddon submit command to make 1000 txs, send txs to all routers at a specified rate and pull blocks from some assembler to observe that txs appear in some block
func TestArmageddonNonTLS(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	listeners := generateInputConfigFileForArmageddon(t, configPath)

	// 2.
	armageddon := NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "1"})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	sessions := runArmaNodes(t, dir, armaBinaryPath, readyChan, listeners)
	defer func() {
		for i := range sessions {
			sessions[i].Kill()
		}
	}()

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
	txSize := "32"
	armageddon.Run([]string{"submit", "--config", userConfigPath, "--transactions", txs, "--rate", rate, "--txSize", txSize})
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

// generateInputConfigFileForArmageddon create a config.yaml file which is the input to armageddon generate command.
// the configuration includes 4 parties and 2 batchers for each party.
func generateInputConfigFileForArmageddon(t *testing.T, path string) map[string]net.Listener {
	var parties []genconfig.Party
	listeners := make(map[string]net.Listener)
	for i := 0; i < 4; i++ {
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
		UseTLSRouter:    "none",
		UseTLSAssembler: "none",
	}

	err := utils.WriteToYAML(network, path)
	require.NoError(t, err)

	return listeners
}

// func readRouterNodeConfigFromYaml(t *testing.T, path string) *config.RouterNodeConfig {
// 	configBytes, err := os.ReadFile(path)
// 	require.NoError(t, err)
// 	routerConfig := config.RouterNodeConfig{}
// 	err = yaml.Unmarshal(configBytes, &routerConfig)
// 	require.NoError(t, err)
// 	return &routerConfig
// }

func readAssemblerNodeConfigFromYaml(t *testing.T, path string) *nodeconfig.AssemblerNodeConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	assemblerConfig := nodeconfig.AssemblerNodeConfig{}
	err = yaml.Unmarshal(configBytes, &assemblerConfig)
	require.NoError(t, err)
	return &assemblerConfig
}

func readConsenterNodeConfigFromYaml(t *testing.T, path string) *nodeconfig.ConsenterNodeConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	consenterConfig := nodeconfig.ConsenterNodeConfig{}
	err = yaml.Unmarshal(configBytes, &consenterConfig)
	require.NoError(t, err)
	return &consenterConfig
}

func readBatcherNodeConfigFromYaml(t *testing.T, path string) *nodeconfig.BatcherNodeConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	batcherConfig := nodeconfig.BatcherNodeConfig{}
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
		nodeconfig.NodeConfigToYAML(batcherConfig, path)
	case "consensus":
		consenterConfig := readConsenterNodeConfigFromYaml(t, path)
		consenterConfig.Directory = dir
		nodeconfig.NodeConfigToYAML(consenterConfig, path)
	case "assembler":
		assemblerConfig := readAssemblerNodeConfigFromYaml(t, path)
		assemblerConfig.Directory = dir
		nodeconfig.NodeConfigToYAML(assemblerConfig, path)
	}
}

// Scenario:
// 1. Create a config YAML file to be an input to armageddon
// 2. Run armageddon generate command to create crypto material and config files
// 3. Check that all required material was generated in the expected structure
func TestArmageddonGenerateNewConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	generateInputConfigFileForArmageddon(t, configPath)

	// 2.
	armageddon := NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--useTLS", "--version", "2"})

	// 3.
	err = checkConfigDir(dir)
	require.NoError(t, err)

	err = checkBootstrapDir(dir)
	require.NoError(t, err)

	err = checkCryptoDir(dir)
	require.NoError(t, err)
}

// Note: this function assumes that there are 2 shards
func checkConfigDir(outputDir string) error {
	configPath := filepath.Join(outputDir, "config")

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return fmt.Errorf("config directory is missing from: %s\n", configPath)
	}

	parties, err := os.ReadDir(configPath)
	if err != nil {
		return fmt.Errorf("error reading config directory: %s\n", err)
	}

	requiredPartyFiles := []string{
		"local_config_assembler.yaml",
		"local_config_batcher1.yaml",
		"local_config_batcher2.yaml",
		"local_config_consenter.yaml",
		"local_config_router.yaml",
	}

	for _, party := range parties {
		if !party.IsDir() {
			return fmt.Errorf("error reading party dir, party %s does not describe a directory\n", party.Name())
		}

		partyDir := filepath.Join(configPath, party.Name())

		for _, file := range requiredPartyFiles {
			filePath := filepath.Join(partyDir, file)
			if !fileExists(filePath) {
				return fmt.Errorf("missing file: %s\n", filePath)
			}
		}
	}

	return nil
}

func checkBootstrapDir(outputDir string) error {
	bootstrapDir := filepath.Join(outputDir, "bootstrap")

	if _, err := os.Stat(bootstrapDir); os.IsNotExist(err) {
		return fmt.Errorf("bootstrap directory is missing from: %s\n", bootstrapDir)
	}

	filePath := filepath.Join(bootstrapDir, "shared_config.yaml")
	if !fileExists(filePath) {
		return fmt.Errorf("missing file: %s\n", filePath)
	}

	return nil
}

func checkCryptoDir(outputDir string) error {
	cryptoDir := filepath.Join(outputDir, "crypto")
	if _, err := os.Stat(cryptoDir); os.IsNotExist(err) {
		return fmt.Errorf("crypto directory is missing from: %s\n", cryptoDir)
	}

	orgsDir := filepath.Join(cryptoDir, "ordererOrganizations")
	if _, err := os.Stat(orgsDir); os.IsNotExist(err) {
		return fmt.Errorf("ordererOrganizations directory is missing from: %s\n", orgsDir)
	}

	orgs, err := os.ReadDir(orgsDir)
	if err != nil {
		return fmt.Errorf("error reading org directories: %s\n", err)
	}

	requiredOrgSubDirs := []string{
		"ca",
		"tlsca",
		"msp",
		"msp/admincerts",
		"orderers",
		"users",
	}

	for i, org := range orgs {
		if !org.IsDir() {
			return fmt.Errorf("error reading org dir, org %s does not describe a directory\n", org.Name())
		}

		orgDir := filepath.Join(orgsDir, org.Name())

		// check org includes all required directories
		for _, subDir := range requiredOrgSubDirs {
			dirPath := filepath.Join(orgDir, subDir)
			if _, err := os.Stat(dirPath); os.IsNotExist(err) {
				return fmt.Errorf("missing directory: %s\n", dirPath)
			}
		}

		// check ca and tlsca directories
		for _, subDir := range []string{"ca", "tlsca"} {
			dirPath := filepath.Join(orgDir, subDir)
			files, err := os.ReadDir(dirPath)
			if err != nil {
				return fmt.Errorf("error reading directory %s\n", filepath.Join(orgDir, subDir))
			}
			for _, file := range files {
				if strings.Contains(file.Name(), "cert") {
					if !strings.HasSuffix(file.Name(), ".pem") {
						return fmt.Errorf("error reading %s files, suffix file is not pem\n", filepath.Join(orgDir, subDir))
					}
				}
			}
		}

		// check users dir
		usersDir := filepath.Join(orgDir, "users")
		files, err := os.ReadDir(usersDir)
		if err != nil {
			return fmt.Errorf("error reading directory %s\n", usersDir)
		}
		for _, file := range files {
			if !strings.HasSuffix(file.Name(), ".pem") {
				return fmt.Errorf("error reading %s files, suffix file is not pem\n", filepath.Join(orgDir, usersDir))
			}
		}

		// check orderers directory
		orderersDir := filepath.Join(orgDir, "orderers")
		if _, err := os.Stat(orderersDir); os.IsNotExist(err) {
			return fmt.Errorf("missing directory: %s\n", orderersDir)
		}

		partyDir := filepath.Join(orderersDir, fmt.Sprintf("party%d", i+1))
		if _, err := os.Stat(partyDir); os.IsNotExist(err) {
			return fmt.Errorf("missing directory: %s\n", partyDir)
		}

		requiredPartyDirs := []string{"assembler", "batcher1", "batcher2", "consenter", "router"}
		for _, partySubDir := range requiredPartyDirs {
			path := filepath.Join(partyDir, partySubDir)
			if _, err := os.Stat(path); os.IsNotExist(err) {
				return fmt.Errorf("missing directory: %s\n", path)
			}
			files, err := os.ReadDir(path)
			if err != nil {
				return fmt.Errorf("error reading directory %s\n", path)
			}
			for _, file := range files {
				if !strings.HasSuffix(file.Name(), ".pem") {
					return fmt.Errorf("error reading %s files, suffix file is not pem\n", filepath.Join(orgDir, path))
				}
			}
		}
	}

	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
