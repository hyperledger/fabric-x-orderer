package armageddon_test

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.ibm.com/decentralized-trust-research/arma/testutil/fabric"

	"github.ibm.com/decentralized-trust-research/arma/cmd/armageddon"
	"github.ibm.com/decentralized-trust-research/arma/cmd/testutils"

	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
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
	netInfo := testutils.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2", "--sampleConfigPath", sampleConfigPath})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutils.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutils.WaitReady(t, readyChan, 20, 10)

	// 4.
	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
	rate := "500"
	txs := "1000"
	txSize := "32"
	armageddon.Run([]string{"submit", "--config", userConfigPath, "--transactions", txs, "--rate", rate, "--txSize", txSize})
}

// Scenario:
// 1. Create a config YAML file to be an input to armageddon
// 2. Run armageddon generate command to create config files in a folder structure with a TLS connection between client and router and assembler, a sampleConfigPath is missing
// 3. Run arma with the generated config files to run each of the nodes for all parties
// 4. Run armageddon submit command to make 1000 txs, send txs to all routers at a specified rate and pull blocks from some assembler to observe that txs appear in some block
func TestArmageddonWithTLSWithNoSampleConfigPathFlag(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutils.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2"})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutils.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutils.WaitReady(t, readyChan, 20, 10)

	// 4.
	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
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
	netInfo := testutils.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--useTLS", "--version", "2", "--sampleConfigPath", sampleConfigPath})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutils.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutils.WaitReady(t, readyChan, 20, 10)
	// 4. + 5.
	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
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
	netInfo := testutils.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2", "--sampleConfigPath", sampleConfigPath})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutils.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutils.WaitReady(t, readyChan, 20, 10)
	// 4. + 5.
	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
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
	netInfo := testutils.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2", "--sampleConfigPath", sampleConfigPath})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutils.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutils.WaitReady(t, readyChan, 20, 10)

	// 4. + 5.
	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
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
	netInfo := testutils.CreateNetwork(t, configPath, 4, 2, "none", "none")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2", "--sampleConfigPath", sampleConfigPath})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.ibm.com/decentralized-trust-research/arma/cmd/arma/main", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutils.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutils.WaitReady(t, readyChan, 20, 10)

	// 4.
	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
	rate := "500"
	txs := "1000"
	txSize := "32"
	armageddon.Run([]string{"submit", "--config", userConfigPath, "--transactions", txs, "--rate", rate, "--txSize", txSize})
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
	testutils.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2", "--sampleConfigPath", sampleConfigPath})

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
