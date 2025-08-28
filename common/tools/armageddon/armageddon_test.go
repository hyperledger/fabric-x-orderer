/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

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
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/fabric"
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
	netInfo := testutil.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--sampleConfigPath", sampleConfigPath})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, 20, 10)

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
	netInfo := testutil.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, 20, 10)

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
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--useTLS", "--sampleConfigPath", sampleConfigPath})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, 20, 10)
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
	netInfo := testutil.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--sampleConfigPath", sampleConfigPath})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, 20, 10)
	// 4. + 5.
	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
	rates := "BOOM"
	txsSent := "10000"
	txSize := "64"

	armageddonBinary, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/armageddon", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, armageddonBinary)
	cmd := exec.Command(armageddonBinary, "load", "--config", userConfigPath, "--transactions", txsSent, "--rate", rates, "--txSize", txSize)
	require.NotNil(t, cmd)
	output, err := cmd.CombinedOutput()
	// Check if the command returned an error and the output contains the expected error message
	require.Contains(t, string(output), "rate is not valid")
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
	netInfo := testutil.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--sampleConfigPath", sampleConfigPath})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, 20, 10)

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
	netInfo := testutil.CreateNetwork(t, configPath, 4, 2, "none", "none")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--sampleConfigPath", sampleConfigPath})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, 20, 10)

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
// 3. Check that createSharedConfigProto command creates the expected output
func TestArmageddonSharedConfigProtoFromSharedConfigYAML(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	blockDir := filepath.Join(dir, "sharedConfig")
	err = os.MkdirAll(blockDir, 0o755)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	testutil.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--sampleConfigPath", sampleConfigPath})

	// 3.
	sharedConfigYAMLPath := filepath.Join(dir, "bootstrap", "shared_config.yaml")
	armageddon.Run([]string{"createSharedConfigProto", "--sharedConfigYaml", sharedConfigYAMLPath, "--output", blockDir})
	err = checkSharedConfigDir(blockDir)
	require.NoError(t, err)
}

func checkSharedConfigDir(outputDir string) error {
	filePath := filepath.Join(outputDir, "shared_config.binpb")
	if !fileExists(filePath) {
		return fmt.Errorf("missing file: %s\n", filePath)
	}

	return nil
}

// Scenario:
// 1. Create a config YAML file to be an input to armageddon
// 2. Run armageddon generate command to create crypto material and config files
// 3. Check that createBlock command creates the expected output
func TestArmageddonCreateBlockFromSharedConfigYAML(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	blockDir := filepath.Join(dir, "sharedConfig")
	err = os.MkdirAll(blockDir, 0o755)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	testutil.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--sampleConfigPath", sampleConfigPath})

	// 3.
	sharedConfigYAMLPath := filepath.Join(dir, "bootstrap", "shared_config.yaml")
	armageddon.Run([]string{"createBlock", "--sharedConfigYaml", sharedConfigYAMLPath, "--output", blockDir, "--sampleConfigPath", sampleConfigPath})
	require.True(t, fileExists(filepath.Join(blockDir, "bootstrap.block")))
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
	testutil.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--sampleConfigPath", sampleConfigPath})

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

			mspPath := filepath.Join(partyDir, partySubDir, "msp")
			if _, err := os.Stat(mspPath); os.IsNotExist(err) {
				return fmt.Errorf("missing directory: %s\n", mspPath)
			}
			requiredMSPSubDirs := []string{"cacerts", "intermediatecerts", "admincerts", "keystore", "signcerts", "tlscacerts", "tlsintermediatecerts"}
			for _, mspSubDir := range requiredMSPSubDirs {
				mspSubDirPath := filepath.Join(mspPath, mspSubDir)
				if _, err := os.Stat(mspSubDirPath); os.IsNotExist(err) {
					return fmt.Errorf("missing directory: %s\n", mspSubDirPath)
				}
				if mspSubDir == "keystore" || mspSubDir == "signcerts" {
					files, err = os.ReadDir(mspSubDirPath)
					if err != nil {
						return fmt.Errorf("error reading directory %s\n", mspSubDirPath)
					}
					for _, file := range files {
						if !strings.HasSuffix(file.Name(), ".pem") && !strings.Contains(file.Name(), "priv_sk") {
							return fmt.Errorf("error reading %s files, expect pem files or file name priv_sk \n", mspSubDirPath)
						}
					}
				}
			}

			tlsPath := filepath.Join(partyDir, partySubDir, "tls")
			if _, err := os.Stat(tlsPath); os.IsNotExist(err) {
				return fmt.Errorf("missing directory: %s\n", tlsPath)
			}
			files, err = os.ReadDir(tlsPath)
			if err != nil {
				return fmt.Errorf("error reading directory %s\n", tlsPath)
			}
			for _, file := range files {
				if !strings.HasSuffix(file.Name(), ".pem") {
					return fmt.Errorf("error reading %s files, suffix file is not pem\n", tlsPath)
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

func TestLoadAndReceiveRouterFaulty(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, 4, 2, "TLS", "TLS")
	fmt.Printf("path is: %v\n", configPath)

	// 2.
	armageddon := armageddon.NewCLI()
	sampleConfigPath := fabric.GetDevConfigDir()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--sampleConfigPath", sampleConfigPath})

	// 3.
	// compile arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// run arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, 20)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, 20, 10)

	fmt.Printf("Arma is up")

	// 4. + 5.
	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
	rate := "1000"
	txs := "100000"
	txSize := "64"

	var waitForTxToBeSentAndReceived sync.WaitGroup
	var waitForStartSend sync.WaitGroup
	waitForTxToBeSentAndReceived.Add(3)
	waitForStartSend.Add(1)

	go func() {
		armageddon.Run([]string{"receive", "--config", userConfigPath, "--pullFromPartyId", "1", "--expectedTxs", "100000", "--output", dir})
		waitForTxToBeSentAndReceived.Done()
	}()

	go func() {
		waitForStartSend.Done()
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", txs, "--rate", rate, "--txSize", txSize})
		waitForTxToBeSentAndReceived.Done()
	}()

	// stop the router
	go func() {
		waitForStartSend.Wait()
		time.Sleep(10 * time.Second)
		armaNetwork.GetRouter(t, 1).StopArmaNode()
		waitForTxToBeSentAndReceived.Done()
	}()

	waitForTxToBeSentAndReceived.Wait()
}
