/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package arma

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.ibm.com/decentralized-trust-research/arma/testutil"

	"github.com/hyperledger/fabric-lib-go/common/flogging"

	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/common/tools/armageddon"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/common/utils"
	"github.ibm.com/decentralized-trust-research/arma/config"
	genconfig "github.ibm.com/decentralized-trust-research/arma/config/generate"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	RouterBaseListenPort    = 6022
	AssemblerBaseListenPort = 6032
	BatcherBaseListenPort   = 6042
	ConsensusBaseListenPort = 6052
)

func TestCLI(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	fPath := filepath.Join(dir, "cli-test")

	err = os.WriteFile(fPath, []byte("the little fox jumped over the lazy dog"), 0o600)
	require.NoError(t, err)

	cli := NewCLI()
	cli.Command("test", "run a router node", func(configFile *os.File) {
		stat, err := configFile.Stat()
		require.NoError(t, err)

		content := make([]byte, stat.Size())
		_, err = io.ReadFull(configFile, content)
		require.NoError(t, err)

		assert.Equal(t, []byte("the little fox jumped over the lazy dog"), content)
	})
	cli.Run([]string{"test", "--config", fPath})
}

func TestLaunchArmaNode(t *testing.T) {
	t.Run("TestRouter", func(t *testing.T) {
		dir := setup(t, 1)
		defer os.RemoveAll(dir)
		testLogger = flogging.MustGetLogger("arma")

		configPath := filepath.Join(dir, "config", "party1", "local_config_router.yaml")
		storagePath := path.Join(dir, "storage", "party1", "router")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)
		err := editBatchersInSharedConfig(dir, 4, 2)
		require.NoError(t, err)

		originalLogger := testLogger
		defer func() {
			testLogger = originalLogger
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Router listening on") {
				wg.Done()
			}
			return nil
		}))

		cli := NewCLI()
		cli.Run([]string{"router", "--config", configPath})
		wg.Wait()
	})

	t.Run("TestBatcher", func(t *testing.T) {
		dir := setup(t, 1)
		defer os.RemoveAll(dir)
		testLogger = flogging.MustGetLogger("arma")

		configPath := filepath.Join(dir, "config", "party1", "local_config_batcher1.yaml")
		storagePath := path.Join(dir, "storage", "party1", "batcher1")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)
		err := editConsentersInSharedConfig(dir, 4)
		require.NoError(t, err)

		originalLogger := testLogger
		defer func() {
			testLogger = originalLogger
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Batcher listening on") {
				wg.Done()
			}
			return nil
		}))

		cli := NewCLI()
		cli.Run([]string{"batcher", "--config", configPath})
		wg.Wait()
	})

	t.Run("TestConsensus", func(t *testing.T) {
		dir := setup(t, 1)
		defer os.RemoveAll(dir)
		testLogger = flogging.MustGetLogger("arma")

		configPath := filepath.Join(dir, "config", "party1", "local_config_consenter.yaml")
		storagePath := path.Join(dir, "storage", "party1", "consenter")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)

		originalLogger := testLogger
		defer func() {
			testLogger = originalLogger
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Consensus listening on") {
				wg.Done()
			}
			return nil
		}))

		cli := NewCLI()
		cli.Run([]string{"consensus", "--config", configPath})
		wg.Wait()
	})

	t.Run("TestAssembler", func(t *testing.T) {
		dir := setup(t, 1)
		defer os.RemoveAll(dir)
		testLogger = flogging.MustGetLogger("arma")

		configPath := filepath.Join(dir, "config", "party1", "local_config_assembler.yaml")
		storagePath := path.Join(dir, "storage", "party1", "assembler")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)

		originalLogger := testLogger
		defer func() {
			testLogger = originalLogger
		}()

		var wg sync.WaitGroup
		wg.Add(2)

		testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Assembler listening on") {
				wg.Done()
			}
			if strings.Contains(entry.Message, "Starting to replicate from consenter") {
				wg.Done()
			}
			return nil
		}))

		cli := NewCLI()
		cli.Run([]string{"assembler", "--config", configPath})
		wg.Wait()
	})
}

func TestLaunchAssemblerAndConsenterWithBlock(t *testing.T) {
	t.Run("TestConsensusWithBlock", func(t *testing.T) {
		dir := setup(t, 50)
		defer os.RemoveAll(dir)

		testLogger = flogging.MustGetLogger("arma")

		configPath := filepath.Join(dir, "config", "party1", "local_config_consenter.yaml")
		storagePath := path.Join(dir, "storage", "party1", "consenter")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)

		block := utils.EmptyGenesisBlock("arma")
		blockBytes := protoutil.MarshalOrPanic(block)
		// where the block should be written in the new config? currently the node checks the "config/party" dir.
		blockPath := filepath.Join(dir, "config", "party1", "genesis.block")
		err := os.WriteFile(blockPath, blockBytes, 0o600)
		require.NoError(t, err)

		originalLogger := testLogger
		defer func() {
			testLogger = originalLogger
		}()

		var wg sync.WaitGroup
		wg.Add(1)

		testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Consensus listening on") {
				wg.Done()
			}
			return nil
		}))

		cli := NewCLI()
		cli.Run([]string{"consensus", "--config", configPath})
		wg.Wait()
	})

	t.Run("TestAssemblerWithBlock", func(t *testing.T) {
		dir := setup(t, 50)
		defer os.RemoveAll(dir)
		testLogger = flogging.MustGetLogger("arma")

		configPath := filepath.Join(dir, "config", "party1", "local_config_assembler.yaml")
		storagePath := path.Join(dir, "storage", "party1", "assembler")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)

		block := utils.EmptyGenesisBlock("arma")
		blockBytes := protoutil.MarshalOrPanic(block)
		// where the block should be written in the new config? currently the node checks the "config/party" dir.
		blockPath := filepath.Join(dir, "config", "party1", "genesis.block")
		err := os.WriteFile(blockPath, blockBytes, 0o600)
		require.NoError(t, err)

		originalLogger := testLogger
		defer func() {
			testLogger = originalLogger
		}()

		var wg sync.WaitGroup
		wg.Add(2)

		testLogger = testLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Assembler listening on") {
				wg.Done()
			}
			if strings.Contains(entry.Message, "Starting to replicate from consenter") {
				wg.Done()
			}
			return nil
		}))

		cli := NewCLI()
		cli.Run([]string{"assembler", "--config", configPath})
		wg.Wait()
	})
}

func setup(t *testing.T, offset int) string {
	dir, err := os.MkdirTemp("", strings.ReplaceAll(t.Name(), "/", ""))
	require.NoError(t, err)

	configPath := filepath.Join(dir, "config.yaml")
	CreateNetworkWithDefaultPorts(t, configPath, offset)

	armageddon := armageddon.NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir, "--version", "2"})
	return dir
}

// editBatchersInSharedConfig edits the endpoints of all batchers in the shared config file to fake endpoints.
// By replacing the endpoints of batchers we effectively allow the router to run without connection dependencies.
func editBatchersInSharedConfig(dir string, parties int, shards int) error {
	for i := 0; i < parties; i++ {
		for j := 0; j < shards; j++ {
			path := filepath.Join(dir, "bootstrap", "shared_config.yaml")
			sharedConfigYaml := config.SharedConfigYaml{}
			err := utils.ReadFromYAML(&sharedConfigYaml, path)
			if err != nil {
				return fmt.Errorf("cannot load shared configuration, failed reading config yaml, err: %s", err)
			}
			sharedConfigYaml.PartiesConfig[i].BatchersConfig[j].Host = "127.0.0.1"
			sharedConfigYaml.PartiesConfig[i].BatchersConfig[j].Port = 80
			err = utils.WriteToYAML(&sharedConfigYaml, path)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// editBatchersInSharedConfig edits the endpoints of all consenters in the shared config file to fake endpoints.
// By replacing the endpoints of consenters we effectively allow the batcher to run without connection dependencies.
func editConsentersInSharedConfig(dir string, parties int) error {
	for i := 0; i < parties; i++ {
		path := filepath.Join(dir, "bootstrap", "shared_config.yaml")
		sharedConfigYaml := config.SharedConfigYaml{}
		err := utils.ReadFromYAML(&sharedConfigYaml, path)
		if err != nil {
			return fmt.Errorf("cannot load shared configuration, failed reading config yaml, err: %s", err)
		}
		sharedConfigYaml.PartiesConfig[i].ConsenterConfig.Host = "127.0.0.1"
		sharedConfigYaml.PartiesConfig[i].ConsenterConfig.Port = 80
		err = utils.WriteToYAML(&sharedConfigYaml, path)
		if err != nil {
			return err
		}
	}
	return nil
}

func CreateNetworkWithDefaultPorts(t *testing.T, path string, offset int) {
	var parties []genconfig.Party

	for i := 0; i < 4; i++ {
		assemblerPort := AssemblerBaseListenPort + i + offset
		consenterPort := ConsensusBaseListenPort + i + offset
		routerPort := RouterBaseListenPort + i + offset
		batcher1Port := BatcherBaseListenPort + i + offset
		batcher2Port := BatcherBaseListenPort + i + 4 + offset

		party := genconfig.Party{
			ID:                types.PartyID(i + 1),
			AssemblerEndpoint: "127.0.0.1:" + strconv.Itoa(assemblerPort),
			ConsenterEndpoint: "127.0.0.1:" + strconv.Itoa(consenterPort),
			RouterEndpoint:    "127.0.0.1:" + strconv.Itoa(routerPort),
			BatchersEndpoints: []string{"127.0.0.1:" + strconv.Itoa(batcher1Port), "127.0.0.1:" + strconv.Itoa(batcher2Port)},
		}

		parties = append(parties, party)
	}

	network := genconfig.Network{
		Parties:         parties,
		UseTLSRouter:    "none",
		UseTLSAssembler: "none",
	}

	err := utils.WriteToYAML(network, path)
	require.NoError(t, err)
}
