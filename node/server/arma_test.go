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

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	"github.com/hyperledger/fabric-x-orderer/common/msp"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	genconfig "github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/node"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	// TODO: remove all files and add a shut down signal to the CLI
	dir := setup(t, 1)
	mspPath := path.Join(dir, "crypto", "ordererOrganizations", "org1", "orderers", "party1", "router", "msp")

	t.Run("TestRouter", func(t *testing.T) {
		testLogger = flogging.MustGetLogger("arma")

		configPath := filepath.Join(dir, "config", "party1", "local_config_router.yaml")
		storagePath := path.Join(dir, "storage", "party1", "router")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)
		testutil.EditLocalMSPDirForNode(t, configPath, mspPath)
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
		testLogger = flogging.MustGetLogger("arma")

		configPath := filepath.Join(dir, "config", "party1", "local_config_batcher1.yaml")
		storagePath := path.Join(dir, "storage", "party1", "batcher1")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)
		testutil.EditLocalMSPDirForNode(t, configPath, mspPath)
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
		testLogger = flogging.MustGetLogger("arma")

		configPath := filepath.Join(dir, "config", "party1", "local_config_consenter.yaml")
		storagePath := path.Join(dir, "storage", "party1", "consenter")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)
		testutil.EditLocalMSPDirForNode(t, configPath, mspPath)

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
		testLogger = flogging.MustGetLogger("arma")

		configPath := filepath.Join(dir, "config", "party1", "local_config_assembler.yaml")
		storagePath := path.Join(dir, "storage", "party1", "assembler")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)
		testutil.EditLocalMSPDirForNode(t, configPath, mspPath)

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

	t.Run("TestRouterWithLastConfigBlock", func(t *testing.T) {
		configPath := filepath.Join(dir, "config", "party1", "local_config_router.yaml")
		storagePath := path.Join(dir, "storage", "party1", "router")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)
		testutil.EditLocalMSPDirForNode(t, configPath, mspPath)
		err := editBatchersInSharedConfig(dir, 4, 2)
		require.NoError(t, err)
		testLogger = flogging.MustGetLogger("arma")

		originalLogger := testLogger
		defer func() {
			testLogger = originalLogger
		}()

		// ReadConfig, expect for genesis block
		_, genesisBlock, err := config.ReadConfig(configPath, testLogger)
		require.NoError(t, err)
		require.NotNil(t, genesisBlock)

		configStore, err := configstore.NewStore(storagePath)
		require.NoError(t, err)
		require.NotNil(t, configStore)
		listBlocks, err := configStore.ListBlocks()
		require.NoError(t, err)
		require.Equal(t, len(listBlocks), 1)
		require.Equal(t, genesisBlock.Header.Number, uint64(0))
		require.Equal(t, listBlocks[0].Header.Number, uint64(0))

		// Add a fake block with block number 5 to the config store
		// ReadConfig again, expect for the fake block to be the last block
		newConfigBlock := genesisBlock
		newConfigBlock.Header.Number = 5
		err = configStore.Add(newConfigBlock)
		require.NoError(t, err)

		_, lastConfigBlock, err := config.ReadConfig(configPath, testLogger)
		require.NoError(t, err)
		require.NotNil(t, lastConfigBlock)

		listBlocks, err = configStore.ListBlocks()
		require.NoError(t, err)
		require.Equal(t, len(listBlocks), 2)
		require.Equal(t, listBlocks[0].Header.Number, uint64(0))
		require.Equal(t, listBlocks[1].Header.Number, uint64(5))
	})

	t.Run("TestBatcherWithLastConfigBlock", func(t *testing.T) {
		configPath := filepath.Join(dir, "config", "party1", "local_config_batcher1.yaml")
		storagePath := path.Join(dir, "storage", "party1", "batcher1")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)
		testutil.EditLocalMSPDirForNode(t, configPath, mspPath)
		err := editBatchersInSharedConfig(dir, 4, 2)
		require.NoError(t, err)
		testLogger = flogging.MustGetLogger("arma")

		originalLogger := testLogger
		defer func() {
			testLogger = originalLogger
		}()

		// ReadConfig, expect for genesis block
		_, genesisBlock, err := config.ReadConfig(configPath, testLogger)
		require.NoError(t, err)
		require.NotNil(t, genesisBlock)

		configStore, err := configstore.NewStore(storagePath)
		require.NoError(t, err)
		require.NotNil(t, configStore)
		listBlocks, err := configStore.ListBlocks()
		require.NoError(t, err)
		require.Equal(t, len(listBlocks), 1)
		require.Equal(t, genesisBlock.Header.Number, uint64(0))
		require.Equal(t, listBlocks[0].Header.Number, uint64(0))

		// Add a fake block with block number 5 to the config store
		// ReadConfig again, expect for the fake block to be the last block
		newConfigBlock := genesisBlock
		newConfigBlock.Header.Number = 6
		err = configStore.Add(newConfigBlock)
		require.NoError(t, err)

		_, lastConfigBlock, err := config.ReadConfig(configPath, testLogger)
		require.NoError(t, err)
		require.NotNil(t, lastConfigBlock)

		listBlocks, err = configStore.ListBlocks()
		require.NoError(t, err)
		require.Equal(t, len(listBlocks), 2)
		require.Equal(t, listBlocks[0].Header.Number, uint64(0))
		require.Equal(t, listBlocks[1].Header.Number, uint64(6))
	})

	t.Run("TestAssemblerWithLastConfigBlock", func(t *testing.T) {
		configPath := filepath.Join(dir, "config", "party1", "local_config_assembler.yaml")
		storagePath := path.Join(dir, "storage", "party1", "assemblerWithLastConfigBlock")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)
		testutil.EditLocalMSPDirForNode(t, configPath, mspPath)
		err := editBatchersInSharedConfig(dir, 4, 2)
		require.NoError(t, err)
		testLogger = flogging.MustGetLogger("arma")

		originalLogger := testLogger
		defer func() {
			testLogger = originalLogger
		}()

		// ReadConfig, expect for genesis block
		configContent, genesisBlock, err := config.ReadConfig(configPath, testLogger)
		require.NoError(t, err)
		require.NotNil(t, genesisBlock)

		// Read assembler ledger and check it is empty
		assemblerLedgerFactory := &node_ledger.DefaultAssemblerLedgerFactory{}
		assemblerLedger, err := assemblerLedgerFactory.Create(testLogger, configContent.LocalConfig.NodeLocalConfig.FileStore.Path)
		require.NoError(t, err)
		require.NotNil(t, assemblerLedger)
		assemblerLedger.Metrics().NewAssemblerLedgerMetrics(monitoring.NewMonitor(monitoring.Endpoint{Host: "127.0.0.1", Port: 0}, "TestAssemblerWithLastConfigBlock").Provider, "party1", testutil.CreateLogger(t, 0))
		require.Equal(t, assemblerLedger.LedgerReader().Height(), uint64(0))
		require.Equal(t, assemblerLedger.GetTxCount(), uint64(0))
		assemblerLedger.Close()

		// Create the assembler and check genesis block was appended
		conf := configContent.ExtractAssemblerConfig(genesisBlock)
		conf.ListenAddress = "127.0.0.1:5020"
		srv := node.CreateGRPCAssembler(conf)
		assembler := assembler.NewAssembler(conf, srv, genesisBlock, testLogger)
		require.NotNil(t, assembler)
		assembler.Stop()

		assemblerLedger, err = assemblerLedgerFactory.Create(testLogger, configContent.LocalConfig.NodeLocalConfig.FileStore.Path)
		require.NoError(t, err)
		require.NotNil(t, assemblerLedger)
		assemblerLedger.Metrics().NewAssemblerLedgerMetrics(monitoring.NewMonitor(monitoring.Endpoint{Host: "127.0.0.1", Port: 0}, "TestAssemblerWithLastConfigBlock").Provider, "party1", testutil.CreateLogger(t, 0))
		require.Equal(t, assemblerLedger.LedgerReader().Height(), uint64(1))

		// Add a fake config block with block number 1 to the ledger
		// ReadConfig again, expect for the fake block to be the last block
		newConfigBlock := &common.Block{
			Header:   &common.BlockHeader{},
			Data:     genesisBlock.Data,
			Metadata: &common.BlockMetadata{Metadata: [][]byte{{}, {}, {}, {}}},
		}
		newConfigBlock.Header.Number = 1
		newConfigBlock.Header.PreviousHash = protoutil.BlockHeaderHash(genesisBlock.Header)
		newConfigBlock.Metadata.Metadata[common.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&common.Metadata{
			Value: protoutil.MarshalOrPanic(&common.LastConfig{Index: 1}),
		})
		assemblerLedger.AppendConfig(&state.OrderingInformation{
			CommonBlock: newConfigBlock,
			DecisionNum: 1,
			BatchIndex:  0,
			BatchCount:  1,
		})
		assemblerLedger.Close()

		_, lastConfigBlock, err := config.ReadConfig(configPath, testLogger)
		require.NoError(t, err)
		require.NotNil(t, lastConfigBlock)

		require.Equal(t, lastConfigBlock.Header.Number, uint64(1))
	})

	t.Run("TestConsensusWithLastConfigBlock", func(t *testing.T) {
		configPath := filepath.Join(dir, "config", "party1", "local_config_consenter.yaml")
		storagePath := path.Join(dir, "storage", "party1", "consenterWithLastConfigBlock")
		testutil.EditDirectoryInNodeConfigYAML(t, configPath, storagePath)
		testutil.EditLocalMSPDirForNode(t, configPath, mspPath)
		err := editBatchersInSharedConfig(dir, 4, 2)
		require.NoError(t, err)
		testLogger = flogging.MustGetLogger("arma")

		originalLogger := testLogger
		defer func() {
			testLogger = originalLogger
		}()

		// ReadConfig, expect for genesis block
		configContent, genesisBlock, err := config.ReadConfig(configPath, testLogger)
		require.NoError(t, err)
		require.NotNil(t, genesisBlock)

		// Read consensus ledger and check it is empty
		consensusLedger, err := node_ledger.NewConsensusLedger(configContent.LocalConfig.NodeLocalConfig.FileStore.Path)
		require.NoError(t, err)
		require.NotNil(t, consensusLedger)
		require.Equal(t, consensusLedger.Height(), uint64(0))
		consensusLedger.Close()

		// Create the consenter and check genesis block was appended
		conf := configContent.ExtractConsenterConfig(genesisBlock)
		conf.ListenAddress = "127.0.0.1:5020"
		srv := node.CreateGRPCConsensus(conf)
		localmsp := msp.BuildLocalMSP(configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID, configContent.LocalConfig.NodeLocalConfig.GeneralConfig.BCCSP)
		signer, err := localmsp.GetDefaultSigningIdentity()
		require.NoError(t, err)

		mockConfigUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
		mockConfigUpdateProposer.ProposeConfigUpdateReturns(nil, nil)

		consensus := consensus.CreateConsensus(conf, srv, genesisBlock, testLogger, signer, mockConfigUpdateProposer)
		require.NotNil(t, consensus)
		consensus.Storage.Close()

		consensusLedger, err = node_ledger.NewConsensusLedger(configContent.LocalConfig.NodeLocalConfig.FileStore.Path)
		require.NoError(t, err)
		require.NotNil(t, consensusLedger)
		require.Equal(t, consensusLedger.Height(), uint64(1))

		// Add two fake decisions:
		// Decision 1 with one data block (header.Number = 1) and one config block (header.Number = 2)
		// Decision 2 with one data block (header.Number = 3)
		// ReadConfig again, expect for the fake config block to be the last block

		dataCommonBlock1 := createDataBlock(uint64(1))

		initialState := &state.State{
			ShardCount: 2,
			N:          4,
			Shards:     []state.ShardTerm{{Shard: 1}, {Shard: 2}},
			Threshold:  2,
			Quorum:     3,
			AppContext: protoutil.MarshalOrPanic(&common.BlockHeader{Number: 0}),
		}

		newConfigCommonBlock := &common.Block{Header: &common.BlockHeader{Number: 2}, Data: genesisBlock.Data}
		newConfigProposal := smartbft_types.Proposal{
			Header: (&state.Header{
				AvailableCommonBlocks:        []*common.Block{dataCommonBlock1, newConfigCommonBlock},
				State:                        initialState,
				Num:                          1,
				DecisionNumOfLastConfigBlock: 1,
			}).Serialize(),
			Metadata: nil,
		}

		dataCommonBlock2 := createDataBlock(uint64(3))

		newProposal := smartbft_types.Proposal{
			Header: (&state.Header{
				AvailableCommonBlocks:        []*common.Block{dataCommonBlock2},
				State:                        initialState,
				Num:                          2,
				DecisionNumOfLastConfigBlock: 1,
			}).Serialize(),
			Metadata: nil,
		}

		consensusLedger.Append(state.DecisionToBytes(newConfigProposal, nil))
		consensusLedger.Append(state.DecisionToBytes(newProposal, nil))
		consensusLedger.Close()

		_, lastConfigBlock, err := config.ReadConfig(configPath, testLogger)
		require.NoError(t, err)
		require.NotNil(t, lastConfigBlock)

		require.Equal(t, lastConfigBlock.Header.Number, uint64(2))
	})
}

func setup(t *testing.T, offset int) string {
	dir, err := os.MkdirTemp("", strings.ReplaceAll(t.Name(), "/", ""))
	require.NoError(t, err)

	configPath := filepath.Join(dir, "config.yaml")
	CreateNetworkWithDefaultPorts(t, configPath, offset)

	armageddon := armageddon.NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir})
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

func createDataBlock(blockNum uint64) *common.Block {
	block := protoutil.NewBlock(blockNum, []byte{})
	block.Data = &common.BlockData{
		Data: [][]byte{
			protoutil.MarshalOrPanic(&common.Envelope{
				Payload: protoutil.MarshalOrPanic(&common.Payload{
					Header: &common.Header{
						ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{
							Type: int32(common.HeaderType_ENDORSER_TRANSACTION),
						}),
					},
				}),
			}),
		},
	}
	block.Header.DataHash, _ = protoutil.BlockDataHash(block.Data)
	protoutil.InitBlockMetadata(block)

	return block
}
