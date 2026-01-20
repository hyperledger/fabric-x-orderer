/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package test

import (
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/msp"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	config "github.com/hyperledger/fabric-x-orderer/config"
	arma_node "github.com/hyperledger/fabric-x-orderer/node"
	batcher_node "github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Scenario:
// 1. Run 4 parties
// 2. Submit 1000 txs to all
// 3. Pull from all
// 4. Stop one of the consenter nodes
// 5. Submit another 500 txs
// 6. Pull from all correct parties
// 7. Restart the consenter node
// 8. Submit 500 more txs
// 9. Pull from all
func TestSubmitStopThenRestartConsenter(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 2
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "TLS", "TLS")

	armageddon := armageddon.NewCLI()
	armageddon.Run([]string{"generate", "--config", configPath, "--output", dir})

	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=github.ibm.com"})
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	readyChan := make(chan struct{}, len(netInfo))
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)

	defer armaNetwork.Stop()

	testutil.WaitReady(t, readyChan, len(netInfo), 10)

	uc, err := testutil.GetUserConfig(dir, 1)
	require.NoError(t, err)
	require.NotNil(t, uc)

	userConfigPath := path.Join(dir, "config", fmt.Sprintf("party%d", 1), "user_config.yaml")
	rate := "500"
	txSize := "64"

	var waitForTxSent sync.WaitGroup
	waitForTxSent.Add(1)
	go func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", strconv.Itoa(1000), "--rate", rate, "--txSize", txSize})
		waitForTxSent.Done()
	}()

	waitForTxSent.Wait()

	parties := make([]types.PartyID, numOfParties)
	for i := 0; i < numOfParties; i++ {
		parties[i] = types.PartyID(i + 1)
	}

	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		StartBlock:   0,
		EndBlock:     math.MaxUint64,
		Transactions: 1000,
		ErrString:    "cancelled pull from assembler: %d",
		Timeout:      30,
	})

	partyToRestart := types.PartyID(3)
	consenterToRestart := armaNetwork.GetConsenter(t, partyToRestart)
	consenterToRestart.StopArmaNode()

	waitForTxSent.Add(1)
	go func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", strconv.Itoa(500), "--rate", rate, "--txSize", txSize})
		waitForTxSent.Done()
	}()

	waitForTxSent.Wait()

	correctParties := make([]types.PartyID, 0)
	for i := 0; i < numOfParties; i++ {
		if types.PartyID(i+1) != partyToRestart {
			correctParties = append(correctParties, types.PartyID(i+1))
		}
	}
	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   uc,
		Parties:      correctParties,
		StartBlock:   0,
		EndBlock:     math.MaxUint64,
		Transactions: 1500,
		ErrString:    "cancelled pull from assembler: %d",
		Timeout:      30,
	})

	consenterToRestart.RestartArmaNode(t, readyChan)
	testutil.WaitReady(t, readyChan, 1, 10)

	waitForTxSent.Add(1)
	go func() {
		armageddon.Run([]string{"load", "--config", userConfigPath, "--transactions", strconv.Itoa(500), "--rate", rate, "--txSize", txSize})
		waitForTxSent.Done()
	}()

	waitForTxSent.Wait()

	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig:   uc,
		Parties:      parties,
		StartBlock:   0,
		EndBlock:     math.MaxUint64,
		Transactions: 2000,
		ErrString:    "cancelled pull from assembler: %d",
		Timeout:      30,
	})
}

func TestConsensusWithRealConfigUpdate(t *testing.T) {
	// Create a temporary directory for the test.
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	numOfParties := 1
	numOfShards := 1

	// Create a config YAML file in the temporary directory.
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "TLS", "none")
	require.NotNil(t, netInfo)

	// Generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	// Update the file store path
	fileStoreDir := t.TempDir()
	defer os.RemoveAll(fileStoreDir)
	nodeConfigPath := filepath.Join(dir, "config", "party1", "local_config_consenter.yaml")
	localConfig, _, err := config.LoadLocalConfig(nodeConfigPath)
	require.NoError(t, err)
	localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
	utils.WriteToYAML(localConfig.NodeLocalConfig, nodeConfigPath)

	// Get all for create consensus
	configContent, lastConfigBlock, err := config.ReadConfig(nodeConfigPath, testutil.CreateLoggerForModule(t, "ReadConfigConsensus", zap.DebugLevel))
	require.NoError(t, err)
	consenterConfig := configContent.ExtractConsenterConfig(lastConfigBlock)
	require.NotNil(t, consenterConfig)
	localmsp := msp.BuildLocalMSP(configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, configContent.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPID, configContent.LocalConfig.NodeLocalConfig.GeneralConfig.BCCSP)
	signer, err := localmsp.GetDefaultSigningIdentity()
	require.NoError(t, err)
	consenterLogger := testutil.CreateLogger(t, int(numOfParties))
	server := arma_node.CreateGRPCConsensus(consenterConfig)
	consensus := consensus.CreateConsensus(consenterConfig, server, lastConfigBlock, consenterLogger, signer, &policy.DefaultConfigUpdateProposer{})

	// Register and start grpc server
	protos.RegisterConsensusServer(server.Server(), consensus)
	orderer.RegisterAtomicBroadcastServer(server.Server(), consensus.DeliverService)
	orderer.RegisterClusterNodeServiceServer(server.Server(), consensus)
	go func() {
		server.Start()
	}()

	// Start consensus
	consensus.Start()
	ledgerListener := &storageListener{c: make(chan *common.Block, 100)}
	consensus.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(ledgerListener)

	// Submit to consensus a simple request (baf) from batcher
	digest := make([]byte, 32-3)
	digest123 := append([]byte{1, 2, 3}, digest...)
	keyBytes, err := os.ReadFile(filepath.Join(dir, "crypto/ordererOrganizations/org1/orderers/party1/batcher1/msp/keystore/priv_sk"))
	require.NoError(t, err)
	privateKey, err := tx.CreateECDSAPrivateKey(keyBytes)
	require.NoError(t, err, "failed to create private key")
	baf, err := batcher_node.CreateBAF((*crypto.ECDSASigner)(privateKey), 1, 1, digest123, 1, 0, 0)
	require.NoError(t, err)
	controlEvent := &state.ControlEvent{BAF: baf}
	consensus.SubmitRequest(controlEvent.Bytes())
	require.Eventually(t, func() bool {
		b := <-ledgerListener.c
		return b.Header.Number == uint64(1)
	}, 30*time.Second, 100*time.Millisecond)
}

type storageListener struct {
	c chan *common.Block
	f func()
}

func (l *storageListener) OnAppend(block *common.Block) {
	if l.f != nil {
		defer l.f()
	}
	l.c <- block
}
