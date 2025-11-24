/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	node2 "github.com/hyperledger/fabric-x-orderer/node"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestConfigDisseminate(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	numParties := 4

	batcherNodes, batcherInfos := createBatcherNodesAndInfo(t, ca, numParties)
	consenterNodes, consenterInfos := createConsenterNodesAndInfo(t, ca, numParties)

	shards := []config.ShardInfo{{ShardId: 1, Batchers: batcherInfos}}

	genesisBlock := utils.EmptyGenesisBlock("arma")

	consenters, cleanConsenters := createConsenters(t, numParties, consenterNodes, consenterInfos, shards, genesisBlock)

	batchers, _, _, cleanBatchers := createBatchersForShard(t, numParties, batcherNodes, shards, consenterInfos, shards[0].ShardId, genesisBlock)

	routers, certs := createRouters(t, numParties, batcherInfos, ca, shards[0].ShardId, consenterNodes[0].Address(), genesisBlock)

	for i := range routers {
		routers[i].StartRouterService()
	}

	// update consensus router config
	for i := range consenters {
		consenters[i].Config.Router = config.RouterInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   routers[i].Address(),
			TLSCACerts: nil,
			TLSCert:    certs[i],
		}
	}

	// update mock config update proposer
	payloadBytes := []byte{1}
	for i := range consenters {
		configRequestEnvelope := tx.CreateStructuredConfigEnvelope(payloadBytes)
		configRequest := &protos.Request{
			Payload:   configRequestEnvelope.Payload,
			Signature: configRequestEnvelope.Signature,
		}
		mockConfigUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
		mockConfigUpdateProposer.ProposeConfigUpdateReturns(configRequest, nil)
		consenters[i].ConfigUpdateProposer = mockConfigUpdateProposer
	}

	// create the assembler
	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	assemblerDir, err := os.MkdirTemp("", fmt.Sprintf("%s-assembler", t.Name()))
	require.NoError(t, err)

	assemblerConf := &config.AssemblerNodeConfig{
		TLSPrivateKeyFile:         ckp.Key,
		TLSCertificateFile:        ckp.Cert,
		PartyId:                   1,
		Directory:                 assemblerDir,
		ListenAddress:             "0.0.0.0:0",
		PrefetchBufferMemoryBytes: 1 * 1024 * 1024 * 1024, // 1GB
		RestartLedgerScanTimeout:  5 * time.Second,
		PrefetchEvictionTtl:       time.Hour,
		PopWaitMonitorTimeout:     time.Second,
		ReplicationChannelSize:    100,
		BatchRequestsChannelSize:  1000,
		Shards:                    shards,
		Consenter:                 consenterInfos[0],
		UseTLS:                    true,
		ClientAuthRequired:        false,
	}

	aLogger := testutil.CreateLogger(t, 1)

	var appendedConfigWG sync.WaitGroup
	appendedConfigWG.Add(2) // once for genesis and once for config
	baseLogger := aLogger.Desugar()
	aLogger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Appended config block") {
			appendedConfigWG.Done()
		}
		return nil
	})).Sugar()

	assemblerGRPC := node2.CreateGRPCAssembler(assemblerConf)

	assembler := assembler.NewAssembler(assemblerConf, assemblerGRPC, genesisBlock, aLogger)

	orderer.RegisterAtomicBroadcastServer(assemblerGRPC.Server(), assembler)

	go func() {
		assemblerGRPC.Start()
	}()

	// cleanup
	defer func() {
		for i := range routers {
			routers[i].Stop()
		}
		cleanBatchers()
		cleanConsenters()
		assembler.Stop()
	}()

	// submit data txs and make sure the assembler got them
	sendTransactions(t, routers, assembler)

	// check the init size of the config store
	require.Equal(t, 1, routers[0].GetConfigStoreSize())
	numbers, err := batchers[0].ConfigStore.ListBlockNumbers()
	require.NoError(t, err)
	require.Len(t, numbers, 1)

	// create a config request and submit
	configReq := tx.CreateStructuredConfigUpdateRequest(payloadBytes)
	for i := range routers {
		routers[i].Submit(context.Background(), configReq)
	}

	// make sure assembler said it appended the config block
	appendedConfigWG.Wait()

	// make sure router said it is stopping
	require.Eventually(t, func() bool {
		req := tx.CreateStructuredRequest([]byte{2})
		resp, err := routers[0].Submit(context.Background(), req)
		require.NoError(t, err)
		return strings.Contains(resp.Error, "router is stopping, cannot process request")
	}, 10*time.Second, 100*time.Millisecond)

	// check config store size
	require.Equal(t, 2, routers[0].GetConfigStoreSize())
	numbers, err = batchers[0].ConfigStore.ListBlockNumbers()
	require.NoError(t, err)
	require.Len(t, numbers, 2)

	// check assembler appended to the ledger a config block
	assembler.Stop()
	al, err := ledger.NewAssemblerLedger(aLogger, assemblerDir)
	require.NoError(t, err)
	h := al.LedgerReader().Height()
	require.GreaterOrEqual(t, h, uint64(3)) // genesis block + at least one data block + config block
	lastBlock, err := al.LedgerReader().RetrieveBlockByNumber(h - 1)
	require.NoError(t, err)
	require.NotNil(t, lastBlock)
	require.True(t, protoutil.IsConfigBlock(lastBlock))
}
