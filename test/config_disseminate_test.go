/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
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

	assemblers, assemblersDir, assemblersLoggers, cleanAssemblers := createAssemblers(t, numParties, ca, shards, consenterInfos, genesisBlock)

	// cleanup
	defer func() {
		for i := range routers {
			routers[i].Stop()
		}
		cleanBatchers()
		cleanConsenters()
		cleanAssemblers()
	}()

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

	// submit data txs and make sure the assembler got them
	sendTransactions(t, routers, assemblers[0])

	// check the init size of the config store
	for i := range routers {
		require.Equal(t, 1, routers[i].GetConfigStoreSize())
		numbers, err := batchers[i].ConfigStore.ListBlockNumbers()
		require.NoError(t, err)
		require.Len(t, numbers, 1)
	}

	// create a config request and submit
	configReq := tx.CreateStructuredConfigUpdateRequest(payloadBytes)
	for i := range routers {
		routers[i].Submit(context.Background(), configReq)
	}

	// check config store size
	for i := range routers {
		require.Eventually(t, func() bool {
			numbers, err := batchers[i].ConfigStore.ListBlockNumbers()
			require.NoError(t, err)
			return routers[i].GetConfigStoreSize() == 2 && len(numbers) == 2
		}, 10*time.Second, 100*time.Millisecond)
	}

	// make sure router said it is stopping
	require.Eventually(t, func() bool {
		req := tx.CreateStructuredRequest([]byte{2})
		resp, err := routers[0].Submit(context.Background(), req)
		require.NoError(t, err)
		return strings.Contains(resp.Error, "router is stopping, cannot process request")
	}, 10*time.Second, 100*time.Millisecond)

	// check assembler appended to the ledger a config block
	time.Sleep(1 * time.Second)
	assemblers[3].Stop()
	al, err := ledger.NewAssemblerLedger(assemblersLoggers[3], assemblersDir[3])
	require.NoError(t, err)
	h := al.LedgerReader().Height()
	require.GreaterOrEqual(t, h, uint64(3)) // genesis block + at least one data block + config block
	lastBlock, err := al.LedgerReader().RetrieveBlockByNumber(h - 1)
	require.NoError(t, err)
	require.NotNil(t, lastBlock)
	require.True(t, protoutil.IsConfigBlock(lastBlock))
}
