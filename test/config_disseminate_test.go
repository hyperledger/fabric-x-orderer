/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/common/util"
	"github.com/hyperledger/fabric-x-common/tools/pkg/identity"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
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

	consenters, consentersConfigs, consentersLoggers, _ := createConsenters(t, numParties, consenterNodes, consenterInfos, shards, genesisBlock)

	batchers, batchersConfigs, batchersLoggers, _ := createBatchersForShard(t, numParties, batcherNodes, shards, consenterInfos, shards[0].ShardId, genesisBlock)

	routers, certs, routersConfigs, routersLoggers := createRouters(t, numParties, batcherInfos, ca, shards[0].ShardId, consenterNodes[0].Address(), genesisBlock)

	for i := range routers {
		routers[i].StartRouterService()
	}

	assemblers, assemblersDir, assemblersConfigs, assemblersLoggers, _ := createAssemblers(t, numParties, ca, shards, consenterInfos, genesisBlock)

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

	// check config store size in routers and batchers
	for i := range routers {
		require.Eventually(t, func() bool {
			routerConfigCount := routers[i].GetConfigStoreSize()

			batcherConfigCount, err := batchers[i].ConfigStore.ListBlockNumbers()
			require.NoError(t, err)

			return routerConfigCount == 2 && len(batcherConfigCount) == 2
		}, 10*time.Second, 100*time.Millisecond)
	}

	// make sure router said it is stopping
	req := tx.CreateStructuredRequest([]byte{2})
	require.Eventually(t, func() bool {
		resp, err := routers[0].Submit(context.Background(), req)
		require.NoError(t, err)
		return strings.Contains(resp.Error, "router is stopping, cannot process request")
	}, 10*time.Second, 100*time.Millisecond)

	// make sure batcher said it is stopping
	batchers[0].Submit(context.Background(), req)
	require.Eventually(t, func() bool {
		_, err := batchers[0].Submit(context.Background(), req)
		require.Error(t, err)
		return strings.Contains(err.Error(), "batcher is stopped")
	}, 10*time.Second, 100*time.Millisecond)

	// make sure consenter said it is stopping
	require.Eventually(t, func() bool {
		baf := types.NewSimpleBatchAttestationFragment(types.ShardID(1), types.PartyID(1), types.BatchSequence(1), []byte{2}, types.PartyID(1), 0)
		ce := &state.ControlEvent{BAF: baf}
		err := consenters[0].SubmitRequest(ce.Bytes())
		require.Error(t, err)
		return strings.Contains(err.Error(), "consensus is soft-stopped")
	}, 10*time.Second, 100*time.Millisecond)

	// check all assemblers appended to the ledger a config block
	time.Sleep(1 * time.Second)
	var lastBlock *common.Block
	for i := range assemblers {
		assemblers[i].Stop()

		al, err := ledger.NewAssemblerLedger(assemblersLoggers[i], assemblersDir[i])
		require.NoError(t, err)

		h := al.LedgerReader().Height()
		require.GreaterOrEqual(t, h, uint64(3)) // genesis block + at least one data block + config block

		lastBlock, err = al.LedgerReader().RetrieveBlockByNumber(h - 1)
		require.NoError(t, err)
		require.True(t, protoutil.IsConfigBlock(lastBlock))

		al.Close()
	}

	// stop all nodes and recover them
	for i := range routers {
		routers[i].Stop()
		batchers[i].Stop()
		consenters[i].Stop()
	}

	for i := range routers {
		batchers[i] = recoverBatcher(t, ca, batchersLoggers[i], batchersConfigs[i], batcherNodes[i])
		consenters[i] = recoverConsenter(t, consenterNodes[i], ca, lastBlock, consentersConfigs[i], consentersLoggers[i])
		assemblers[i] = recoverAssembler(assemblersConfigs[i], assemblersLoggers[i])

		routers[i] = recoverRouters(routersConfigs[i], routersLoggers[i])
		routers[i].StartRouterService()
	}

	// check router and batcher config store after recovery
	for i := range routers {
		require.Eventually(t, func() bool {
			routerConfigCount := routers[i].GetConfigStoreSize()

			batcherConfigCount, err := batchers[i].ConfigStore.ListBlockNumbers()
			require.NoError(t, err)

			return routerConfigCount == 2 && len(batcherConfigCount) == 2
		}, 10*time.Second, 100*time.Millisecond)
	}

	// submit data txs and make sure the assembler receives them after recovery
	sendTransactions(t, routers, assemblers[0])

	for i := range routers {
		routers[i].Stop()
		batchers[i].Stop()
		consenters[i].Stop()
		assemblers[i].Stop()
	}
}

func TestConfigTXDisseminationWithVerification(t *testing.T) {
	// Compile Arma
	armaBinaryPath, err := gexec.BuildWithEnvironment("github.com/hyperledger/fabric-x-orderer/cmd/arma", []string{"GOPRIVATE=" + os.Getenv("GOPRIVATE")})
	defer gexec.CleanupBuildArtifacts()
	require.NoError(t, err)
	require.NotNil(t, armaBinaryPath)

	// Generate the configuration with clientSignatureVerificationRequired = True
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	numOfParties := 4
	numOfShards := 2
	orgNum := 1
	netInfo := testutil.CreateNetwork(t, configPath, numOfParties, numOfShards, "mTLS", "mTLS")
	require.NoError(t, err)
	numOfArmaNodes := len(netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir, "--clientSignatureVerificationRequired"})

	// Run Arma nodes
	// NOTE: if one of the nodes is not started within 10 seconds, there is no point in continuing the test, so fail it
	readyChan := make(chan struct{}, numOfArmaNodes)
	armaNetwork := testutil.RunArmaNodes(t, dir, armaBinaryPath, readyChan, netInfo)
	testutil.WaitReady(t, readyChan, numOfArmaNodes, 10)

	// Create a broadcast client
	uc, err := testutil.GetUserConfig(dir, 1)
	assert.NoError(t, err)
	assert.NotNil(t, uc)

	broadcastClient := client.NewBroadcastTxClient(uc, 10*time.Second)
	defer broadcastClient.Stop()

	// Prepare a Config TX, i.e. an envelope signed by an admin of org1
	// the envelope.Payload contains marshaled bytes of configUpdateEnvelope, which is an envelope with Header.Type = HeaderType_CONFIG_UPDATE, signed by majority of admins

	// Create an admin signer serves as the client which signs the config transaction
	keyPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", orgNum), "users", "admin", "msp", "keystore", "priv_sk")
	signer, err := CreateSigner(keyPath)
	require.NoError(t, err)
	require.NotNil(t, signer)

	certPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", orgNum), "users", "admin", "msp", "signcerts", fmt.Sprintf("Admin@Org%d-cert.pem", orgNum))
	certBytes, err := os.ReadFile(certPath)
	require.NoError(t, err)
	require.NotNil(t, certBytes)

	// Create the config transaction
	genesisBlockPath := filepath.Join(dir, "bootstrap/bootstrap.block")
	env := createConfigTX(t, dir, numOfParties, genesisBlockPath, signer, certBytes, fmt.Sprintf("org%d", orgNum))
	require.NotNil(t, env)

	// Send the config tx
	err = broadcastClient.SendTx(env)
	require.NoError(t, err)

	// Pull from assembler
	parties := []types.PartyID{}
	for partyID := 1; partyID <= numOfParties; partyID++ {
		parties = append(parties, types.PartyID(partyID))
	}

	startBlock := uint64(0)
	endBlock := uint64(1)

	PullFromAssemblers(t, &BlockPullerOptions{
		UserConfig: uc,
		Parties:    parties,
		StartBlock: startBlock,
		EndBlock:   endBlock,
		Blocks:     2,
		ErrString:  "cancelled pull from assembler: %d",
	})

	// Check config store size of routers
	for i := 0; i < numOfParties; i++ {
		localConfigPath := armaNetwork.GetRouter(t, types.PartyID(i+1)).RunInfo.NodeConfigPath
		localConfig := testutil.ReadNodeConfigFromYaml(t, localConfigPath)
		require.Eventually(t, func() bool {
			configStore, err := configstore.NewStore(localConfig.FileStore.Path)
			require.NoError(t, err)
			listBlockNumbers, err := configStore.ListBlockNumbers()
			require.NoError(t, err)
			routerConfigCount := len(listBlockNumbers)
			return routerConfigCount == 2
		}, 60*time.Second, 100*time.Millisecond)
	}

	// Check config store size of batchers
	for i := 0; i < numOfParties; i++ {
		for j := 0; j < numOfShards; j++ {
			batcher := armaNetwork.GetBatcher(t, types.PartyID(i+1), types.ShardID(j+1))
			localConfigPath := batcher.RunInfo.NodeConfigPath
			localConfig := testutil.ReadNodeConfigFromYaml(t, localConfigPath)
			require.Eventually(t, func() bool {
				configStore, err := configstore.NewStore(localConfig.FileStore.Path)
				require.NoError(t, err)
				listBlockNumbers, err := configStore.ListBlockNumbers()
				require.NoError(t, err)
				batcherConfigCount := len(listBlockNumbers)
				return batcherConfigCount == 2
			}, 60*time.Second, 100*time.Millisecond)
		}
	}

	armaNetwork.Stop()

	// Check ledger height of consenters
	for i := 0; i < numOfParties; i++ {
		localConfigPath := armaNetwork.GetConsenter(t, types.PartyID(i+1)).RunInfo.NodeConfigPath
		localConfig := testutil.ReadNodeConfigFromYaml(t, localConfigPath)
		require.Eventually(t, func() bool {
			consensusLedger, err := ledger.NewConsensusLedger(localConfig.FileStore.Path)
			require.NoError(t, err)
			defer consensusLedger.Close()
			consensusLedgerCount := consensusLedger.Height()
			return consensusLedgerCount == 2
		}, 60*time.Second, 100*time.Millisecond)
	}
}

func CreateSigner(privateKeyPath string) (*crypto.ECDSASigner, error) {
	// Read the private key
	keyBytes, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key, err: %v", err)
	}

	// Create a ECDSA Singer
	privateKey, err := tx.CreateECDSAPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create ECDSA Signer, err: %v", err)
	}

	return (*crypto.ECDSASigner)(privateKey), nil
}

func createConfigTX(t *testing.T, dir string, numOfParties int, genesisBlockPath string, signer identity.SignerSerializer, signerCert []byte, org string) *common.Envelope {
	// Create ConfigUpdateBytes
	configUpdatePbBytes := CreateConfigUpdate(t, dir, genesisBlockPath)
	require.NotNil(t, configUpdatePbBytes)

	// create ConfigUpdateEnvelope
	configUpdateEnvelope := &common.ConfigUpdateEnvelope{
		ConfigUpdate: configUpdatePbBytes,
		Signatures:   []*common.ConfigSignature{},
	}

	// sign with majority admins (for 4 parties the majority is 3)
	for i := 0; i < (numOfParties/2)+1; i++ {
		// Read admin of organization i
		keyPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", i+1), "users", "admin", "msp", "keystore", "priv_sk")
		adminSigner, err := CreateSigner(keyPath)
		require.NoError(t, err)
		require.NotNil(t, adminSigner)

		certPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", i+1), "users", "admin", "msp", "signcerts", fmt.Sprintf("Admin@Org%d-cert.pem", i+1))
		adminCertBytes, err := os.ReadFile(certPath)
		require.NoError(t, err)
		require.NotNil(t, adminCertBytes)

		sId := &msp.SerializedIdentity{
			Mspid:   fmt.Sprintf("org%d", i+1),
			IdBytes: adminCertBytes,
		}

		sigHeader, err := protoutil.NewSignatureHeader(adminSigner)
		require.NoError(t, err)

		sigHeader.Creator = protoutil.MarshalOrPanic(sId)

		configSig := &common.ConfigSignature{
			SignatureHeader: protoutil.MarshalOrPanic(sigHeader),
		}
		configSig.Signature, err = adminSigner.Sign(util.ConcatenateBytes(configSig.SignatureHeader, configUpdateEnvelope.ConfigUpdate))
		require.NoError(t, err)

		configUpdateEnvelope.Signatures = append(configUpdateEnvelope.Signatures, configSig)
	}

	configUpdateEnvelopeBytes, err := proto.Marshal(configUpdateEnvelope)
	require.NoError(t, err)

	// Wrap the ConfigUpdateEnvelope with an Envelope signed by the admin
	payload := tx.CreatePayloadWithConfigUpdate(configUpdateEnvelopeBytes, signerCert, org)
	require.NotNil(t, payload)
	env, err := tx.CreateSignedEnvelope(payload, signer)
	require.NoError(t, err)
	require.NotNil(t, env)

	return env
}
