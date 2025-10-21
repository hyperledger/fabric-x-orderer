/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/internal/pkg/identity/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"

	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/router"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func init() {
	// set the gRPC logger to a logger that discards the log output.
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))
}

func createRouterTestSetupFromConfig(t *testing.T, dir string, partyID types.PartyID, numOfShards int) *routerTestSetup {
	// Create a config YAML file in the temporary directory.
	configPath := filepath.Join(dir, "config.yaml")
	testutil.CreateNetwork(t, configPath, 1, numOfShards, "none", "none")

	// Generate the config files in the temporary directory using the armageddon generate command.
	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	logger := testutil.CreateLogger(t, 0)

	// create stub batchers
	var batchers []*stubBatcher
	var shards []node_config.ShardInfo

	fileStoreDir := t.TempDir()
	os.MkdirAll(fileStoreDir, 0o750)
	defer os.RemoveAll(fileStoreDir)

	for i := range numOfShards {
		config_file_path := filepath.Join(dir, fmt.Sprintf("config/party%d/local_config_batcher%d.yaml", partyID, i+1))
		localConfig, _, err := config.LoadLocalConfig(config_file_path)
		require.NoError(t, err)

		localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
		utils.WriteToYAML(localConfig.NodeLocalConfig, config_file_path)

		conf, configBlock, err := config.ReadConfig(config_file_path, logger)
		if err != nil {
			panic(fmt.Sprintf("error reading config, err: %s", err))
		}

		batcher_config := conf.ExtractBatcherConfig(configBlock)
		certKeyPair := &tlsgen.CertKeyPair{Cert: batcher_config.TLSCertificateFile, Key: batcher_config.TLSPrivateKeyFile}
		batcher := NewStubBatcher(t, certKeyPair, partyID, types.ShardID(i+1))
		batchers = append(batchers, &batcher)

		shards = append(shards, node_config.ShardInfo{
			ShardId:  types.ShardID(i + 1),
			Batchers: []node_config.BatcherInfo{{PartyID: 1, Endpoint: batcher.server.Address(), TLSCACerts: []node_config.RawBytes{certKeyPair.Cert}}},
		})
	}

	// start the batchers
	for _, batcher := range batchers {
		batcher.Start()
	}

	// modify the router config to use the file store dir
	config_file_path := filepath.Join(dir, fmt.Sprintf("config/party%d/local_config_router.yaml", partyID))
	localConfig, _, err := config.LoadLocalConfig(config_file_path)
	require.NoError(t, err)

	localConfig.NodeLocalConfig.FileStore.Path = fileStoreDir
	utils.WriteToYAML(localConfig.NodeLocalConfig, config_file_path)

	conf, configBlock, err := config.ReadConfig(config_file_path, logger)
	if err != nil {
		panic(fmt.Sprintf("error reading config, err: %s", err))
	}

	// create router config
	router_config := conf.ExtractRouterConfig(configBlock)
	router_config.ClientSignatureVerificationRequired = true
	router_config.Shards = shards

	// create and start router
	router := createAndStartRouterFromConfig(t, router_config)

	return &routerTestSetup{
		batchers: batchers,
		router:   router,
	}
}

// TestClientRouterBroadcastSignedRequests verifies that the router correctly accepts and
// processes signed broadcast requests from a client.
//
// The test performs the following high-level steps:
//   - Creates a temporary directory and initializes a router test environment for a single party.
//   - Establishes a non-TLS client connection to the router.
//   - Loads the client's private key and signing certificate from the generated MSP artifacts.
//   - Submits a batch of signed broadcast requests (10 requests) to the router using the client connection.
//   - Asserts that the submission returns no error and that the router's first batcher receives all
//     expected messages within the configured timeout.
func TestClientRouterBroadcastSignedRequests(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	testSetup := createRouterTestSetupFromConfig(t, dir, types.PartyID(1), 1)
	err = createNoTLSConnection(testSetup)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	defer testSetup.Close()

	keyBytes, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("crypto/ordererOrganizations/org%d/users/user/msp/keystore/priv_sk", types.PartyID(1))))
	require.NoError(t, err)

	certBytes, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("crypto/ordererOrganizations/org%d/users/user/msp/signcerts/sign-cert.pem", types.PartyID(1))))
	require.NoError(t, err)

	res := submitSignedBroadcastRequests(t, "org1", keyBytes, certBytes, testSetup.clientConn, 10)
	require.NoError(t, res.err)

	require.Eventually(t, func() bool {
		return testSetup.batchers[0].ReceivedMessageCount() == uint32(10)
	}, 100*time.Second, 10*time.Millisecond)
}

func submitSignedBroadcastRequests(t *testing.T, org string, keyBytes []byte, certBytes []byte, conn *grpc.ClientConn, numOfRequests int) (res testStreamResult) {
	res = testStreamResult{
		failRequests: numOfRequests,
	}

	pk, err := tx.CreateECDSASigner(keyBytes)
	require.NoError(t, err)

	cl := ab.NewAtomicBroadcastClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := cl.Broadcast(ctx)
	if err != nil {
		res.err = err
		return res
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for j := 0; j < numOfRequests; j++ {
			payload := tx.PrepareTxWithTimestamp(j, 64, []byte("sessionNumber"))
			env := tx.CreateSignedStructuredEnvelope(payload, pk, certBytes, org)
			err := stream.Send(env)
			if err != nil {
				return
			}
		}
	}()

	for j := 0; j < numOfRequests; j++ {
		select {
		default:
			resp, err := stream.Recv()
			if err != nil {
				res.err = fmt.Errorf("error receiving response: %s", err)
			}
			if resp.Status != common.Status_SUCCESS {
				requestErr := fmt.Errorf("receiving response with error: %s", resp.Info)
				res.respondsErrors = append(res.respondsErrors, requestErr)
				res.err = requestErr
			} else {
				res.successRequests++
				res.failRequests--
			}
		case <-ctx.Done():
			res.err = fmt.Errorf("a time out occured during submitting request: %w", ctx.Err())
		}
	}

	wg.Wait()

	return res
}

func createAndStartRouterFromConfig(t *testing.T, conf *node_config.RouterNodeConfig) *router.Router {
	logger := testutil.CreateLogger(t, 0)

	fakeSigner := &mocks.SignerSerializer{}
	configUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
	configUpdateProposer.ProposeConfigUpdateReturns(nil, nil)
	r := router.NewRouter(conf, logger, fakeSigner, configUpdateProposer)
	r.StartRouterService()

	return r
}
