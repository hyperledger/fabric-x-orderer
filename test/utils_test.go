/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-common/protoutil/identity/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
	ordererRulesMocks "github.com/hyperledger/fabric-x-orderer/config/verify/mocks"
	node2 "github.com/hyperledger/fabric-x-orderer/node"
	"github.com/hyperledger/fabric-x-orderer/node/assembler"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	consensusMocks "github.com/hyperledger/fabric-x-orderer/node/consensus/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/node/router"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

//go:generate counterfeiter -o mocks/config_resources.go . configResources
type configResources channelconfig.Resources

var _ configResources = &configMocks.FakeConfigResources{}

type node struct {
	*comm.GRPCServer
	TLSCert []byte
	TLSKey  []byte
	sk      *ecdsa.PrivateKey
	pk      node_config.RawBytes
}

func (n *node) ToString() string {
	return fmt.Sprintf("GRPC.Address: %s", n.GRPCServer.Address())
}

func newGRPCServer(addr string, ca tlsgen.CA, kp *tlsgen.CertKeyPair) (*comm.GRPCServer, error) {
	return comm.NewGRPCServer(addr, comm.ServerConfig{
		SecOpts: comm.SecureOptions{
			ClientRootCAs:     [][]byte{ca.CertBytes()},
			Key:               kp.Key,
			Certificate:       kp.Cert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     [][]byte{ca.CertBytes()},
		},
	})
}

func keygen(t *testing.T) (*ecdsa.PrivateKey, []byte) {
	sk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	rawPK, err := x509.MarshalPKIXPublicKey(&sk.PublicKey)
	require.NoError(t, err)
	return sk, rawPK
}

func createRouters(t *testing.T, num int, batcherInfos []node_config.BatcherInfo, ca tlsgen.CA, shardId types.ShardID, consenterEndpoint []string, genesisBlock *common.Block) ([]*router.Router, []node_config.RawBytes, []*node_config.RouterNodeConfig, []*flogging.FabricLogger) {
	var routers []*router.Router
	var certs []node_config.RawBytes
	var configs []*node_config.RouterNodeConfig
	var loggers []*flogging.FabricLogger
	for i := 0; i < num; i++ {
		l := testutil.CreateLogger(t, i)
		loggers = append(loggers, l)
		kp, err := ca.NewServerCertKeyPair("127.0.0.1")
		require.NoError(t, err)

		bundle := &configMocks.FakeConfigResources{}
		configtxValidator := &policyMocks.FakeConfigtxValidator{}
		configtxValidator.ChannelIDReturns("arma")
		bundle.ConfigtxValidatorReturns(configtxValidator)
		policy := &policyMocks.FakePolicyEvaluator{}
		policy.EvaluateSignedDataReturns(nil)
		policyManager := &policyMocks.FakePolicyManager{}
		policyManager.GetPolicyReturns(policy, true)
		bundle.PolicyManagerReturns(policyManager)

		fakeSigner := &mocks.SignerSerializer{}

		fileStorePath := t.TempDir()
		cs, err := configstore.NewStore(fileStorePath)
		require.NoError(t, err)
		require.NoError(t, cs.Add(genesisBlock))

		config := &node_config.RouterNodeConfig{
			ListenAddress:           "0.0.0.0:0",
			MonitoringListenAddress: "127.0.0.1:0",
			MetricsLogInterval:      5 * time.Second,
			FileStorePath:           fileStorePath,
			TLSPrivateKeyFile:       kp.Key,
			TLSCertificateFile:      kp.Cert,
			PartyID:                 types.PartyID(i + 1),
			Shards: []node_config.ShardInfo{{
				ShardId:  shardId,
				Batchers: batcherInfos,
			}},
			UseTLS:                              true,
			RequestMaxBytes:                     1 << 10,
			ClientSignatureVerificationRequired: false,
			Bundle:                              bundle,
			Consenter:                           node_config.ConsenterInfo{PartyID: types.PartyID(i + 1), Endpoint: consenterEndpoint[i], TLSCACerts: []node_config.RawBytes{ca.CertBytes()}},
		}
		configs = append(configs, config)

		certs = append(certs, kp.Cert)

		configUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
		req := &protos.Request{}
		configUpdateProposer.ProposeConfigUpdateReturns(req, nil)

		configRulesVerifier := &ordererRulesMocks.FakeOrdererRules{}
		configRulesVerifier.ValidateNewConfigReturns(nil)
		configRulesVerifier.ValidateTransitionReturns(nil)

		router := router.NewRouter(config, l, fakeSigner, configUpdateProposer, configRulesVerifier)
		routers = append(routers, router)
	}

	return routers, certs, configs, loggers
}

func createAssemblers(t *testing.T, num int, ca tlsgen.CA, shards []node_config.ShardInfo, consenterInfos []node_config.ConsenterInfo, genesisBlock *common.Block) ([]*assembler.Assembler, []string, []*node_config.AssemblerNodeConfig, []*flogging.FabricLogger, func()) {
	var assemblerDirs []string
	var assemblers []*assembler.Assembler
	var loggers []*flogging.FabricLogger
	var configs []*node_config.AssemblerNodeConfig

	for i := 0; i < num; i++ {
		ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
		require.NoError(t, err)

		assemblerDir := t.TempDir()
		assemblerDirs = append(assemblerDirs, assemblerDir)

		assemblerConf := &node_config.AssemblerNodeConfig{
			TLSPrivateKeyFile:         ckp.Key,
			TLSCertificateFile:        ckp.Cert,
			PartyId:                   types.PartyID(i + 1),
			Directory:                 assemblerDir,
			ListenAddress:             "0.0.0.0:0",
			PrefetchBufferMemoryBytes: 1 * 1024 * 1024 * 1024, // 1GB
			RestartLedgerScanTimeout:  5 * time.Second,
			PrefetchEvictionTtl:       time.Hour,
			PopWaitMonitorTimeout:     time.Second,
			ReplicationChannelSize:    100,
			BatchRequestsChannelSize:  1000,
			Shards:                    shards,
			Consenter:                 consenterInfos[i],
			UseTLS:                    true,
			ClientAuthRequired:        false,
			MonitoringListenAddress:   "127.0.0.1:0",
			MetricsLogInterval:        5 * time.Second,
			Bundle:                    testutil.CreateAssemblerBundleForTest(0),
		}
		configs = append(configs, assemblerConf)

		logger := testutil.CreateLogger(t, i+1)
		loggers = append(loggers, logger)

		assemblerGRPC := node2.CreateGRPCAssembler(assemblerConf)

		assembler := assembler.NewAssembler(assemblerConf, assemblerGRPC, genesisBlock, logger)
		assemblers = append(assemblers, assembler)

		orderer.RegisterAtomicBroadcastServer(assemblerGRPC.Server(), assembler)

		go func() {
			assemblerGRPC.Start()
		}()
	}

	return assemblers, assemblerDirs, configs, loggers, func() {
		for i := range assemblers {
			assemblers[i].Stop()
		}
	}
}

func createConsenters(t *testing.T, num int, consenterNodes []*node, consenterInfos []node_config.ConsenterInfo, shardInfo []node_config.ShardInfo, genesisBlock *common.Block) ([]*consensus.Consensus, []*node_config.ConsenterNodeConfig, []*flogging.FabricLogger, func()) {
	var consensuses []*consensus.Consensus
	var loggers []*flogging.FabricLogger
	var configs []*node_config.ConsenterNodeConfig

	for i := 0; i < num; i++ {

		gRPCServer := consenterNodes[i].Server()

		partyID := types.PartyID(i + 1)

		logger := testutil.CreateLogger(t, int(partyID))
		loggers = append(loggers, logger)

		sk, err := x509.MarshalPKCS8PrivateKey(consenterNodes[i].sk)
		require.NoError(t, err)

		dir := t.TempDir()

		BFTConfig := config.DefaultArmaBFTConfig()
		BFTConfig.SelfID = uint64(partyID)

		bundle := &configMocks.FakeConfigResources{}
		configtxValidator := &policyMocks.FakeConfigtxValidator{}
		configtxValidator.ChannelIDReturns("arma")
		bundle.ConfigtxValidatorReturns(configtxValidator)

		policy := &policyMocks.FakePolicyEvaluator{}
		policy.EvaluateSignedDataReturns(nil)
		policyManager := &policyMocks.FakePolicyManager{}
		policyManager.GetPolicyReturns(policy, true)
		bundle.PolicyManagerReturns(policyManager)

		conf := &node_config.ConsenterNodeConfig{
			ListenAddress:                       "0.0.0.0:0",
			Shards:                              shardInfo,
			Consenters:                          consenterInfos,
			PartyId:                             partyID,
			TLSPrivateKeyFile:                   consenterNodes[i].TLSKey,
			TLSCertificateFile:                  consenterNodes[i].TLSCert,
			SigningPrivateKey:                   pem.EncodeToMemory(&pem.Block{Bytes: sk}),
			Directory:                           dir,
			BFTConfig:                           BFTConfig,
			Bundle:                              bundle,
			ClientSignatureVerificationRequired: false,
			RequestMaxBytes:                     1000,
			MonitoringListenAddress:             "127.0.0.1:0",
			MetricsLogInterval:                  5 * time.Second,
		}
		configs = append(configs, conf)

		net := consenterNodes[i].GRPCServer
		signer := crypto.ECDSASigner(*consenterNodes[i].sk)

		mockConfigUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
		mockConfigUpdateProposer.ProposeConfigUpdateReturns(nil, nil)

		c := consensus.CreateConsensus(conf, net, genesisBlock, logger, signer, mockConfigUpdateProposer)
		mockConfigApplier := &consensusMocks.FakeConfigApplier{}
		mockConfigApplier.ApplyConfigToStateCalls(func(s *state.State, request *state.ConfigRequest) (*state.State, error) {
			return s, nil
		})
		c.ConfigApplier = mockConfigApplier

		consensuses = append(consensuses, c)
		protos.RegisterConsensusServer(gRPCServer, c)
		orderer.RegisterAtomicBroadcastServer(gRPCServer, c.DeliverService)
		orderer.RegisterClusterNodeServiceServer(gRPCServer, c)

		go consenterNodes[i].Start()
		err = c.Start()
		require.NoError(t, err)
		t.Log("Consenter gRPC service listening on", consenterNodes[i].Address())
	}

	return consensuses, configs, loggers, func() {
		for i := range consensuses {
			consensuses[i].Stop()
		}
	}
}

func createBatchersForShard(t *testing.T, num int, batcherNodes []*node, shards []node_config.ShardInfo, consenterInfos []node_config.ConsenterInfo, shardID types.ShardID, genesisBlock *common.Block) ([]*batcher.Batcher, []*node_config.BatcherNodeConfig, []*flogging.FabricLogger, func()) {
	var batchers []*batcher.Batcher
	var loggers []*flogging.FabricLogger
	var configs []*node_config.BatcherNodeConfig

	for i := 0; i < num; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("%s-batcher%d", t.Name(), i+1))
		require.NoError(t, err)

		key, err := x509.MarshalPKCS8PrivateKey(batcherNodes[i].sk)
		require.NoError(t, err)

		bundle := &configMocks.FakeConfigResources{}
		configtxValidator := &policyMocks.FakeConfigtxValidator{}
		configtxValidator.ChannelIDReturns("arma")
		bundle.ConfigtxValidatorReturns(configtxValidator)

		configStorePath := t.TempDir()
		cs, err := configstore.NewStore(configStorePath)
		require.NoError(t, err)
		require.NoError(t, cs.Add(genesisBlock))

		batcherConf := &node_config.BatcherNodeConfig{
			ListenAddress:                       "0.0.0.0:0",
			Shards:                              shards,
			ShardId:                             shardID,
			ConfigStorePath:                     configStorePath,
			PartyId:                             types.PartyID(i + 1),
			Consenters:                          consenterInfos,
			TLSPrivateKeyFile:                   batcherNodes[i].TLSKey,
			TLSCertificateFile:                  batcherNodes[i].TLSCert,
			SigningPrivateKey:                   node_config.RawBytes(pem.EncodeToMemory(&pem.Block{Bytes: key})),
			Directory:                           dir,
			MemPoolMaxSize:                      1000000,
			BatchMaxSize:                        10000,
			BatchMaxBytes:                       1024 * 1024 * 10,
			RequestMaxBytes:                     1024 * 1024,
			SubmitTimeout:                       time.Millisecond * 500,
			FirstStrikeThreshold:                time.Second * 10,
			SecondStrikeThreshold:               time.Second * 10,
			AutoRemoveTimeout:                   time.Second * 10,
			BatchCreationTimeout:                time.Millisecond * 500,
			BatchSequenceGap:                    types.BatchSequence(10),
			ClientSignatureVerificationRequired: false,
			Bundle:                              bundle,
			MonitoringListenAddress:             "127.0.0.1:0",
			MetricsLogInterval:                  3 * time.Second,
		}

		configs = append(configs, batcherConf)

		logger := testutil.CreateLogger(t, i+int(shardID)*10)
		loggers = append(loggers, logger)
		signer := crypto.ECDSASigner(*batcherNodes[i].sk)

		batcher := batcher.CreateBatcher(batcherConf, logger, batcherNodes[i], &batcher.ConsensusDecisionReplicatorFactory{}, &batcher.ConsenterControlEventSenderFactory{}, signer)
		batchers = append(batchers, batcher)
		batcher.Run()

		protos.RegisterRequestTransmitServer(batcherNodes[i].Server(), batcher)
		protos.RegisterBatcherControlServiceServer(batcherNodes[i].Server(), batcher)
		orderer.RegisterAtomicBroadcastServer(batcherNodes[i].Server(), batcher)

		go func() {
			err := batcherNodes[i].Start()
			if err != nil {
				panic(err)
			}
		}()

		t.Log("Batcher gRPC service listening on", batcherNodes[i].Address())
	}

	return batchers, configs, loggers, func() {
		for _, b := range batchers {
			b.Stop()
		}
	}
}

func createBatcherNodesAndInfo(t *testing.T, ca tlsgen.CA, num int) ([]*node, []node_config.BatcherInfo) {
	nodes := createNodes(t, num, ca)

	var batchersInfo []node_config.BatcherInfo
	for i := 0; i < num; i++ {
		batchersInfo = append(batchersInfo, node_config.BatcherInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   nodes[i].Address(),
			TLSCert:    nodes[i].TLSCert,
			TLSCACerts: []node_config.RawBytes{ca.CertBytes()},
			PublicKey:  nodes[i].pk,
		})
	}

	return nodes, batchersInfo
}

func createConsenterNodesAndInfo(t *testing.T, ca tlsgen.CA, num int) ([]*node, []node_config.ConsenterInfo) {
	nodes := createNodes(t, num, ca)

	var consentersInfo []node_config.ConsenterInfo
	for i := 0; i < num; i++ {
		consentersInfo = append(consentersInfo, node_config.ConsenterInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   nodes[i].Address(),
			TLSCACerts: []node_config.RawBytes{ca.CertBytes()},
			PublicKey:  nodes[i].pk,
		})
	}

	return nodes, consentersInfo
}

func createNodes(t *testing.T, num int, ca tlsgen.CA) []*node {
	var result []*node
	var sks []*ecdsa.PrivateKey
	var pks []node_config.RawBytes

	for i := 0; i < num; i++ {
		sk, rawPK := keygen(t)
		sks = append(sks, sk)
		pks = append(pks, pem.EncodeToMemory(&pem.Block{Bytes: rawPK, Type: "PUBLIC KEY"}))
	}

	for i := 0; i < num; i++ {
		kp, err := ca.NewServerCertKeyPair("127.0.0.1")
		require.NoError(t, err)
		srv, err := newGRPCServer("127.0.0.1:0", ca, kp)
		require.NoError(t, err)

		result = append(result, &node{GRPCServer: srv, TLSKey: kp.Key, TLSCert: kp.Cert, pk: pks[i], sk: sks[i]})
	}

	return result
}

func recoverBatcher(t *testing.T, ca tlsgen.CA, conf *node_config.BatcherNodeConfig, batcherNode *node, logger *flogging.FabricLogger) *batcher.Batcher {
	newBatcherNode := &node{
		TLSCert: batcherNode.TLSCert,
		TLSKey:  batcherNode.TLSKey,
		sk:      batcherNode.sk,
		pk:      batcherNode.pk,
	}
	var err error

	kp := &tlsgen.CertKeyPair{
		Key:  batcherNode.TLSKey,
		Cert: batcherNode.TLSCert,
	}

	newBatcherNode.GRPCServer, err = newGRPCServer(batcherNode.Address(), ca, kp)
	require.NoError(t, err)
	signer := crypto.ECDSASigner(*newBatcherNode.sk)

	batcher := batcher.CreateBatcher(conf, logger, newBatcherNode, &batcher.ConsensusDecisionReplicatorFactory{}, &batcher.ConsenterControlEventSenderFactory{}, signer)
	batcher.Run()

	gRPCServer := newBatcherNode.Server()
	protos.RegisterRequestTransmitServer(gRPCServer, batcher)
	protos.RegisterBatcherControlServiceServer(gRPCServer, batcher)
	orderer.RegisterAtomicBroadcastServer(gRPCServer, batcher)

	go func() {
		err := newBatcherNode.Start()
		require.NoError(t, err)
	}()

	return batcher
}

func recoverConsenter(t *testing.T, ca tlsgen.CA, conf *node_config.ConsenterNodeConfig, consenterNode *node, logger *flogging.FabricLogger, lastConfigBlock *common.Block) *consensus.Consensus {
	newConsenterNode := &node{
		TLSCert: consenterNode.TLSCert,
		TLSKey:  consenterNode.TLSKey,
		sk:      consenterNode.sk,
		pk:      consenterNode.pk,
	}
	var err error

	kp := &tlsgen.CertKeyPair{
		Key:  newConsenterNode.TLSKey,
		Cert: newConsenterNode.TLSCert,
	}

	newConsenterNode.GRPCServer, err = newGRPCServer(consenterNode.Address(), ca, kp)
	require.NoError(t, err)
	signer := crypto.ECDSASigner(*newConsenterNode.sk)

	mockConfigUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
	mockConfigUpdateProposer.ProposeConfigUpdateReturns(nil, nil)

	consenter := consensus.CreateConsensus(conf, newConsenterNode.GRPCServer, lastConfigBlock, logger, signer, mockConfigUpdateProposer)
	mockConfigApplier := &consensusMocks.FakeConfigApplier{}
	mockConfigApplier.ApplyConfigToStateCalls(func(s *state.State, request *state.ConfigRequest) (*state.State, error) {
		return s, nil
	})
	consenter.ConfigApplier = mockConfigApplier

	gRPCServer := newConsenterNode.Server()
	protos.RegisterConsensusServer(gRPCServer, consenter)
	orderer.RegisterAtomicBroadcastServer(gRPCServer, consenter.DeliverService)
	orderer.RegisterClusterNodeServiceServer(gRPCServer, consenter)

	go func() {
		err := newConsenterNode.Start()
		require.NoError(t, err)
	}()

	err = consenter.Start()
	require.NoError(t, err)

	return consenter
}

func recoverAssembler(t *testing.T, conf *node_config.AssemblerNodeConfig, logger *flogging.FabricLogger, lastConfigBlock *common.Block) *assembler.Assembler {
	assemblerGRPC := node2.CreateGRPCAssembler(conf)
	assembler := assembler.NewAssembler(conf, assemblerGRPC, lastConfigBlock, logger)

	orderer.RegisterAtomicBroadcastServer(assemblerGRPC.Server(), assembler)

	go func() {
		err := assemblerGRPC.Start()
		require.NoError(t, err)
	}()

	return assembler
}

func recoverRouter(conf *node_config.RouterNodeConfig, logger *flogging.FabricLogger) *router.Router {
	bundle := &configMocks.FakeConfigResources{}
	configtxValidator := &policyMocks.FakeConfigtxValidator{}
	configtxValidator.ChannelIDReturns("arma")
	bundle.ConfigtxValidatorReturns(configtxValidator)
	fakeSigner := &mocks.SignerSerializer{}

	configUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
	req := &protos.Request{}
	configUpdateProposer.ProposeConfigUpdateReturns(req, nil)

	configRulesVerifier := &ordererRulesMocks.FakeOrdererRules{}
	configRulesVerifier.ValidateNewConfigReturns(nil)
	configRulesVerifier.ValidateTransitionReturns(nil)

	router := router.NewRouter(conf, logger, fakeSigner, configUpdateProposer, configRulesVerifier)
	router.StartRouterService()

	return router
}

func sendTxn(workerID int, txnNum int, routers []*router.Router) {
	txn := make([]byte, 32)
	binary.BigEndian.PutUint64(txn, uint64(txnNum))
	binary.BigEndian.PutUint16(txn[30:], uint16(workerID))

	for routerId := 0; routerId < len(routers); routerId++ {
		req := tx.CreateStructuredRequest(txn)
		routers[routerId].Submit(context.Background(), req)
	}
}

type BlockPullerInfo struct {
	TotalTxs    int
	TotalBlocks int
	Primary     map[types.ShardID]types.PartyID
	TermChanged bool
	Duplicate   []uint64
	Missing     []uint64
	Status      common.Status
}

// TestBlockHandler defines an interface for performing an action by an user on a block.
type TestBlockHandler interface {
	HandleBlock(t *testing.T, block *common.Block) error
}

type BlockPullerOptions struct {
	UserConfig       *armageddon.UserConfig
	Parties          []types.PartyID
	NeedVerification bool
	StartBlock       uint64
	EndBlock         uint64
	Transactions     int
	Blocks           int
	Timeout          int
	ErrString        string
	LogString        string
	Status           *common.Status
	Verifier         *crypto.ECDSAVerifier
	BlockHandler     TestBlockHandler
	Signer           identity.SignerSerializer
}

func PullFromAssemblers(t *testing.T, options *BlockPullerOptions) map[types.PartyID]*BlockPullerInfo {
	require.NotNil(t, options)
	require.NotNil(t, options.UserConfig)
	require.NotEmpty(t, options.Parties)
	require.GreaterOrEqual(t, options.StartBlock, uint64(0))
	require.GreaterOrEqual(t, options.EndBlock, uint64(0))
	require.GreaterOrEqual(t, options.Transactions, 0)
	require.GreaterOrEqual(t, options.Blocks, 0)

	var buf bytes.Buffer
	// Set log output to the buffer
	log.SetOutput(&buf)
	defer log.SetOutput(nil) // Reset log output at the end of the test

	log.SetFlags(0) // Disable log flags like date/time

	if options.Timeout <= 0 {
		options.Timeout = 30
	}

	if options.EndBlock == 0 {
		options.EndBlock = math.MaxUint64
	}

	var waitForPullDone sync.WaitGroup
	pullInfos := make(map[types.PartyID]*BlockPullerInfo, len(options.Parties))
	lock := sync.Mutex{}

	for _, partyID := range options.Parties {
		waitForPullDone.Add(1)

		go func() {
			defer waitForPullDone.Done()

			pullInfo, err := pullFromAssembler(t, options.UserConfig, partyID, options.StartBlock, options.EndBlock, (len(options.Parties)-1)/3, options.Transactions,
				options.Blocks, options.Timeout, options.NeedVerification, options.Verifier, options.BlockHandler, options.Signer)
			lock.Lock()
			defer lock.Unlock()
			pullInfos[partyID] = pullInfo

			require.True(t, err != nil && options.ErrString != "" || options.ErrString == "" && err == nil)

			if len(options.ErrString) > 0 {
				errString := fmt.Sprintf(options.ErrString, partyID)
				require.ErrorContains(t, err, errString)
			}

			if options.Status != nil {
				require.Equal(t, *options.Status, pullInfo.Status)
			}
			require.GreaterOrEqual(t, int64(pullInfo.TotalTxs), int64(options.Transactions))
			require.GreaterOrEqual(t, int64(pullInfo.TotalBlocks), int64(options.Blocks))
			require.Empty(t, pullInfo.Missing)

			if len(options.LogString) > 0 {
				logOutput := buf.String()
				logString := fmt.Sprintf(options.LogString, partyID)
				require.Contains(t, logOutput, logString, "Expected log output to contain specified log string")
			}
		}()
	}

	waitForPullDone.Wait()
	return pullInfos
}

func pullFromAssembler(t *testing.T, userConfig *armageddon.UserConfig, partyID types.PartyID,
	startBlock uint64, endBlock uint64, fval, transactions, blocks, timeout int,
	needVerification bool, sigVerifier *crypto.ECDSAVerifier, blockHandler TestBlockHandler, signer identity.SignerSerializer,
) (*BlockPullerInfo, error) {
	dc := client.NewDeliverClient(userConfig)
	toCtx, toCancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer toCancel()

	totalTxs := 0
	totalBlocks := 0
	primaryMap := make(map[types.ShardID]types.PartyID)
	termChanged := false

	expectedNumOfTxs := transactions + 1
	expectedNumOfBlocks := blocks

	m := make(map[uint64]int, transactions)
	for i := range transactions {
		m[uint64(i)] = 0
	}

	handler := func(block *common.Block) error {
		if block == nil {
			return errors.New("nil block")
		}
		if block.Header == nil {
			return errors.New("nil block header")
		}

		if blockHandler != nil {
			if err := blockHandler.HandleBlock(t, block); err != nil {
				toCancel()
			}
		}

		totalBlocks++

		data := block.GetData().GetData()
		transactionsNumber := len(data)

		// Check if the block is genesis block or not
		isGenesisBlock := block.Header.Number == 0 || block.Header.GetDataHash() == nil
		isConfigBlock := protoutil.IsConfigBlock(block)

		if !isGenesisBlock {
			shardIDBytes := block.GetMetadata().GetMetadata()[common.BlockMetadataIndex_ORDERER][2:4]
			shardID := types.ShardID(binary.BigEndian.Uint16(shardIDBytes))
			primaryIDBytes := block.GetMetadata().GetMetadata()[common.BlockMetadataIndex_ORDERER][0:2]
			primaryID := types.PartyID(binary.BigEndian.Uint16(primaryIDBytes))

			if pr, ok := primaryMap[shardID]; !ok {
				primaryMap[shardID] = primaryID
			} else if pr != primaryID {
				t.Logf("primary id changed from %d to %d", pr, primaryID)
				termChanged = true
				primaryMap[shardID] = primaryID
			}
		} else if needVerification {
			require.Equal(t, 1, transactionsNumber)
			totalTxs++
			t.Log("skipping genesis block")
			return nil
		}

		if sigVerifier != nil && !isGenesisBlock {
			bhdr := &common.BlockHeader{Number: block.Header.Number, DataHash: block.Header.DataHash, PreviousHash: block.Header.PreviousHash}
			sigsBytes := block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES]
			md := &common.Metadata{}
			if err := proto.Unmarshal(sigsBytes, md); err != nil {
				return errors.Wrapf(err, "error unmarshalling signatures from metadata: %v", err)
			}

			verifiedSigns := 0
			for _, metadataSignature := range md.Signatures {
				identifierHeader, err := protoutil.UnmarshalIdentifierHeader(metadataSignature.IdentifierHeader)
				if err != nil {
					return fmt.Errorf("failed unmarshalling identifier header for block %d: %v", block.Header.GetNumber(), err)
				}
				if err = sigVerifier.VerifySignature(types.PartyID(identifierHeader.Identifier), types.ShardIDConsensus, protoutil.BlockHeaderBytes(bhdr), metadataSignature.GetSignature()); err != nil {
					t.Logf("failed verifying signature for block %d: %v", block.Header.GetNumber(), err)
					continue
				}

				verifiedSigns++
			}

			if verifiedSigns < 2*fval+1 {
				return fmt.Errorf("not enough signatures: got %d, need at least %d (2*f + 1)", verifiedSigns, 2*fval+1)
			}

			if isConfigBlock {
				log.Printf("configuration block %d partyID %d verified with %d signatures\n", block.Header.Number, partyID, verifiedSigns)
			}
		}

		if isConfigBlock && needVerification {
			require.Equal(t, 1, transactionsNumber)
			// skip config block txs
			m[uint64(len(m)-1)]++
			totalTxs++
			return nil
		}

		for i := range transactionsNumber {
			envelope, err := protoutil.UnmarshalEnvelope(data[i])
			if err != nil {
				return errors.Wrapf(err, "failed to unmarshal envelope %v", err)
			}

			if needVerification && transactions > 0 {
				data, err := tx.GetDataFromEnvelope(envelope)
				require.NoError(t, err)
				require.NotNil(t, data)
				txNumber := binary.BigEndian.Uint64(data[0:8])
				// count only unique txs
				if m[txNumber] == 0 {
					totalTxs++
				}
				m[txNumber]++
			} else {
				totalTxs++
			}
		}

		if blocks > 0 && totalBlocks >= expectedNumOfBlocks {
			toCancel()
		}
		if transactions > 0 && totalTxs >= expectedNumOfTxs {
			toCancel()
		}

		return nil
	}

	t.Logf("Pulling from party: %d\n", partyID)
	status, err := dc.PullBlocks(toCtx, partyID, startBlock, endBlock, handler, signer)
	t.Logf("Finished pull and count: blocks %d, txs %d from party: %d\n", totalBlocks, totalTxs, partyID)
	blockPullerInfo := &BlockPullerInfo{TotalTxs: totalTxs, TotalBlocks: totalBlocks, Primary: primaryMap, TermChanged: termChanged, Missing: make([]uint64, 0), Duplicate: make([]uint64, 0), Status: status}

	if needVerification {
		for k, v := range m {
			if v == 0 {
				blockPullerInfo.Missing = append(blockPullerInfo.Missing, k)
			} else if v > 1 {
				blockPullerInfo.Duplicate = append(blockPullerInfo.Duplicate, k)
			}
		}
	}
	return blockPullerInfo, err
}

func BuildVerifier(configDir string, partyID types.PartyID, logger *flogging.FabricLogger) *crypto.ECDSAVerifier {
	localConfig, _, err := config.LoadLocalConfig(filepath.Join(configDir, fmt.Sprintf("config/party%d/local_config_consenter.yaml", int(partyID))))
	if err != nil {
		logger.Panicf("Failed loading local config: %v", err)
	}
	sharedConfig, _, err := config.LoadSharedConfig(filepath.Join(configDir, "bootstrap/shared_config.yaml"))
	if err != nil {
		logger.Panicf("Failed loading shared config: %v", err)
	}

	conf := &config.Configuration{
		LocalConfig:  localConfig,
		SharedConfig: sharedConfig,
	}
	consenterInfos := conf.ExtractConsenters()

	verifier := make(crypto.ECDSAVerifier)
	for _, ci := range consenterInfos {
		pk, _ := pem.Decode(ci.PublicKey)
		if pk == nil || pk.Bytes == nil {
			logger.Panicf("Failed decoding consenter public key")
		}

		pk4, err := x509.ParsePKIXPublicKey(pk.Bytes)
		if err != nil {
			logger.Panicf("Failed parsing consenter public key: %v", err)
		}

		verifier[crypto.ShardPartyKey{Shard: types.ShardIDConsensus, Party: types.PartyID(ci.PartyID)}] = *pk4.(*ecdsa.PublicKey)
	}

	return &verifier
}
