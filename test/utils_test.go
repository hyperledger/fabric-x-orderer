/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	config "github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/node/router"
	configMocks "github.com/hyperledger/fabric-x-orderer/test/mocks"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/client"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
	pk      nodeconfig.RawBytes
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

func createRouters(t *testing.T, num int, batcherInfos []nodeconfig.BatcherInfo, ca tlsgen.CA, shardId types.ShardID) []*router.Router {
	var routers []*router.Router
	for i := 0; i < num; i++ {
		l := testutil.CreateLogger(t, i)
		kp, err := ca.NewServerCertKeyPair("127.0.0.1")
		require.NoError(t, err)

		bundle := &configMocks.FakeConfigResources{}
		configtxValidator := &policyMocks.FakeConfigtxValidator{}
		configtxValidator.ChannelIDReturns("arma")
		bundle.ConfigtxValidatorReturns(configtxValidator)

		config := &nodeconfig.RouterNodeConfig{
			ListenAddress:      "0.0.0.0:0",
			ConfigStorePath:    t.TempDir(),
			TLSPrivateKeyFile:  kp.Key,
			TLSCertificateFile: kp.Cert,
			PartyID:            types.PartyID(i + 1),
			Shards: []nodeconfig.ShardInfo{{
				ShardId:  shardId,
				Batchers: batcherInfos,
			}},
			UseTLS:                              true,
			RequestMaxBytes:                     1 << 10,
			ClientSignatureVerificationRequired: false,
			Bundle:                              bundle,
		}

		router := router.NewRouter(config, l)
		routers = append(routers, router)
	}

	return routers
}

func createConsenters(t *testing.T, num int, consenterNodes []*node, consenterInfos []nodeconfig.ConsenterInfo, shardInfo []nodeconfig.ShardInfo, genesisBlock *common.Block) ([]*consensus.Consensus, func()) {
	var consensuses []*consensus.Consensus
	var cleans []func()

	for i := 0; i < num; i++ {

		gRPCServer := consenterNodes[i].Server()

		partyID := types.PartyID(i + 1)

		logger := testutil.CreateLogger(t, int(partyID))

		sk, err := x509.MarshalPKCS8PrivateKey(consenterNodes[i].sk)
		require.NoError(t, err)

		dir, err := os.MkdirTemp("", fmt.Sprintf("%s-consenter%d", t.Name(), i+1))
		require.NoError(t, err)

		cleans = append(cleans, func() {
			defer os.RemoveAll(dir)
		})

		BFTConfig := config.DefaultArmaBFTConfig()
		BFTConfig.SelfID = uint64(partyID)

		conf := &nodeconfig.ConsenterNodeConfig{
			ListenAddress:      "0.0.0.0:0",
			Shards:             shardInfo,
			Consenters:         consenterInfos,
			PartyId:            partyID,
			TLSPrivateKeyFile:  consenterNodes[i].TLSKey,
			TLSCertificateFile: consenterNodes[i].TLSCert,
			SigningPrivateKey:  pem.EncodeToMemory(&pem.Block{Bytes: sk}),
			Directory:          dir,
			BFTConfig:          BFTConfig,
		}

		net := consenterNodes[i].GRPCServer
		signer := crypto.ECDSASigner(*consenterNodes[i].sk)
		c := consensus.CreateConsensus(conf, net, genesisBlock, logger, signer)

		consensuses = append(consensuses, c)
		protos.RegisterConsensusServer(gRPCServer, c)
		orderer.RegisterAtomicBroadcastServer(gRPCServer, c.DeliverService)
		orderer.RegisterClusterNodeServiceServer(gRPCServer, c)

		go consenterNodes[i].Start()
		err = c.Start()
		require.NoError(t, err)
		t.Log("Consenter gRPC service listening on", consenterNodes[i].Address())
	}

	return consensuses, func() {
		for i, clean := range cleans {
			clean()
			consensuses[i].Stop()
		}
	}
}

func createBatchersForShard(t *testing.T, num int, batcherNodes []*node, shards []nodeconfig.ShardInfo, consenterInfos []nodeconfig.ConsenterInfo, shardID types.ShardID) ([]*batcher.Batcher, []*nodeconfig.BatcherNodeConfig, []*zap.SugaredLogger, func()) {
	var batchers []*batcher.Batcher
	var loggers []*zap.SugaredLogger
	var configs []*nodeconfig.BatcherNodeConfig

	for i := 0; i < num; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("%s-batcher%d", t.Name(), i+1))
		require.NoError(t, err)

		key, err := x509.MarshalPKCS8PrivateKey(batcherNodes[i].sk)
		require.NoError(t, err)

		bundle := &configMocks.FakeConfigResources{}
		configtxValidator := &policyMocks.FakeConfigtxValidator{}
		configtxValidator.ChannelIDReturns("arma")
		bundle.ConfigtxValidatorReturns(configtxValidator)

		batcherConf := &nodeconfig.BatcherNodeConfig{
			ListenAddress:                       "0.0.0.0:0",
			Shards:                              shards,
			ShardId:                             shardID,
			ConfigStorePath:                     t.TempDir(),
			PartyId:                             types.PartyID(i + 1),
			Consenters:                          consenterInfos,
			TLSPrivateKeyFile:                   batcherNodes[i].TLSKey,
			TLSCertificateFile:                  batcherNodes[i].TLSCert,
			SigningPrivateKey:                   nodeconfig.RawBytes(pem.EncodeToMemory(&pem.Block{Bytes: key})),
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
		}

		configs = append(configs, batcherConf)

		logger := testutil.CreateLogger(t, i+int(shardID)*10)
		loggers = append(loggers, logger)
		signer := crypto.ECDSASigner(*batcherNodes[i].sk)

		batcher := batcher.CreateBatcher(batcherConf, logger, batcherNodes[i], &batcher.ConsensusStateReplicatorFactory{}, &batcher.ConsenterControlEventSenderFactory{}, signer)
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

func createBatcherNodesAndInfo(t *testing.T, ca tlsgen.CA, num int) ([]*node, []nodeconfig.BatcherInfo) {
	nodes := createNodes(t, num, ca)

	var batchersInfo []nodeconfig.BatcherInfo
	for i := 0; i < num; i++ {
		batchersInfo = append(batchersInfo, nodeconfig.BatcherInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   nodes[i].Address(),
			TLSCert:    nodes[i].TLSCert,
			TLSCACerts: []nodeconfig.RawBytes{ca.CertBytes()},
			PublicKey:  nodes[i].pk,
		})
	}

	return nodes, batchersInfo
}

func createConsenterNodesAndInfo(t *testing.T, ca tlsgen.CA, num int) ([]*node, []nodeconfig.ConsenterInfo) {
	nodes := createNodes(t, num, ca)

	var consentersInfo []nodeconfig.ConsenterInfo
	for i := 0; i < num; i++ {
		consentersInfo = append(consentersInfo, nodeconfig.ConsenterInfo{
			PartyID:    types.PartyID(i + 1),
			Endpoint:   nodes[i].Address(),
			TLSCACerts: []nodeconfig.RawBytes{ca.CertBytes()},
			PublicKey:  nodes[i].pk,
		})
	}

	return nodes, consentersInfo
}

func createNodes(t *testing.T, num int, ca tlsgen.CA) []*node {
	var result []*node
	var sks []*ecdsa.PrivateKey
	var pks []nodeconfig.RawBytes

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

func recoverBatcher(t *testing.T, ca tlsgen.CA, logger *zap.SugaredLogger, conf *nodeconfig.BatcherNodeConfig, batcherNode *node) *batcher.Batcher {
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

	batcher := batcher.CreateBatcher(conf, logger, newBatcherNode, &batcher.ConsensusStateReplicatorFactory{}, &batcher.ConsenterControlEventSenderFactory{}, signer)
	batcher.Run()

	gRPCServer := newBatcherNode.Server()
	protos.RegisterRequestTransmitServer(gRPCServer, batcher)
	protos.RegisterBatcherControlServiceServer(gRPCServer, batcher)
	orderer.RegisterAtomicBroadcastServer(gRPCServer, batcher)

	go func() {
		err := newBatcherNode.Start()
		if err != nil {
			panic(err)
		}
	}()

	return batcher
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
	Status           *common.Status
	Verifier         *crypto.ECDSAVerifier
}

func PullFromAssemblers(t *testing.T, options *BlockPullerOptions) map[types.PartyID]*BlockPullerInfo {
	require.NotNil(t, options)
	require.NotNil(t, options.UserConfig)
	require.NotEmpty(t, options.Parties)
	require.GreaterOrEqual(t, options.StartBlock, uint64(0))
	require.GreaterOrEqual(t, options.EndBlock, uint64(0))
	require.GreaterOrEqual(t, options.Transactions, 0)
	require.GreaterOrEqual(t, options.Blocks, 0)

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
				options.Blocks, options.Timeout, options.NeedVerification, options.Verifier)
			lock.Lock()
			defer lock.Unlock()
			pullInfos[partyID] = pullInfo

			require.True(t, err != nil && options.ErrString != "" || options.ErrString == "" && err == nil)

			if options.ErrString != "" {
				errString := fmt.Sprintf(options.ErrString, partyID)
				require.ErrorContains(t, err, errString)
			}

			if options.Status != nil {
				require.Equal(t, *options.Status, pullInfo.Status)
			}
			require.GreaterOrEqual(t, int64(pullInfo.TotalTxs), int64(options.Transactions))
			require.GreaterOrEqual(t, int64(pullInfo.TotalBlocks), int64(options.Blocks))
			require.Empty(t, pullInfo.Missing)
		}()
	}

	waitForPullDone.Wait()
	return pullInfos
}

func pullFromAssembler(t *testing.T, userConfig *armageddon.UserConfig, partyID types.PartyID,
	startBlock uint64, endBlock uint64, fval, transactions, blocks, timeout int,
	needVerification bool, sigVerifier *crypto.ECDSAVerifier,
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
	for i := 0; i < transactions; i++ {
		m[uint64(i)] = 0
	}

	handler := func(block *common.Block) error {
		if block == nil {
			return errors.New("nil block")
		}
		if block.Header == nil {
			return errors.New("nil block header")
		}

		totalBlocks++

		data := block.GetData().GetData()
		transactionsNumber := len(data)

		// Check if the block is genesis block or not
		isGenesisBlock := block.Header.Number == 0 || block.Header.GetDataHash() == nil

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
		}

		for i := 0; i < transactionsNumber; i++ {
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
	status, err := dc.PullBlocks(toCtx, partyID, startBlock, endBlock, handler)
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

func BuildVerifier(consenters []nodeconfig.ConsenterInfo, logger types.Logger) crypto.ECDSAVerifier {
	verifier := make(crypto.ECDSAVerifier)
	for _, ci := range consenters {
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

	return verifier
}
