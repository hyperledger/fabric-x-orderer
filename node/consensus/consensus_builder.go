/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-orderer/common/configrulesverifier"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blockledger"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/badb"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/configrequest"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/protobuf/proto"
)

func CreateConsensus(conf *config.ConsenterNodeConfig, net NetStopper, lastConfigBlock *common.Block, logger arma_types.Logger, signer Signer, configUpdateProposer policy.ConfigUpdateProposer) *Consensus {
	if lastConfigBlock == nil {
		logger.Panicf("Error creating Consensus%d, last config block is nil", conf.PartyId)
		return nil
	}

	if lastConfigBlock.Header == nil {
		logger.Panicf("Error creating Consensus%d, last config block header is nil", conf.PartyId)
		return nil
	}

	logger.Infof("Creating consensus, party: %d, address: %s, with last config block number: %d", conf.PartyId, conf.ListenAddress, lastConfigBlock.Header.Number)

	var currentNodes []uint64
	for _, node := range conf.Consenters {
		currentNodes = append(currentNodes, uint64(node.PartyID))
	}

	consLedger, err := ledger.NewConsensusLedger(conf.Directory)
	if err != nil {
		logger.Panicf("Failed creating consensus ledger: %s", err)
	}

	initialState, metadata, lastProposal, lastSigs, decisionNumOfLastConfigBlock := getInitialStateAndMetadata(logger, conf, lastConfigBlock, consLedger)

	dbDir := filepath.Join(conf.Directory, "batchDB")
	os.MkdirAll(dbDir, 0o755)

	badb, err := badb.NewBatchAttestationDB(dbDir, logger)
	if err != nil {
		logger.Panicf("Failed creating Batch attestation DB: %v", err)
	}

	c := &Consensus{
		DeliverService: delivery.DeliverService(map[string]blockledger.Reader{"consensus": consLedger}),
		Net:            net,
		Config:         conf,
		BFTConfig:      conf.BFTConfig,
		Arma: &Consenter{
			DB:              badb,
			Logger:          logger,
			BAFDeserializer: &state.BAFDeserialize{},
		},
		BADB:                         badb,
		Logger:                       logger,
		State:                        initialState,
		lastConfigBlockNum:           lastConfigBlock.Header.Number,
		decisionNumOfLastConfigBlock: decisionNumOfLastConfigBlock,
		CurrentNodes:                 currentNodes,
		Storage:                      consLedger,
		SigVerifier:                  buildVerifier(conf.Consenters, conf.Shards, logger),
		Signer:                       signer,
		Metrics:                      NewConsensusMetrics(conf, consLedger.Height(), logger),
		RequestVerifier:              CreateConsensusRulesVerifier(conf),
		ConfigUpdateProposer:         configUpdateProposer,
		ConfigApplier:                &DefaultConfigApplier{},
		ConfigRequestValidator: &configrequest.DefaultValidateConfigRequest{
			ConfigUpdateProposer: configUpdateProposer,
			Bundle:               conf.Bundle,
		},
		ConfigRulesVerifier: &configrulesverifier.DefaultConfigRulesVerifier{},
	}

	c.BFT = createBFT(c, metadata, lastProposal, lastSigs, conf.WALDir)
	setupComm(c)
	sync := createSynchronizer(consLedger, c)
	c.BFT.Synchronizer = sync
	c.Synchronizer = sync

	return c
}

func createBFT(c *Consensus, m *smartbftprotos.ViewMetadata, lastProposal *smartbft_types.Proposal, lastSigs []smartbft_types.Signature, walPath string) *consensus.Consensus {
	walDir := walPath
	if walDir == "" {
		walDir = filepath.Join(c.Config.Directory, "wal")
	}

	bftWAL, walInitState, err := wal.InitializeAndReadAll(c.Logger, walDir, wal.DefaultOptions())
	if err != nil {
		c.Logger.Panicf("Failed creating BFT WAL: %v", err)
	}

	bft := &consensus.Consensus{
		Config:            c.BFTConfig,
		Application:       c,
		Assembler:         c,
		WAL:               bftWAL,
		WALInitialContent: walInitState,
		Signer:            c,
		Verifier:          c,
		RequestInspector:  c,
		Logger:            c.Logger,
		Metadata:          m,
		Scheduler:         time.NewTicker(time.Second).C,
		ViewChangerTicker: time.NewTicker(time.Second).C,
	}
	if lastProposal != nil {
		bft.LastProposal = *lastProposal
		bft.LastSignatures = lastSigs
	}
	return bft
}

func createSynchronizer(ledger *ledger.ConsensusLedger, c *Consensus) *synchronizer {
	synchronizer := &synchronizer{
		deliver: func(proposal smartbft_types.Proposal, signatures []smartbft_types.Signature) {
			c.Deliver(proposal, signatures)
		},
		getHeight: func() uint64 {
			return ledger.Height()
		},
		getBlock: func(seq uint64) *common.Block {
			block, err := ledger.RetrieveBlockByNumber(seq)
			if err != nil {
				panic(err)
			}
			return block
		},
		pruneRequestsFromMemPool: func(req []byte) {
			c.BFT.Pool.RemoveRequest(c.RequestID(req))
		},
		memStore: make(map[uint64]*common.Block),
		cc:       c.clientConfig(),
		logger:   c.Logger,
		endpoint: c.pickEndpoint,
		nextSeq: func() uint64 {
			return ledger.Height()
		},
		latestCommittedBlock: ledger.Height() - 1,
		BFTConfig:            c.BFTConfig,
		CurrentNodes:         c.CurrentNodes,
	}

	ledger.RegisterAppendListener(synchronizer)

	if len(c.CurrentNodes) > 1 { // don't run the synchronizer when there is only one node
		defer func() {
			go synchronizer.run()
		}()
	}

	return synchronizer
}

func buildVerifier(consenterInfos []config.ConsenterInfo, shardInfo []config.ShardInfo, logger arma_types.Logger) crypto.ECDSAVerifier {
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

		verifier[crypto.ShardPartyKey{Shard: arma_types.ShardIDConsensus, Party: arma_types.PartyID(ci.PartyID)}] = *pk4.(*ecdsa.PublicKey)
	}

	for _, shard := range shardInfo {
		for _, bi := range shard.Batchers {
			pk := bi.PublicKey

			pk3, _ := pem.Decode(pk)
			if pk == nil {
				logger.Panicf("Failed decoding batcher public key")
			}

			pk4, err := x509.ParsePKIXPublicKey(pk3.Bytes)
			if err != nil {
				logger.Panicf("Failed parsing batcher public key: %v", err)
			}

			verifier[crypto.ShardPartyKey{Shard: arma_types.ShardID(shard.ShardId), Party: arma_types.PartyID(bi.PartyID)}] = *pk4.(*ecdsa.PublicKey)
		}
	}

	return verifier
}

func getInitialStateAndMetadata(logger arma_types.Logger, config *config.ConsenterNodeConfig, lastConfigBlock *common.Block, ledger *ledger.ConsensusLedger) (*state.State, *smartbftprotos.ViewMetadata, *smartbft_types.Proposal, []smartbft_types.Signature, arma_types.DecisionNum) {
	height := ledger.Height()
	logger.Infof("Initial consenter ledger height is: %d", height)
	if height == 0 {
		initState := initialStateFromConfig(config)
		if lastConfigBlock == nil {
			panic(fmt.Sprintf("Error creating Consensus%d, genesis block is nil", config.PartyId))
		}
		appendGenesisBlock(lastConfigBlock, initState, ledger)
		return initState, &smartbftprotos.ViewMetadata{}, nil, nil, 0
	}

	block, err := ledger.RetrieveBlockByNumber(height - 1)
	if err != nil {
		panic("couldn't retrieve last block from ledger")
	}

	proposal, sigs, err := state.BytesToDecision(block.Data.Data[0])
	if err != nil {
		panic("couldn't read decision from last block")
	}

	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(proposal.Metadata, md); err != nil {
		panic(err)
	}

	header := &state.Header{}
	if err := header.Deserialize(proposal.Header); err != nil {
		panic(err)
	}

	return header.State, md, &proposal, sigs, header.DecisionNumOfLastConfigBlock
}

func initialStateFromConfig(config *config.ConsenterNodeConfig) *state.State {
	var initState state.State
	initState.ShardCount = uint16(len(config.Shards))
	initState.N = uint16(len(config.Consenters))
	_, T, Q := utils.ComputeFTQ(initState.N)
	initState.Threshold = T
	initState.Quorum = Q

	for _, shard := range config.Shards {
		initState.Shards = append(initState.Shards, state.ShardTerm{
			Shard: arma_types.ShardID(shard.ShardId),
			Term:  0,
		})
	}

	sort.Slice(initState.Shards, func(i, j int) bool {
		return int(initState.Shards[i].Shard) < int(initState.Shards[j].Shard)
	})

	initialAppContext := &common.BlockHeader{
		Number:       0, // We want the first block to start with 0, this is how we signal bootstrap
		PreviousHash: nil,
		DataHash:     nil,
	}
	initState.AppContext = protoutil.MarshalOrPanic(initialAppContext)

	return &initState
}

func appendGenesisBlock(genesisBlock *common.Block, initState *state.State, consensusLedger *ledger.ConsensusLedger) {
	genesisDigest := protoutil.ComputeBlockDataHash(genesisBlock.GetData())

	lastCommonBlockHeader := &common.BlockHeader{}
	if err := proto.Unmarshal(initState.AppContext, lastCommonBlockHeader); err != nil {
		panic(fmt.Sprintf("Failed unmarshaling app context to BlockHeader from initial state: %v", err))
	}

	lastCommonBlockHeader.DataHash = genesisDigest

	initState.AppContext = protoutil.MarshalOrPanic(lastCommonBlockHeader)

	availableCommonBlocks := []*common.Block{genesisBlock}

	protoutil.InitBlockMetadata(availableCommonBlocks[0])

	blockMetadata, err := ledger.AssemblerBlockMetadataToBytes(state.NewAvailableBatch(0, arma_types.ShardIDConsensus, 0, genesisDigest), &state.OrderingInformation{DecisionNum: 0, BatchCount: 1, BatchIndex: 0}, 1)
	if err != nil {
		panic("failed to invoke AssemblerBlockMetadataToBytes")
	}

	availableCommonBlocks[0].Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = blockMetadata

	genesisProposal := smartbft_types.Proposal{
		Payload: protoutil.MarshalOrPanic(genesisBlock),
		Header: (&state.Header{
			AvailableCommonBlocks: availableCommonBlocks,
			State:                 initState,
			Num:                   0,
		}).Serialize(),
		Metadata: nil,
	}

	consensusLedger.Append(state.DecisionToBytes(genesisProposal, nil))
}

func (c *Consensus) clientConfig() comm.ClientConfig {
	var tlsCAs [][]byte

	for _, ci := range c.Config.Consenters {
		for _, tlsCACert := range ci.TLSCACerts {
			tlsCAs = append(tlsCAs, tlsCACert)
		}
	}

	cert := c.Config.TLSCertificateFile

	tlsKey := c.Config.TLSPrivateKeyFile

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               tlsKey,
			Certificate:       cert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}

func setupComm(c *Consensus) {
	selfID := getSelfID(c.Config.Consenters, c.Config.PartyId)
	c.ClusterService = &comm.ClusterService{
		Logger:                           c.Logger,
		CertExpWarningThreshold:          time.Hour,
		NodeIdentity:                     selfID,
		StepLogger:                       c.Logger,
		MinimumExpirationWarningInterval: time.Hour,
		RequestHandler:                   c,
	}

	var consenterConfigs []*common.Consenter
	var remotesNodes []comm.RemoteNode
	for _, node := range c.Config.Consenters {
		var tlsCAs [][]byte
		for _, caCert := range node.TLSCACerts {
			tlsCAs = append(tlsCAs, caCert)
		}

		identity := node.PublicKey

		remotesNodes = append(remotesNodes, comm.RemoteNode{
			NodeCerts: comm.NodeCerts{
				Identity:     identity,
				ServerRootCA: tlsCAs,
			},
			NodeAddress: comm.NodeAddress{
				ID:       uint64(node.PartyID),
				Endpoint: node.Endpoint,
			},
		})
		consenterConfigs = append(consenterConfigs, &common.Consenter{
			Identity: identity,
			Id:       uint32(node.PartyID),
		})
	}
	c.ConfigureNodeCerts(consenterConfigs)

	commAuth := &comm.AuthCommMgr{
		Logger:         c.Logger,
		Signer:         c.Signer,
		SendBufferSize: 2000,
		NodeIdentity:   selfID,
		Connections:    comm.NewConnectionMgr(c.clientConfig()),
	}

	commAuth.Configure(remotesNodes)

	c.BFT.Comm = &comm.Egress{
		NodeList: c.CurrentNodes,
		Logger:   c.Logger,
		RPC: &comm.RPC{
			StreamsByType: comm.NewStreamsByType(),
			Timeout:       time.Minute,
			Logger:        c.Logger,
			Comm:          commAuth,
		},
	}
}

func getSelfID(consenterInfos []config.ConsenterInfo, partyID arma_types.PartyID) []byte {
	var myIdentity []byte
	for _, ci := range consenterInfos {
		pk := ci.PublicKey

		if ci.PartyID == partyID {
			myIdentity = pk
			break
		}
	}
	return myIdentity
}

func CreateConsensusRulesVerifier(config *config.ConsenterNodeConfig) *requestfilter.RulesVerifier {
	rv := requestfilter.NewRulesVerifier(nil)
	rv.AddRule(requestfilter.PayloadNotEmptyRule{})
	rv.AddRule(requestfilter.NewMaxSizeFilter(config))
	rv.AddStructureRule(requestfilter.NewSigFilter(config, policies.ChannelWriters))
	return rv
}
