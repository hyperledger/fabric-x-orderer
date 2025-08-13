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
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/ledger/blockledger"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/badb"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/crypto"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/protobuf/proto"
)

func CreateConsensus(conf *config.ConsenterNodeConfig, net Net, genesisBlock *common.Block, logger arma_types.Logger, signer Signer) *Consensus {
	logger.Infof("Creating consensus, party: %d, address: %s, with genesis block: %t", conf.PartyId, conf.ListenAddress, genesisBlock != nil)

	var currentNodes []uint64
	for _, node := range conf.Consenters {
		currentNodes = append(currentNodes, uint64(node.PartyID))
	}

	consLedger, err := ledger.NewConsensusLedger(conf.Directory)
	if err != nil {
		logger.Panicf("Failed creating consensus ledger: %s", err)
	}

	initialState, metadata, lastProposal, lastSigs := getInitialStateAndMetadata(logger, conf, genesisBlock, consLedger)

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
			State:           initialState,
			DB:              badb,
			Logger:          logger,
			BAFDeserializer: &state.BAFDeserialize{},
		},
		BADB:         badb,
		Logger:       logger,
		State:        initialState,
		CurrentNodes: currentNodes,
		Storage:      consLedger,
		SigVerifier:  buildVerifier(conf.Consenters, conf.Shards, logger),
		Signer:       signer,
	}

	c.BFT = createBFT(c, metadata, lastProposal, lastSigs, conf.WALDir)
	setupComm(c)
	c.Synchronizer = createSynchronizer(consLedger, c)
	c.BFT.Synchronizer = c.Synchronizer

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

func getInitialStateAndMetadata(logger arma_types.Logger, config *config.ConsenterNodeConfig, genesisBlock *common.Block, ledger *ledger.ConsensusLedger) (*state.State, *smartbftprotos.ViewMetadata, *smartbft_types.Proposal, []smartbft_types.Signature) {
	height := ledger.Height()
	logger.Infof("Initial consenter ledger height is: %d", height)
	if height == 0 {
		initState := initialStateFromConfig(config)
		if genesisBlock == nil {
			panic(fmt.Sprintf("Error creating Consensus%d, genesis block is nil", config.PartyId))
		}
		appendGenesisBlock(genesisBlock, initState, ledger)
		return initState, &smartbftprotos.ViewMetadata{}, nil, nil
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

	return header.State, md, &proposal, sigs
}

func initialStateFromConfig(config *config.ConsenterNodeConfig) *state.State {
	var initState state.State
	initState.ShardCount = uint16(len(config.Shards))
	initState.N = uint16(len(config.Consenters))
	F := (uint16(initState.N) - 1) / 3
	initState.Threshold = F + 1
	initState.Quorum = uint16(math.Ceil((float64(initState.N) + float64(F) + 1) / 2.0))

	for _, shard := range config.Shards {
		initState.Shards = append(initState.Shards, state.ShardTerm{
			Shard: arma_types.ShardID(shard.ShardId),
			Term:  0,
		})
	}

	sort.Slice(initState.Shards, func(i, j int) bool {
		return int(initState.Shards[i].Shard) < int(initState.Shards[j].Shard)
	})

	// TODO set right initial app context
	initialAppContext := &state.BlockHeader{
		Number:   0, // We want the first block to start with 0, this is how we signal bootstrap
		PrevHash: nil,
		Digest:   nil,
	}
	initState.AppContext = initialAppContext.Bytes()

	return &initState
}

func appendGenesisBlock(genesisBlock *common.Block, initState *state.State, ledger *ledger.ConsensusLedger) {
	genesisBlocks := make([]state.AvailableBlock, 1)
	genesisDigest := protoutil.ComputeBlockDataHash(genesisBlock.GetData())

	genesisBlocks[0] = state.AvailableBlock{
		Header: &state.BlockHeader{
			Number:   0,
			PrevHash: nil,
			Digest:   genesisDigest,
		},
		Batch: state.NewAvailableBatch(0, arma_types.ShardIDConsensus, 0, genesisDigest),
	}

	var lastBlockHeader state.BlockHeader
	if err := lastBlockHeader.FromBytes(initState.AppContext); err != nil {
		panic(fmt.Sprintf("Failed deserializing app context to BlockHeader from initial state: %v", err))
	}
	lastBlockHeader.Digest = genesisDigest
	initState.AppContext = lastBlockHeader.Bytes()

	genesisProposal := smartbft_types.Proposal{
		Payload: protoutil.MarshalOrPanic(genesisBlock), // TODO create a correct payload
		Header: (&state.Header{
			AvailableBlocks: genesisBlocks,
			State:           initState,
			Num:             0,
		}).Serialize(),
		Metadata: nil, // TODO maybe use this metadata
	}

	ledger.Append(state.DecisionToBytes(genesisProposal, nil))
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
