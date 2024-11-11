package consensus

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"math"
	"os"
	"path/filepath"
	"time"

	arma_types "arma/common/types"
	"arma/core"
	"arma/core/badb"
	"arma/node/comm"
	"arma/node/config"
	"arma/node/consensus/state"
	"arma/node/crypto"
	"arma/node/delivery"
	"arma/node/ledger"

	"github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
)

func CreateConsensus(conf config.ConsenterNodeConfig, logger arma_types.Logger) *Consensus {
	var currentNodes []uint64
	for _, node := range conf.Consenters {
		currentNodes = append(currentNodes, uint64(node.PartyID))
	}

	initialState := initialStateFromConfig(conf)

	consLedger, err := ledger.NewConsensusLedger(conf.Directory)
	if err != nil {
		logger.Panicf("Failed creating consensus ledger: %s", err)
	}

	bftConfig := createBFTconfig(conf)

	c := &Consensus{
		DeliverService: delivery.DeliverService(map[string]blockledger.Reader{"consensus": consLedger}),
		Config:         conf,
		BFTConfig:      bftConfig,
		Arma:           createConsenter(conf, logger, &initialState),
		Logger:         logger,
		State:          &initialState,
		CurrentNodes:   currentNodes,
		Storage:        consLedger,
		SigVerifier:    buildVerifier(conf.Consenters, conf.Shards, logger),
		Signer:         buildSigner(conf, logger),
	}

	genesisBlocks := make([]state.AvailableBlock, 1)
	genesisBlocks[0] = state.AvailableBlock{
		Header: &state.BlockHeader{
			Number:   0,
			PrevHash: nil,
			Digest:   nil, // TODO create a correct digest
		},
		Batch: state.NewAvailableBatch(0, math.MaxUint16, 0, nil), // TODO create a correct digest
	}

	genesisProposal := types.Proposal{
		Payload: []byte("placeholder for config tx"), // TODO create a correct payload
		Header: (&state.Header{
			AvailableBlocks: genesisBlocks,
			State:           &initialState,
			Num:             0,
		}).Serialize(),
		Metadata: nil, // TODO maybe use this metadata
	}

	c.Storage.Append(state.DecisionToBytes(genesisProposal, nil))

	c.BFT = createBFT(c)

	c.BFT.Synchronizer = createSynchronizer(consLedger, c)

	setupComm(c, getOurIdentity(conf.Consenters, arma_types.PartyID(conf.PartyId)))

	return c
}

func buildSigner(conf config.ConsenterNodeConfig, logger arma_types.Logger) Signer {
	privateKey, _ := pem.Decode(conf.SigningPrivateKey)
	if privateKey == nil || privateKey.Bytes == nil {
		logger.Panicf("Failed decoding private key PEM")
	}

	priv, err := x509.ParsePKCS8PrivateKey(privateKey.Bytes)
	if err != nil {
		logger.Panicf("Failed parsing private key DER: %v", err)
	}

	return crypto.ECDSASigner(*priv.(*ecdsa.PrivateKey))
}

func createBFT(c *Consensus) *consensus.Consensus {
	wal, err := wal.Create(c.Logger, filepath.Join(c.Config.Directory, "wal"), &wal.Options{
		FileSizeBytes:   wal.FileSizeBytesDefault,
		BufferSizeBytes: wal.BufferSizeBytesDefault,
	})
	if err != nil {
		c.Logger.Panicf("Failed creating WAL: %v", err)
	}

	return &consensus.Consensus{
		Config:            c.BFTConfig,
		Application:       c,
		Assembler:         c,
		WAL:               wal,
		Signer:            c,
		Verifier:          c,
		RequestInspector:  c,
		Logger:            c.Logger,
		Metadata:          &smartbftprotos.ViewMetadata{},
		Scheduler:         time.NewTicker(time.Second).C,
		ViewChangerTicker: time.NewTicker(time.Second).C,
	}
}

func createSynchronizer(ledger *ledger.ConsensusLedger, c *Consensus) *synchronizer {
	synchronizer := &synchronizer{
		deliver: func(proposal types.Proposal, signatures []types.Signature) {
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
		endpoint: func() string {
			leader := c.BFT.GetLeaderID()
			for i, node := range c.BFT.Comm.Nodes() {
				if node == leader {
					return c.Config.Consenters[i].Endpoint
				}
			}
			return ""
		},
		nextSeq: func() uint64 {
			return ledger.Height()
		},
		BFTConfig:    c.BFTConfig,
		CurrentNodes: c.CurrentNodes,
	}

	ledger.RegisterAppendListener(synchronizer)

	defer func() {
		go synchronizer.run()
	}()

	return synchronizer
}

func createConsenter(conf config.ConsenterNodeConfig, logger arma_types.Logger, initState *core.State) *core.Consenter {
	dbDir := filepath.Join(conf.Directory, "batchDB")
	os.MkdirAll(dbDir, 0o755)

	db, err := badb.NewBatchAttestationDB(dbDir, logger)
	if err != nil {
		logger.Panicf("Failed creating Batch attestation DB: %v", err)
	}

	return &core.Consenter{
		State:           initState,
		DB:              db,
		Logger:          logger,
		BAFDeserializer: &state.BAFDeserializer{},
	}
}

func createBFTconfig(conf config.ConsenterNodeConfig) types.Configuration {
	config := types.DefaultConfig
	config.RequestBatchMaxInterval = time.Millisecond * 500
	if conf.BatchTimeout != 0 {
		config.RequestBatchMaxInterval = conf.BatchTimeout
	}
	config.RequestForwardTimeout = time.Second * 10
	config.SelfID = uint64(conf.PartyId)
	config.DecisionsPerLeader = 0
	config.LeaderRotation = false
	return config
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

		verifier[crypto.ShardPartyKey{Shard: crypto.CONSENSUS_CLUSTER_SHARD, Party: arma_types.PartyID(ci.PartyID)}] = *pk4.(*ecdsa.PublicKey)
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

func getOurIdentity(consenterInfos []config.ConsenterInfo, partyID arma_types.PartyID) []byte {
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

func initialStateFromConfig(config config.ConsenterNodeConfig) core.State {
	var initState core.State
	initState.ShardCount = uint16(len(config.Shards))
	initState.N = uint16(len(config.Consenters))
	F := (uint16(initState.N) - 1) / 3
	initState.Threshold = F + 1
	initState.Quorum = uint16(math.Ceil((float64(initState.N) + float64(F) + 1) / 2.0))

	for _, shard := range config.Shards {
		initState.Shards = append(initState.Shards, core.ShardTerm{
			Shard: arma_types.ShardID(shard.ShardId),
			Term:  0,
		})
	}

	// TODO set right initial app context
	initialAppContext := &state.BlockHeader{
		Number:   0, // We want the first block to start with 0, this is how we signal bootstrap
		PrevHash: nil,
		Digest:   nil,
	}
	initState.AppContext = initialAppContext.Bytes()

	return initState
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

func setupComm(c *Consensus, selfID []byte) {
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
