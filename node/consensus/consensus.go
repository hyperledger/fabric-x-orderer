package consensus

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	arma_types "arma/common/types"
	arma "arma/core"
	"arma/node/comm"
	"arma/node/config"
	"arma/node/consensus/state"
	"arma/node/crypto"
	"arma/node/delivery"
	"arma/node/ledger"
	protos "arma/node/protos/comm"

	"github.com/hyperledger-labs/SmartBFT/pkg/api"
	"github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type Storage interface {
	Append([]byte)
}

type Signer interface {
	Sign(message []byte) ([]byte, error)
}

type SigVerifier interface {
	VerifySignature(id arma.PartyID, shardID arma.ShardID, msg, sig []byte) error
}

type Arma interface {
	SimulateStateTransition(prevState []byte, events [][]byte) ([]byte, [][]arma.BatchAttestationFragment)
	Commit(events [][]byte)
}

type BFT interface {
	SubmitRequest(req []byte) error
	Start() error
	HandleMessage(targetID uint64, m *smartbftprotos.Message)
	HandleRequest(targetID uint64, request []byte)
}

type Consensus struct {
	delivery.DeliverService
	*comm.ClusterService
	Config        config.ConsenterNodeConfig
	PrevHash      []byte
	SigVerifier   SigVerifier
	Signer        Signer
	CurrentNodes  []uint64
	CurrentConfig types.Configuration
	BFT           *consensus.Consensus
	Storage       Storage
	Arma          Arma
	stateLock     sync.Mutex
	State         []byte
	Logger        arma.Logger
	WAL           api.WriteAheadLog
	sync          *synchronizer
}

func (c *Consensus) Start() error {
	return c.BFT.Start()
}

func (c *Consensus) OnConsensus(channel string, sender uint64, request *orderer.ConsensusRequest) error {
	msg := &smartbftprotos.Message{}
	if err := proto.Unmarshal(request.Payload, msg); err != nil {
		c.Logger.Warnf("Malformed message: %v", err)
		return errors.Wrap(err, "malformed message")
	}
	c.BFT.HandleMessage(sender, msg)
	return nil
}

func (c *Consensus) OnSubmit(channel string, sender uint64, req *orderer.SubmitRequest) error {
	rawCE := req.Payload.Payload
	var ce arma.ControlEvent
	bafd := &state.BAFDeserializer{}
	if err := ce.FromBytes(rawCE, bafd.Deserialize); err != nil {
		c.Logger.Errorf("Failed unmarshaling control event %s: %v", base64.StdEncoding.EncodeToString(rawCE), err)
		return nil
	}
	return nil
}

func (c *Consensus) NotifyEvent(stream protos.Consensus_NotifyEventServer) error {
	for {
		event, err := stream.Recv()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		var ce arma.ControlEvent
		bafd := &state.BAFDeserializer{}
		if err := ce.FromBytes(event.GetPayload(), bafd.Deserialize); err != nil {
			return fmt.Errorf("malformed control event: %v", err)
		}

		c.Logger.Infof("Received event %x", event.Payload)

		if err := c.SubmitRequest(event.GetPayload()); err != nil {
			c.Logger.Warnf("Failed submitting request: %v", err)
		}
	}
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

func SetupComm(c *Consensus, selfID []byte) {
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

func (c *Consensus) SubmitRequest(req []byte) error {
	if _, err := c.VerifyRequest(req); err != nil {
		c.Logger.Warnf("Received bad request: %v", err)
		return err
	}
	return c.BFT.SubmitRequest(req)
}

func (c *Consensus) VerifyProposal(proposal types.Proposal) ([]types.RequestInfo, error) {
	batch := arma.BatchFromRaw(proposal.Payload)
	var hdr state.Header
	if err := hdr.FromBytes(proposal.Header); err != nil {
		return nil, err
	}

	c.stateLock.Lock()
	computedState, _ := c.Arma.SimulateStateTransition(c.State, batch)
	c.stateLock.Unlock()
	if !bytes.Equal(hdr.State, computedState) {
		return nil, fmt.Errorf("proposed state %x isn't equal to computed state %x", hdr.State, computedState)
	}

	reqInfos := make([]types.RequestInfo, 0, len(batch))
	for _, rawReq := range batch {
		reqID, err := c.VerifyRequest(rawReq)
		if err != nil {
			return nil, fmt.Errorf("invalid request %s: %v", rawReq, err)
		}

		reqInfos = append(reqInfos, reqID)
	}

	return reqInfos, nil
}

func (c *Consensus) VerifyRequest(req []byte) (types.RequestInfo, error) {
	var ce arma.ControlEvent
	bafd := &state.BAFDeserializer{}
	if err := ce.FromBytes(req, bafd.Deserialize); err != nil {
		return types.RequestInfo{}, err
	}

	reqID := c.RequestID(req)

	if ce.Complaint != nil {
		ce.Complaint.Bytes()
		err := c.SigVerifier.VerifySignature(ce.Complaint.Signer, ce.Complaint.Shard, ToBeSignedComplaint(ce.Complaint), ce.Complaint.Signature)
		return reqID, err
	} else if ce.BAF != nil {
		msg := state.ToBeSignedBAF(ce.BAF)
		err := c.SigVerifier.VerifySignature(ce.BAF.Signer(), ce.BAF.Shard(), msg, ce.BAF.(*arma_types.SimpleBatchAttestationFragment).Sig)
		return reqID, err
	} else {
		return types.RequestInfo{}, fmt.Errorf("empty Control Event")
	}
}

func ToBeSignedComplaint(c *arma.Complaint) []byte {
	buff := make([]byte, 12)
	var pos int
	binary.BigEndian.PutUint16(buff, uint16(c.Shard))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], c.Term)
	pos += 8
	binary.BigEndian.PutUint16(buff[pos:], uint16(c.Signer))

	return buff
}

func (c *Consensus) VerifyConsenterSig(signature types.Signature, prop types.Proposal) ([]byte, error) {
	var msgs Bytes
	if _, err := asn1.Unmarshal(signature.Msg, &msgs); err != nil {
		return nil, err
	}

	var values Bytes
	if _, err := asn1.Unmarshal(signature.Value, &values); err != nil {
		return nil, err
	}

	if err := c.VerifySignature(types.Signature{
		Value: values[len(values)-1],
		Msg:   []byte(prop.Digest()),
		ID:    signature.ID,
	}); err != nil {
		return nil, err
	}

	for i, msg := range msgs {
		if err := c.VerifySignature(types.Signature{
			Value: values[i],
			Msg:   msg,
			ID:    signature.ID,
		}); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (c *Consensus) VerifySignature(signature types.Signature) error {
	return c.SigVerifier.VerifySignature(arma.PartyID(signature.ID), arma.ShardID(math.MaxUint16), signature.Msg, signature.Value)
}

func (c *Consensus) VerificationSequence() uint64 {
	return 0
}

func (c *Consensus) RequestsFromProposal(proposal types.Proposal) []types.RequestInfo {
	batch := arma.BatchFromRaw(proposal.Payload)
	reqInfos := make([]types.RequestInfo, 0, len(batch))
	for _, rawReq := range batch {
		reqID, err := c.VerifyRequest(rawReq)
		if err != nil {
			panic(fmt.Errorf("invalid request %s: %v", rawReq, err))
		}

		reqInfos = append(reqInfos, reqID)
	}

	return reqInfos
}

func (c *Consensus) AuxiliaryData(i []byte) []byte {
	return nil
}

type Bytes [][]byte

func (c *Consensus) Sign(msg []byte) []byte {
	sig, err := c.Signer.Sign(msg)
	if err != nil {
		panic(err)
	}

	return sig
}

func (c *Consensus) RequestID(req []byte) types.RequestInfo {
	var ce arma.ControlEvent
	bafd := &state.BAFDeserializer{}
	if err := ce.FromBytes(req, bafd.Deserialize); err != nil {
		return types.RequestInfo{}
	}

	var clientID string
	var payloadToHash []byte
	if ce.Complaint != nil {
		ce.Complaint.Signature = nil
		payloadToHash = ce.Complaint.Bytes()
		clientID = fmt.Sprintf("%d", ce.Complaint.Signer)
	} else if ce.BAF != nil {
		clientID = fmt.Sprintf("%d", ce.BAF.Signer())
		payloadToHash = make([]byte, 26)
		binary.BigEndian.PutUint64(payloadToHash, ce.BAF.Seq())
		binary.BigEndian.PutUint64(payloadToHash[8:], ce.BAF.Epoch())
		binary.BigEndian.PutUint16(payloadToHash[16:], uint16(ce.BAF.Signer()))
		binary.BigEndian.PutUint16(payloadToHash[18:], uint16(ce.BAF.Primary()))
		binary.BigEndian.PutUint16(payloadToHash[20:], uint16(ce.BAF.Shard()))
		copy(payloadToHash[22:], ce.BAF.Digest())
	} else {
		c.Logger.Warnf("Empty ControlEvent")
		return types.RequestInfo{}
	}

	dig := sha256.Sum256(payloadToHash)
	return types.RequestInfo{
		ID:       hex.EncodeToString(dig[:]),
		ClientID: clientID,
	}
}

func (c *Consensus) SignProposal(proposal types.Proposal, _ []byte) *types.Signature {
	requests := arma.BatchFromRaw(proposal.Payload)

	c.stateLock.Lock()
	_, bafs := c.Arma.SimulateStateTransition(c.State, requests)
	c.stateLock.Unlock()

	sigs := make(Bytes, 0, len(bafs)+1)
	msgs := make(Bytes, 0, len(bafs)+1)

	for _, ba := range bafs {
		var hdr state.BAHeader
		hdr.Digest = ba[0].Digest()
		hdr.Sequence = ba[0].Seq()
		hdr.PrevHash = c.PrevHash
		c.PrevHash = hdr.Hash()
		msg := hdr.Serialize()
		sig, err := c.Signer.Sign(msg)
		if err != nil {
			panic(err)
		}

		sigs = append(sigs, sig)
		msgs = append(msgs, msg)
	}

	proposalSig, err := c.Signer.Sign([]byte(proposal.Digest()))
	if err != nil {
		panic(err)
	}

	sigs = append(sigs, proposalSig)

	msgsRaw, err := asn1.Marshal(msgs)
	if err != nil {
		panic(err)
	}

	sigsRaw, err := asn1.Marshal(sigs)
	if err != nil {
		panic(err)
	}

	return &types.Signature{
		Msg:   msgsRaw,
		Value: sigsRaw,
		ID:    c.CurrentConfig.SelfID,
	}
}

func (c *Consensus) AssembleProposal(metadata []byte, requests [][]byte) types.Proposal {
	c.stateLock.Lock()
	newRawState, attestations := c.Arma.SimulateStateTransition(c.State, requests)
	c.stateLock.Unlock()

	c.Logger.Infof("Created proposal with %d attestations", len(attestations))

	availableBatches := make([]state.AvailableBatch, 0, len(attestations))
	for _, ba := range attestations {
		availableBatches = append(availableBatches, state.NewAvailableBatch(uint16(ba[0].Primary()), uint16(ba[0].Shard()), ba[0].Seq(), ba[0].Digest()))
	}

	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(metadata, md); err != nil {
		panic(err)
	}

	return types.Proposal{
		Header: (&state.Header{
			AvailableBatches: availableBatches,
			State:            newRawState,
			Num:              md.LatestSequence,
		}).Bytes(),
		Metadata: metadata,
		Payload:  arma.BatchedRequests(requests).ToBytes(),
	}
}

func (c *Consensus) Deliver(proposal types.Proposal, signatures []types.Signature) types.Reconfig {
	rawDecision := decisionToBytes(proposal, signatures)

	hdr := &state.Header{}
	if err := hdr.FromBytes(proposal.Header); err != nil {
		c.Logger.Panicf("Failed deserializing header: %v", err)
		return types.Reconfig{}
	}

	controlEvents := arma.BatchFromRaw(proposal.Payload)
	// Why do we first give Arma the events and then append the decision to storage?
	// Upon commit, Arma indexes the batch attestations which passed the threshold in its index,
	// to avoid signing them again in the (near) future.
	// If we crash after this, we will replicate the block and will overwrite the index again.
	// However, if we first commit the decision and then index afterwards and crash during or right before
	// we index, next time we spawn, we will not recognize we did not index and as a result we will may sign
	// a batch attestation twice.
	// This is true because a Commit(controlEvents) with the same controlEvents is idempotent.
	c.Arma.Commit(controlEvents)
	c.Storage.Append(rawDecision)

	c.stateLock.Lock()
	c.State = hdr.State
	c.stateLock.Unlock()

	return types.Reconfig{
		CurrentNodes:  c.CurrentNodes,
		CurrentConfig: c.CurrentConfig,
	}
}

type Signature struct {
	ID    int64
	Value []byte
	Msg   []byte
}

func decisionToBytes(proposal types.Proposal, signatures []types.Signature) []byte {
	sigBuff := bytes.Buffer{}

	for _, sig := range signatures {
		rawSig, err := asn1.Marshal(Signature{Msg: sig.Msg, Value: sig.Value, ID: int64(sig.ID)})
		if err != nil {
			panic(err)
		}
		rawSigSize := make([]byte, 2)
		binary.BigEndian.PutUint16(rawSigSize, uint16(len(rawSig)))
		sigBuff.Write(rawSigSize)
		sigBuff.Write(rawSig)
	}

	buff := make([]byte, 4*3+len(proposal.Header)+len(proposal.Payload)+len(proposal.Metadata)+sigBuff.Len())
	binary.BigEndian.PutUint32(buff, uint32(len(proposal.Header)))
	binary.BigEndian.PutUint32(buff[4:], uint32(len(proposal.Payload)))
	binary.BigEndian.PutUint32(buff[8:], uint32(len(proposal.Metadata)))
	copy(buff[12:], proposal.Header)
	copy(buff[12+len(proposal.Header):], proposal.Payload)
	copy(buff[12+len(proposal.Header)+len(proposal.Payload):], proposal.Metadata)
	copy(buff[12+len(proposal.Header)+len(proposal.Payload)+len(proposal.Metadata):], sigBuff.Bytes())

	return buff
}

func bytesToDecision(rawBytes []byte) (types.Proposal, []types.Signature, error) {
	buff := bytes.NewBuffer(rawBytes)
	headerSize := make([]byte, 4)
	if _, err := buff.Read(headerSize); err != nil {
		return types.Proposal{}, nil, err
	}

	payloadSize := make([]byte, 4)
	if _, err := buff.Read(payloadSize); err != nil {
		return types.Proposal{}, nil, err
	}

	metadataSize := make([]byte, 4)
	if _, err := buff.Read(metadataSize); err != nil {
		return types.Proposal{}, nil, err
	}

	header := make([]byte, binary.BigEndian.Uint32(headerSize))
	if _, err := buff.Read(header); err != nil {
		return types.Proposal{}, nil, err
	}

	payload := make([]byte, binary.BigEndian.Uint32(payloadSize))
	if _, err := buff.Read(payload); err != nil {
		return types.Proposal{}, nil, err
	}

	metadata := make([]byte, binary.BigEndian.Uint32(metadataSize))
	if _, err := buff.Read(metadata); err != nil {
		return types.Proposal{}, nil, err
	}

	proposalSize := 4*3 + len(header) + len(payload) + len(metadata)

	signatureBuff := make([]byte, len(rawBytes)-proposalSize)

	if _, err := buff.Read(signatureBuff); err != nil {
		return types.Proposal{}, nil, err
	}

	var sigs []types.Signature

	var pos int
	for pos < len(signatureBuff) {
		sigSize := int(binary.BigEndian.Uint16([]byte{signatureBuff[pos], signatureBuff[pos+1]}))
		pos += 2
		sig := Signature{}
		if _, err := asn1.Unmarshal(signatureBuff[pos:pos+sigSize], &sig); err != nil {
			return types.Proposal{}, nil, err
		}
		pos += sigSize
		sigs = append(sigs, types.Signature{
			Msg:   sig.Msg,
			Value: sig.Value,
			ID:    uint64(sig.ID),
		})
	}

	return types.Proposal{
		Header:   header,
		Payload:  payload,
		Metadata: metadata,
	}, sigs, nil
}

func initialStateFromConfig(config config.ConsenterNodeConfig) []byte {
	var state arma.State
	state.ShardCount = uint16(len(config.Shards))
	state.N = uint16(len(config.Consenters))
	F := (uint16(state.N) - 1) / 3
	state.Threshold = F + 1
	state.Quorum = uint16(math.Ceil((float64(state.N) + float64(F) + 1) / 2.0))

	for _, shard := range config.Shards {
		state.Shards = append(state.Shards, arma.ShardTerm{
			Shard: arma.ShardID(shard.ShardId),
			Term:  0,
		})
	}

	return state.Serialize()
}

func CreateConsensus(conf config.ConsenterNodeConfig, logger arma.Logger) *Consensus {
	privateKey, _ := pem.Decode(conf.SigningPrivateKey)
	if privateKey == nil || privateKey.Bytes == nil {
		logger.Panicf("Failed decoding private key PEM")
	}

	priv, err := x509.ParsePKCS8PrivateKey(privateKey.Bytes)
	if err != nil {
		logger.Panicf("Failed parsing private key DER: %v", err)
	}

	var currentNodes []uint64
	for _, node := range conf.Consenters {
		currentNodes = append(currentNodes, uint64(node.PartyID))
	}

	initialState := initialStateFromConfig(conf)

	config := types.DefaultConfig
	config.RequestBatchMaxInterval = time.Millisecond * 500
	if conf.BatchTimeout != 0 {
		config.RequestBatchMaxInterval = conf.BatchTimeout
	}
	config.RequestForwardTimeout = time.Second * 10
	config.SelfID = uint64(conf.PartyId)
	config.DecisionsPerLeader = 0
	config.LeaderRotation = false

	dbDir := filepath.Join(conf.Directory, "batchDB")
	os.MkdirAll(dbDir, 0o755)

	db, err := NewBatchAttestationDB(dbDir, logger)
	if err != nil {
		logger.Panicf("Failed creating Batch attestation DB: %v", err)
	}

	consenterVerifier := buildVerifier(conf.Consenters, conf.Shards, logger)

	wal, err := wal.Create(logger, filepath.Join(conf.Directory, "wal"), &wal.Options{
		FileSizeBytes:   wal.FileSizeBytesDefault,
		BufferSizeBytes: wal.BufferSizeBytesDefault,
	})
	if err != nil {
		logger.Panicf("Failed creating WAL: %v", err)
	}

	consLedger, err := ledger.NewConsensusLedger(conf.Directory)
	if err != nil {
		logger.Panicf("Failed creating consensus ledger: %s", err)
	}

	c := &Consensus{
		DeliverService: delivery.DeliverService(map[string]blockledger.Reader{"consensus": consLedger}),
		Config:         conf,
		WAL:            wal,
		CurrentConfig:  types.Configuration{SelfID: uint64(conf.PartyId)},
		Arma: &arma.Consenter{
			State:           initialState,
			DB:              db,
			Logger:          logger,
			BAFDeserializer: &state.BAFDeserializer{},
		},
		Logger:       logger,
		State:        initialState,
		CurrentNodes: currentNodes,
		Storage:      consLedger,
		SigVerifier:  consenterVerifier,
		Signer:       crypto.ECDSASigner(*priv.(*ecdsa.PrivateKey)),
	}

	bft := &consensus.Consensus{
		Metadata:          &smartbftprotos.ViewMetadata{},
		Logger:            logger,
		Config:            config,
		WAL:               wal,
		RequestInspector:  c,
		Signer:            c,
		Assembler:         c,
		Scheduler:         time.NewTicker(time.Second).C,
		ViewChangerTicker: time.NewTicker(time.Second).C,
		Application:       c,
		Verifier:          c,
	}

	c.sync = &synchronizer{
		deliver: func(proposal types.Proposal, signatures []types.Signature) {
			c.Deliver(proposal, signatures)
		},
		getHeight: func() uint64 {
			return consLedger.Height()
		},
		getBlock: func(seq uint64) *common.Block {
			block, err := consLedger.RetrieveBlockByNumber(seq)
			if err != nil {
				panic(err)
			}
			return block
		},
		pruneRequestsFromMemPool: func(req []byte) {
			bft.Pool.RemoveRequest(c.RequestID(req))
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
			return consLedger.Height()
		},
		CurrentConfig: c.CurrentConfig,
		CurrentNodes:  c.CurrentNodes,
	}

	consLedger.RegisterAppendListener(c.sync)

	defer func() {
		go c.sync.run()
	}()

	bft.Synchronizer = c.sync

	c.BFT = bft

	myIdentity := getOurIdentity(conf.Consenters, arma.PartyID(conf.PartyId))

	SetupComm(c, myIdentity)

	return c
}

func buildVerifier(consenterInfos []config.ConsenterInfo, shardInfo []config.ShardInfo, logger arma.Logger) crypto.ECDSAVerifier {
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

		verifier[crypto.ShardPartyKey{Shard: crypto.CONSENSUS_CLUSTER_SHARD, Party: arma.PartyID(ci.PartyID)}] = *pk4.(*ecdsa.PublicKey)
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

			verifier[crypto.ShardPartyKey{Shard: arma.ShardID(shard.ShardId), Party: arma.PartyID(bi.PartyID)}] = *pk4.(*ecdsa.PublicKey)
		}
	}

	return verifier
}

func getOurIdentity(consenterInfos []config.ConsenterInfo, partyID arma.PartyID) []byte {
	var myIdentity []byte
	for _, ci := range consenterInfos {
		pk := ci.PublicKey

		if ci.PartyID == uint16(partyID) {
			myIdentity = pk
			break
		}
	}
	return myIdentity
}
