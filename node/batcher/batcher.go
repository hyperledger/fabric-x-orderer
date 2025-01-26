package batcher

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"arma/common/types"
	"arma/core"
	"arma/node/comm"
	node_config "arma/node/config"
	"arma/node/crypto"
	node_ledger "arma/node/ledger"
	protos "arma/node/protos/comm"
	"arma/request"

	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

var defaultBatcherMemPoolOpts = request.PoolOptions{
	AutoRemoveTimeout:     time.Second * 10,
	BatchMaxSize:          1000 * 10,
	BatchMaxSizeBytes:     1024 * 1024 * 10,
	MaxSize:               1000 * 1000,
	FirstStrikeThreshold:  time.Second * 10,
	SecondStrikeThreshold: time.Second * 10,
	SubmitTimeout:         time.Second * 10,
	RequestMaxBytes:       1024 * 1024,
}

//go:generate counterfeiter -o mocks/state_replicator.go . StateReplicator
type StateReplicator interface {
	ReplicateState() <-chan *core.State
}

type Batcher struct {
	batcherDeliverService *BatcherDeliverService
	stateReplicator       StateReplicator
	logger                types.Logger
	batcher               *core.Batcher
	batcherCerts2IDs      map[string]types.PartyID
	consensusStreams      []protos.Consensus_NotifyEventClient
	connections           []*grpc.ClientConn
	config                node_config.BatcherNodeConfig
	batchers              []node_config.BatcherInfo
	privateKey            *ecdsa.PrivateKey
	tlsKey                []byte
	tlsCert               []byte
	stateRef              atomic.Value
	stateChan             chan *core.State
	running               sync.WaitGroup
	stopOnce              sync.Once
	stopChan              chan struct{}

	primaryLock         sync.Mutex
	primaryID           types.PartyID
	primaryEndpoint     string
	primaryTLSCA        []node_config.RawBytes
	primaryClient       protos.AckServiceClient
	primaryClientStream protos.AckService_NotifyAckClient
	primaryConn         *grpc.ClientConn
}

func (b *Batcher) Run() {
	b.stopChan = make(chan struct{})

	b.stateChan = make(chan *core.State, 1)

	go b.replicateState()

	b.logger.Infof("Starting batcher")
	b.batcher.Start()
}

func (b *Batcher) Stop() {
	b.stopOnce.Do(func() { close(b.stopChan) })
	b.batcher.Stop()
	for len(b.stateChan) > 0 {
		<-b.stateChan // drain state channel
	}
	b.running.Wait()
}

// replicateState runs by a separate go routine
func (b *Batcher) replicateState() {
	b.logger.Infof("Started replicating state")
	b.running.Add(1)
	defer func() {
		b.running.Done()
		b.logger.Infof("Stopped replicating state")
	}()
	stateChan := b.stateReplicator.ReplicateState()
	for {
		select {
		case state := <-stateChan:
			b.stateRef.Store(state)
			b.stateChan <- state
			primaryID := b.getPrimaryID(state)
			b.primaryLock.Lock()
			if b.primaryID != primaryID {
				b.logger.Infof("Primary changed from %d to %d", b.primaryID, primaryID)
				b.primaryID = primaryID
				b.setupPrimaryEndpoint()
				b.primaryConn = nil
				b.primaryClient = nil
				b.primaryClientStream = nil
				b.connectToPrimaryIfNeeded()
			}
			b.primaryLock.Unlock()
		case <-b.stopChan:
			return
		}
	}
}

func (b *Batcher) GetLatestStateChan() <-chan *core.State {
	return b.stateChan
}

func (b *Batcher) Broadcast(_ orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (b *Batcher) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	return b.batcherDeliverService.Deliver(stream)
}

func (b *Batcher) Submit(ctx context.Context, req *protos.Request) (*protos.SubmitResponse, error) {
	traceId := req.TraceId
	req.TraceId = nil

	rawReq, err := proto.Marshal(req)
	if err != nil {
		b.logger.Panicf("Failed marshaling request: %v", err)
	}

	b.logger.Infof("Received request %x", req.Payload)

	var resp protos.SubmitResponse
	resp.TraceId = traceId
	if err := b.batcher.Submit(rawReq); err != nil {
		resp.Error = err.Error()
	}

	return &resp, nil
}

func (b *Batcher) SubmitStream(stream protos.RequestTransmit_SubmitStreamServer) error {
	stop := make(chan struct{})
	defer close(stop)

	defer func() {
		b.logger.Infof("Client disconnected")
	}()

	responses := make(chan *protos.SubmitResponse, 1000)

	go b.sendResponses(stream, responses, stop)

	return b.dispatchRequests(stream, responses)
}

func (b *Batcher) dispatchRequests(stream protos.RequestTransmit_SubmitStreamServer, responses chan *protos.SubmitResponse) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		traceId := req.TraceId
		req.TraceId = nil

		rawReq, err := proto.Marshal(req)
		if err != nil {
			b.logger.Panicf("Failed marshaling request: %v", err)
		}

		var resp protos.SubmitResponse
		resp.TraceId = traceId

		if err := b.batcher.Submit(rawReq); err != nil {
			resp.Error = err.Error()
		}

		if len(traceId) > 0 {
			responses <- &resp
		}

		b.logger.Debugf("Submitted request %x", traceId)

	}
}

func (b *Batcher) sendResponses(stream protos.RequestTransmit_SubmitStreamServer, responses chan *protos.SubmitResponse, stop chan struct{}) {
	for {
		select {
		case resp := <-responses:
			b.logger.Debugf("Sending response %x", resp.TraceId)
			stream.Send(resp)
			b.logger.Debugf("Sent response %x", resp.TraceId)
		case <-stop:
			b.logger.Debugf("Stopped sending responses")
			return
		}
	}
}

func (b *Batcher) NotifyAck(stream protos.AckService_NotifyAckServer) error {
	cert := ExtractCertificateFromContext(stream.Context())

	from, exists := b.batcherCerts2IDs[string(cert)]
	if !exists {
		return fmt.Errorf("access denied")
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		b.batcher.HandleAck(types.BatchSequence(msg.Seq), from)
	}
}

func NewBatcher(logger types.Logger, config node_config.BatcherNodeConfig, ledger core.BatchLedger, bp core.BatchPuller, ds *BatcherDeliverService, sr StateReplicator) *Batcher {
	b := &Batcher{
		batcherDeliverService: ds,
		stateReplicator:       sr,
		privateKey:            createSigner(logger, config),
		logger:                logger,
		batcherCerts2IDs:      make(map[string]types.PartyID),
		config:                config,
		tlsKey:                config.TLSPrivateKeyFile,
		tlsCert:               config.TLSCertificateFile,
	}

	b.batchers = batchersFromConfig(config)
	if len(b.batchers) == 0 {
		logger.Panicf("Failed locating the configuration of our shard (%d) among %v", config.ShardId, config.Shards)
	}

	initState := computeZeroState(config)
	b.stateRef.Store(&initState)

	b.primaryID = b.getPrimaryID(&initState)

	b.indexTLSCerts()

	f := (initState.N - 1) / 3

	b.batcher = &core.Batcher{
		Batchers:      getBatchersIDs(b.batchers),
		BatchPuller:   bp,
		Threshold:     int(f + 1),
		N:             initState.N,
		BatchTimeout:  time.Millisecond * 500,
		Ledger:        ledger,
		MemPool:       b.createMemPool(config),
		AttestBatch:   b.attestBatch,
		StateProvider: b,
		ID:            config.PartyId,
		Shard:         config.ShardId,
		Logger:        logger,
		Digest: func(data [][]byte) []byte {
			batch := types.BatchedRequests(data)
			digest := sha256.Sum256(batch.Serialize())
			return digest[:]
		},
		RequestInspector: b,
		TotalOrderBAF:    b.broadcastEvent,
		AckBAF:           b.sendAck,
	}

	b.setupPrimaryEndpoint()

	return b
}

func getBatchersIDs(batchers []node_config.BatcherInfo) []types.PartyID {
	var parties []types.PartyID
	for _, batcher := range batchers {
		parties = append(parties, batcher.PartyID)
	}

	return parties
}

func (b *Batcher) createMemPool(config node_config.BatcherNodeConfig) core.MemPool {
	opts := defaultBatcherMemPoolOpts
	opts.BatchMaxSizeBytes = config.BatchMaxBytes
	opts.MaxSize = config.MemPoolMaxSize
	opts.BatchMaxSize = config.BatchMaxSize
	opts.RequestMaxBytes = config.RequestMaxBytes
	opts.SubmitTimeout = config.BatchTimeout

	opts.OnFirstStrikeTimeout = func(key []byte) {
		// TODO send tx to primary
		b.logger.Warnf("First strike timeout occurred on request %s", b.RequestID(key))
	}
	opts.OnSecondStrikeTimeout = func() {
		// TODO complain
		b.logger.Warnf("Second strike timeout occurred")
	}

	return request.NewPool(b.logger, b, opts)
}

func (b *Batcher) indexTLSCerts() {
	for _, batcher := range b.batchers {
		rawTLSCert := batcher.TLSCert
		bl, _ := pem.Decode(rawTLSCert)
		if bl == nil || bl.Bytes == nil {
			b.logger.Panicf("Failed decoding TLS certificate of %d from PEM", batcher.PartyID)
		}

		b.batcherCerts2IDs[string(bl.Bytes)] = batcher.PartyID
	}
}

func createSigner(logger types.Logger, config node_config.BatcherNodeConfig) *ecdsa.PrivateKey {
	rawKey := config.SigningPrivateKey
	bl, _ := pem.Decode(rawKey)

	if bl == nil || bl.Bytes == nil {
		logger.Panicf("Signing key is not a valid PEM")
	}

	sk, err := x509.ParsePKCS8PrivateKey(bl.Bytes)
	if err != nil {
		logger.Panicf("Signing key is not a valid PKCS8 private key: %v", err)
	}

	return sk.(*ecdsa.PrivateKey)
}

func (b *Batcher) attestBatch(seq types.BatchSequence, primary types.PartyID, shard types.ShardID, digest []byte) core.BatchAttestationFragment {
	baf, err := CreateBAF(b.privateKey, b.config.PartyId, shard, digest, primary, seq)
	if err != nil {
		b.logger.Panicf("Failed creating batch attestation fragment: %v", err)
	}

	return baf
}

func (b *Batcher) setupPrimaryEndpoint() {
	for _, batcher := range b.batchers {
		if batcher.PartyID == b.primaryID {
			b.primaryEndpoint = batcher.Endpoint
			b.primaryTLSCA = batcher.TLSCACerts
			b.logger.Infof("Primary for shard %d: %d %s", b.config.ShardId, b.primaryID, b.primaryEndpoint)
			return
		}
	}

	b.logger.Panicf("Could not find primaryID %d of shard %d within %v", b.primaryID, b.config.ShardId, b.batchers)
}

func (b *Batcher) GetPrimaryID() types.PartyID {
	b.primaryLock.Lock()
	defer b.primaryLock.Unlock()
	return b.primaryID
}

func (b *Batcher) getPrimaryID(state *core.State) types.PartyID {
	term := uint64(math.MaxUint64)
	for _, shard := range state.Shards {
		if shard.Shard == b.config.ShardId {
			term = shard.Term
		}
	}

	if term == math.MaxUint64 {
		b.logger.Panicf("Could not find our shard (%d) within the shards: %v", b.config.ShardId, state.Shards)
	}

	primaryIndex := types.PartyID((uint64(b.config.ShardId) + term) % uint64(state.N))

	primaryID := b.batchers[primaryIndex].PartyID

	return primaryID
}

func computeZeroState(config node_config.BatcherNodeConfig) core.State {
	var state core.State
	for _, shard := range config.Shards {
		state.Shards = append(state.Shards, core.ShardTerm{
			Shard: shard.ShardId,
		})
	}

	state.N = uint16(len(config.Consenters))

	return state
}

func batchersFromConfig(config node_config.BatcherNodeConfig) []node_config.BatcherInfo {
	var batchers []node_config.BatcherInfo
	for _, shard := range config.Shards {
		if shard.ShardId == config.ShardId {
			batchers = shard.Batchers
		}
	}

	sort.Slice(batchers, func(i, j int) bool {
		return int(batchers[i].PartyID) < int(batchers[j].PartyID)
	})

	return batchers
}

func (b *Batcher) sendAck(seq types.BatchSequence, to types.PartyID) {
	b.primaryLock.Lock()
	b.connectToPrimaryIfNeeded()

	if b.primaryClientStream == nil {
		b.primaryLock.Unlock()
		return
	}

	b.primaryLock.Unlock()
	err := b.primaryClientStream.Send(&protos.Ack{Shard: uint32(b.config.ShardId), Seq: uint64(seq)})
	if err != nil {
		b.logger.Errorf("Failed sending ack to %s", b.primaryEndpoint)
		b.primaryClientStream = nil
	}
}

func (b *Batcher) connectToPrimaryIfNeeded() {
	if b.primaryConn == nil {
		cc := b.clientConfig(b.primaryTLSCA)

		conn, err := cc.Dial(b.primaryEndpoint)
		if err != nil {
			b.logger.Errorf("Failed connecting to %s: %v", b.primaryEndpoint, err)
			return
		}

		b.primaryConn = conn
		b.primaryClient = protos.NewAckServiceClient(conn)
	}

	if b.primaryClientStream == nil {
		stream, err := b.primaryClient.NotifyAck(context.Background())
		if err != nil {
			b.logger.Errorf("Failed creating ack stream: %d %d, err: %v", b.config.ShardId, b.config.PartyId, err)
			return
		}
		b.primaryClientStream = stream
		b.logger.Infof("Created ack stream: %d %d ", b.config.ShardId, b.config.PartyId)
	}
}

func (b *Batcher) broadcastEvent(baf core.BatchAttestationFragment) {
	b.initConsensusConnections()

	for index, stream := range b.consensusStreams {
		b.sendBAF(baf, stream, index)
	}
}

func (b *Batcher) initConsensusConnections() {
	b.initStreamsToConsensus()

	for index := range b.consensusStreams {
		b.connectToConsensusNodeIfNeeded(index)
	}
}

func (b *Batcher) sendBAF(baf core.BatchAttestationFragment, stream protos.Consensus_NotifyEventClient, index int) {
	t1 := time.Now()

	defer func() {
		b.logger.Infof("Sending BAF took %v", time.Since(t1))
	}()

	if b.consensusStreams[index] == nil {
		return
	}

	var ce core.ControlEvent
	ce.BAF = baf

	err := stream.Send(&protos.Event{
		Payload: ce.Bytes(),
	})
	if err != nil {
		b.logger.Errorf("Failed sending batch attestation fragment to %d (%s): %v", b.config.Consenters[index].PartyID, b.config.Consenters[index].Endpoint, err)
		b.consensusStreams[index] = nil
		return
	}
}

func (b *Batcher) initStreamsToConsensus() {
	if len(b.connections) == 0 {
		b.connections = make([]*grpc.ClientConn, len(b.config.Consenters))
		b.consensusStreams = make([]protos.Consensus_NotifyEventClient, len(b.config.Consenters))
	}
}

func (b *Batcher) connectToConsensusNodeIfNeeded(index int) {
	if b.connections[index] != nil {
		if b.consensusStreams[index] == nil {
			b.createStream(index)
		}
		return
	}

	consenterConfig := b.config.Consenters[index]

	cc := b.clientConfig(consenterConfig.TLSCACerts)

	conn, err := cc.Dial(consenterConfig.Endpoint)
	if err != nil {
		b.logger.Errorf("Failed connecting to %s: %v", consenterConfig.Endpoint, err)
		return
	}

	b.connections[index] = conn
	b.createStream(index)
}

func (b *Batcher) createStream(index int) {
	cl := protos.NewConsensusClient(b.connections[index])

	var err error
	b.consensusStreams[index], err = cl.NotifyEvent(context.Background())
	if err != nil {
		b.logger.Errorf("Failed creating stream: %v", err)
	}
}

func (b *Batcher) clientConfig(TlsCACert []node_config.RawBytes) comm.ClientConfig {
	var tlsCAs [][]byte
	for _, cert := range TlsCACert {
		tlsCAs = append(tlsCAs, cert)
	}

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			Key:               b.tlsKey,
			Certificate:       b.tlsCert,
			RequireClientCert: true,
			UseTLS:            true,
			ServerRootCAs:     tlsCAs,
		},
		DialTimeout: time.Second * 5,
	}
	return cc
}

func (b *Batcher) RequestID(req []byte) string {
	// TODO maybe calculate the request ID differently
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}

func CreateBAF(sk *ecdsa.PrivateKey, id types.PartyID, shard types.ShardID, digest []byte, primary types.PartyID, seq types.BatchSequence) (core.BatchAttestationFragment, error) {
	baf := types.NewSimpleBatchAttestationFragment(shard, primary, seq, digest, id, nil, 0, nil)
	signer := crypto.ECDSASigner(*sk) // TODO maybe use a different signer
	sig, err := signer.Sign(baf.ToBeSigned())
	if err != nil {
		return nil, err
	}
	baf.SetSignature(sig)

	return baf, nil
}

func ExtractCertificateFromContext(ctx context.Context) []byte {
	pr, extracted := peer.FromContext(ctx)
	if !extracted {
		return nil
	}

	authInfo := pr.AuthInfo
	if authInfo == nil {
		return nil
	}

	tlsInfo, isTLSConn := authInfo.(credentials.TLSInfo)
	if !isTLSConn {
		return nil
	}
	certs := tlsInfo.State.PeerCertificates
	if len(certs) == 0 {
		return nil
	}

	if certs[0] == nil {
		return nil
	}

	return certs[0].Raw
}

func CreateBatcher(conf node_config.BatcherNodeConfig, logger types.Logger) *Batcher {
	conf = maybeSetDefaultConfig(conf)

	var parties []types.PartyID
	for shIdx, sh := range conf.Shards {
		if sh.ShardId != conf.ShardId {
			continue
		}

		for _, b := range conf.Shards[shIdx].Batchers {
			parties = append(parties, b.PartyID)
		}
		break
	}

	ledgerArray, err := node_ledger.NewBatchLedgerArray(conf.ShardId, conf.PartyId, parties, conf.Directory, logger)
	if err != nil {
		logger.Panicf("Failed creating BatchLedgerArray: %s", err)
	}

	deliveryService := &BatcherDeliverService{
		LedgerArray: ledgerArray,
		Logger:      logger,
	}

	bp := NewBatchPuller(conf, ledgerArray, logger)

	cr := CreateConsensusReplicator(conf, logger)

	batcher := NewBatcher(logger, conf, ledgerArray, bp, deliveryService, cr)

	batcher.initConsensusConnections()
	batcher.connectToPrimaryIfNeeded()

	return batcher
}

func maybeSetDefaultConfig(conf node_config.BatcherNodeConfig) node_config.BatcherNodeConfig {
	for {
		switch {
		case conf.BatchMaxSize == 0:
			conf.BatchMaxSize = defaultBatcherMemPoolOpts.BatchMaxSize
		case conf.BatchTimeout == 0:
			conf.BatchTimeout = defaultBatcherMemPoolOpts.SubmitTimeout
		case conf.RequestMaxBytes == 0:
			conf.RequestMaxBytes = defaultBatcherMemPoolOpts.RequestMaxBytes
		case conf.BatchMaxBytes == 0:
			conf.BatchMaxBytes = defaultBatcherMemPoolOpts.BatchMaxSizeBytes
		case conf.MemPoolMaxSize == 0:
			conf.MemPoolMaxSize = defaultBatcherMemPoolOpts.MaxSize
		default:
			return conf
		}
	}
}
