package node

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
	"sync"
	"sync/atomic"
	"time"

	arma "arma/core"
	node_batcher "arma/node/batcher"
	"arma/node/comm"
	node_config "arma/node/config"
	node_ledger "arma/node/ledger"
	protos "arma/node/protos/comm"
	"arma/request"

	"github.com/hyperledger/fabric-protos-go/orderer"
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

type Batcher struct {
	ds                  *node_batcher.BatcherDeliverService
	logger              arma.Logger
	b                   *arma.Batcher
	batcherCerts2IDs    map[string]arma.PartyID
	primaryEndpoint     string
	primaryTLSCA        []node_config.RawBytes
	consensusStreams    []protos.Consensus_NotifyEventClient
	primaryClient       protos.AckServiceClient
	primaryClientStream protos.AckService_NotifyAckClient
	primaryConn         *grpc.ClientConn
	connections         []*grpc.ClientConn
	config              node_config.BatcherNodeConfig
	sk                  *ecdsa.PrivateKey
	tlsKey              []byte
	tlsCert             []byte
	stateRef            atomic.Value
	stateChan           chan *arma.State
	running             sync.WaitGroup
	stopChan            chan struct{}
}

func (b *Batcher) Run() {
	b.running.Add(1)
	b.stopChan = make(chan struct{})

	b.stateChan = make(chan *arma.State)

	bar := b.createBAR()

	go b.replicateState(0, bar) // TODO use correct seq

	b.logger.Infof("Starting batcher")
	b.b.Start()
}

func (b *Batcher) Stop() {
	b.b.Stop()
	close(b.stopChan)
	b.running.Wait()
}

func (b *Batcher) createBAR() *BAReplicator {
	var endpoint string
	var tlsCAs []node_config.RawBytes
	for i := 0; i < len(b.config.Consenters); i++ {
		consenter := b.config.Consenters[i]
		if consenter.PartyID == b.config.PartyId {
			endpoint = consenter.Endpoint
			tlsCAs = consenter.TLSCACerts
		}
	}

	if endpoint == "" || len(tlsCAs) == 0 {
		b.logger.Panicf("Failed finding endpoint and TLS CAs for party %d", b.config.PartyId)
	}

	bar := &BAReplicator{
		logger:   b.logger,
		endpoint: endpoint,
		tlsKey:   b.tlsKey,
		tlsCert:  b.tlsCert,
		cc:       clientConfig(tlsCAs, b.tlsKey, b.tlsCert),
	}

	return bar
}

func (b *Batcher) replicateState(seq uint64, bar *BAReplicator) {
	defer b.running.Done()
	for {
		select {
		case state := <-bar.ReplicateState(seq):
			b.stateRef.Store(state)
			b.stateChan <- state
		case <-b.stopChan:
			return
		}
	}
}

func (b *Batcher) GetLatestStateChan() <-chan *arma.State {
	return b.stateChan
}

func (b *Batcher) Broadcast(_ orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (b *Batcher) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	return b.ds.Deliver(stream)
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
	if err := b.b.Submit(rawReq); err != nil {
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

		if err := b.b.Submit(rawReq); err != nil {
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
		b.b.HandleAck(msg.Seq, from)
	}
}

func NewBatcher(logger arma.Logger, config node_config.BatcherNodeConfig, ledger arma.BatchLedger, bp arma.BatchPuller, ds *node_batcher.BatcherDeliverService) *Batcher {
	cert := config.TLSCertificateFile
	sk := config.TLSPrivateKeyFile

	b := &Batcher{
		ds:               ds,
		sk:               createSigner(logger, config),
		logger:           logger,
		batcherCerts2IDs: make(map[string]arma.PartyID),
		config:           config,
		tlsKey:           sk,
		tlsCert:          cert,
	}

	initState := computeZeroState(config)
	b.stateRef.Store(&initState)

	b.indexTLSCerts()

	parties := batcherIDs(logger, config)

	f := (initState.N - 1) / 3

	b.b = &arma.Batcher{
		Batchers:      parties,
		BatchPuller:   bp,
		Threshold:     int(f + 1),
		N:             initState.N,
		BatchTimeout:  time.Millisecond * 500,
		Ledger:        ledger,
		MemPool:       b.createMemPool(config),
		AttestBatch:   b.attestBatch,
		StateProvider: b,
		ID:            arma.PartyID(config.PartyId),
		Shard:         arma.ShardID(config.ShardId),
		Logger:        logger,
		Digest: func(data [][]byte) []byte {
			batch := arma.BatchedRequests(data)
			digest := sha256.Sum256(batch.ToBytes())
			return digest[:]
		},
		RequestInspector: b,
		TotalOrderBAF:    b.broadcastEvent,
		AckBAF:           b.sendAck,
	}

	b.setupPrimaryEndpoint()

	return b
}

func batcherIDs(logger arma.Logger, config node_config.BatcherNodeConfig) []arma.PartyID {
	batchers := batchersFromConfig(logger, config)
	var parties []arma.PartyID
	for _, batcher := range batchers {
		parties = append(parties, arma.PartyID(batcher.PartyID))
	}
	return parties
}

func (b *Batcher) createMemPool(config node_config.BatcherNodeConfig) arma.MemPool {
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
		b.logger.Warnf("second strike timeout occurred")
	}

	return request.NewPool(b.logger, b, opts)
}

func (b *Batcher) indexTLSCerts() {
	batchers := batchersFromConfig(b.logger, b.config)
	for _, batcher := range batchers {
		rawTLSCert := batcher.TLSCert
		bl, _ := pem.Decode(rawTLSCert)
		if bl == nil || bl.Bytes == nil {
			b.logger.Panicf("Failed decoding TLS certificate of %d from PEM", batcher.PartyID)
		}

		b.batcherCerts2IDs[string(bl.Bytes)] = arma.PartyID(batcher.PartyID)
	}
}

func createSigner(logger arma.Logger, config node_config.BatcherNodeConfig) *ecdsa.PrivateKey {
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

func (b *Batcher) attestBatch(seq uint64, primary arma.PartyID, shard arma.ShardID, digest []byte) arma.BatchAttestationFragment {
	var pending []arma.BatchAttestationFragment
	ref := b.stateRef.Load()
	if ref != nil {
		state := ref.(*arma.State)
		pending = state.Pending
	}
	baf, err := createBAF(b.sk, b.config.PartyId, uint16(shard), digest, uint16(primary), seq, pending...)
	if err != nil {
		b.logger.Panicf("Failed creating batch attestation fragment: %v", err)
	}

	return baf
}

func (b *Batcher) setupPrimaryEndpoint() {
	batchers := batchersFromConfig(b.logger, b.config)
	b.logger.Infof("Batchers: %v", batchers)

	primaryID := b.getPrimaryID()

	for _, batcher := range batchers {
		if arma.PartyID(batcher.PartyID) == primaryID {
			b.primaryEndpoint = batcher.Endpoint
			b.primaryTLSCA = batcher.TLSCACerts
			b.logger.Infof("Primary for shard %d: %d %s", b.config.ShardId, primaryID, b.primaryEndpoint)
			return
		}
	}

	b.logger.Panicf("Could not find primaryID %d of shard %d within %v", primaryID, b.config.ShardId, batchers)
}

func (b *Batcher) getPrimaryID() arma.PartyID {
	state := b.stateRef.Load().(*arma.State)
	term := uint64(math.MaxUint64)
	for _, shard := range state.Shards {
		if shard.Shard == arma.ShardID(b.config.ShardId) {
			term = shard.Term
		}
	}

	if term == math.MaxUint64 {
		b.logger.Panicf("Could not find our shard (%d) within the shards: %v", b.config.ShardId, state.Shards)
	}

	primaryIndex := arma.PartyID((uint64(b.config.ShardId) + term) % uint64(state.N))

	batchers := batchersFromConfig(b.logger, b.config)

	primaryID := arma.PartyID(batchers[primaryIndex].PartyID)

	return primaryID
}

func computeZeroState(config node_config.BatcherNodeConfig) arma.State {
	var state arma.State
	for _, shard := range config.Shards {
		state.Shards = append(state.Shards, arma.ShardTerm{
			Shard: arma.ShardID(shard.ShardId),
		})
	}

	state.N = uint16(len(config.Consenters))

	return state
}

func batchersFromConfig(logger arma.Logger, config node_config.BatcherNodeConfig) []node_config.BatcherInfo {
	var batchers []node_config.BatcherInfo
	for _, shard := range config.Shards {
		if shard.ShardId == config.ShardId {
			batchers = shard.Batchers
		}
	}

	if len(batchers) == 0 {
		logger.Panicf("Failed locating the configuration of our shard (%d) among %v", config.ShardId, config.Shards)
	}
	// TODO make sure this is sorted
	return batchers
}

func (b *Batcher) sendAck(seq uint64, to arma.PartyID) {
	b.connectToPrimaryIfNeeded()

	if b.primaryClientStream == nil {
		return
	}

	err := b.primaryClientStream.Send(&protos.Ack{Shard: uint32(b.config.ShardId), Seq: seq})
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

func (b *Batcher) broadcastEvent(baf arma.BatchAttestationFragment) {
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

func (b *Batcher) sendBAF(baf arma.BatchAttestationFragment, stream protos.Consensus_NotifyEventClient, index int) {
	t1 := time.Now()

	defer func() {
		b.logger.Infof("Sending BAF took %v", time.Since(t1))
	}()

	if b.consensusStreams[index] == nil {
		return
	}

	var ce arma.ControlEvent
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
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}

func createBAF(sk *ecdsa.PrivateKey, id uint16, shard uint16, digest []byte, primary uint16, seq uint64, pending ...arma.BatchAttestationFragment) (arma.BatchAttestationFragment, error) {
	epoch := int(time.Now().Unix()) / 10

	gc := make([][]byte, 0, len(pending))
	for _, baf := range pending {
		if int(baf.Epoch())+3 < epoch {
			gc = append(gc, baf.Digest())
		}
	}

	baf := &arma.SimpleBatchAttestationFragment{
		Gc:  gc,
		Ep:  epoch,
		Sh:  int(shard),
		Si:  int(id),
		Se:  int(seq),
		Dig: digest,
		P:   int(primary),
	}

	signer := ECDSASigner(*sk)

	tbs := ToBeSignedBAF(baf)

	sig, err := signer.Sign(tbs)
	baf.Sig = sig

	return baf, err
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

func CreateBatcher(conf node_config.BatcherNodeConfig, logger arma.Logger) *Batcher {
	conf = maybeSetDefaultConfig(conf)

	var parties []arma.PartyID
	for shIdx, sh := range conf.Shards {
		if sh.ShardId != conf.ShardId {
			continue
		}

		for _, b := range conf.Shards[shIdx].Batchers {
			parties = append(parties, arma.PartyID(b.PartyID))
		}
		break
	}

	ledgerArray, err := node_ledger.NewBatchLedgerArray(arma.ShardID(conf.ShardId), arma.PartyID(conf.PartyId), parties, conf.Directory, logger)
	if err != nil {
		logger.Panicf("Failed creating BatchLedgerArray: %s", err)
	}

	deliveryService := &node_batcher.BatcherDeliverService{
		LedgerArray: ledgerArray,
		Logger:      logger,
	}

	bp := node_batcher.NewBatchPuller(conf, ledgerArray, logger)

	batcher := NewBatcher(logger, conf, ledgerArray, bp, deliveryService)

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
