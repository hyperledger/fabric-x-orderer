package node

import (
	arma "arma/pkg"
	"arma/request"
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"github.com/hyperledger/fabric-lib-go/common/metrics/disabled"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blockledger/fileledger"
	"io"
	"math"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm"
	protos "github.ibm.com/Yacov-Manevich/ARMA/node/protos/comm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var (
	opts = request.PoolOptions{
		AutoRemoveTimeout:     time.Second * 10,
		BatchMaxSize:          1000 * 2,
		BatchMaxSizeBytes:     1024 * 1024 * 10,
		MaxSize:               1000 * 1000,
		FirstStrikeThreshold:  time.Second * 3,
		SecondStrikeThreshold: time.Second * 3,
		SubmitTimeout:         time.Second * 5,
		RequestMaxBytes:       1024 * 1024,
	}
)

type Batcher struct {
	ds               DeliverService
	logger           arma.Logger
	b                *arma.Batcher
	batcherCerts2IDs map[string]arma.PartyID
	primaryEndpoint  string
	primaryTLSCA     []RawBytes
	request.Pool
	consensusClients []protos.ConsensusClient
	primaryClient    protos.AckServiceClient
	primaryConn      *grpc.ClientConn
	connections      []*grpc.ClientConn
	config           BatcherNodeConfig
	sk               *ecdsa.PrivateKey
	tlsKey           []byte
	tlsCert          []byte
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
		b.logger.Infof("Submitting request %x", traceId)

		if err := b.b.Submit(rawReq); err != nil {
			resp.Error = err.Error()
		}

		responses <- &resp

		b.logger.Infof("Submitted request %x", traceId)

	}
}

func (b *Batcher) sendResponses(stream protos.RequestTransmit_SubmitStreamServer, responses chan *protos.SubmitResponse, stop chan struct{}) {
	for {
		select {
		case resp := <-responses:
			b.logger.Infof("Sending response %x", resp.TraceId)
			stream.Send(resp)
			b.logger.Infof("Sent response %x", resp.TraceId)
		case <-stop:
			b.logger.Infof("Stopped sending responses")
			return
		}
	}
}

func (b *Batcher) NotifyAck(ctx context.Context, ack *protos.Ack) (*protos.EventResponse, error) {
	cert := ExtractCertificateFromContext(ctx)
	from, exists := b.batcherCerts2IDs[string(cert)]
	if !exists {
		return nil, fmt.Errorf("access denied")
	}

	b.b.HandleAck(ack.Seq, from)
	return &protos.EventResponse{}, nil
}

func NewBatcher(logger arma.Logger, config BatcherNodeConfig, ledger arma.BatchLedger, bp arma.BatchPuller, ds DeliverService) *Batcher {

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

	b.indexTLSCerts()

	b.b = &arma.Batcher{
		BatchPuller:          bp,
		Threshold:            2,
		BatchTimeout:         time.Millisecond * 50,
		Ledger:               ledger,
		AttestationFromBytes: BatchAttestationFromBytes,
		MemPool:              b.createMemPool(),
		AttestBatch:          b.attestBatch,
		State:                computeZeroState(config),
		ID:                   arma.PartyID(config.PartyId),
		Shard:                arma.ShardID(config.ShardId),
		Logger:               logger,
		Digest: func(data [][]byte) []byte {
			batch := arma.BatchedRequests(data)
			digest := sha256.Sum256(batch.ToBytes())
			return digest[:]
		},
		RequestInspector: b,
		TotalOrderBAF:    b.broadcastEvent,
		AckBAF:           b.sendAck,
	}

	b.setupPrimaryEndpoint(logger, config, b.b)

	f := (b.b.State.N - 1) / 3
	b.b.Threshold = int(f + 1)

	return b
}

func (b *Batcher) createMemPool() arma.MemPool {
	opts := opts
	opts.OnFirstStrikeTimeout = func(key []byte) {
		b.logger.Panicf("First strike timeout occurred on request %s", b.b.RequestInspector.RequestID(key))
	}
	opts.OnSecondStrikeTimeout = func() {
		b.logger.Panicf("second strike timeout occurred")
	}

	return request.NewPool(b.logger, b, opts)
}

func getEndpoint(logger arma.Logger, config BatcherNodeConfig) string {
	var endpoint string
	for _, shard := range config.Shards {
		if shard.ShardId != config.ShardId {
			continue
		}

		for _, batcher := range shard.Batchers {
			if batcher.PartyID != config.PartyId {
				continue
			}

			endpoint = batcher.Endpoint
		}
	}

	if endpoint == "" {
		logger.Panicf("Could not find our endpoint in shard definition %v", config.Shards)
	}

	return endpoint
}

func (b *Batcher) indexTLSCerts() {
	batchers := batchersFromConfig(b.logger, b.config)
	for _, batcher := range batchers {
		rawTLSCert := batcher.TLSCert
		bl, _ := pem.Decode(rawTLSCert)
		if bl == nil {
			b.logger.Panicf("Failed decoding TLS certificate of %d from PEM", batcher.PartyID)
		}

		b.batcherCerts2IDs[string(bl.Bytes)] = arma.PartyID(batcher.PartyID)
	}
}

func createSigner(logger arma.Logger, config BatcherNodeConfig) *ecdsa.PrivateKey {
	rawKey := config.SigningPrivateKey
	bl, _ := pem.Decode(rawKey)

	if bl == nil {
		logger.Panicf("Signing key is not a valid PEM")
	}

	sk, err := x509.ParsePKCS8PrivateKey(bl.Bytes)
	if err != nil {
		logger.Panicf("Signing key is not a valid PKCS8 private key: %v", err)
	}

	return sk.(*ecdsa.PrivateKey)
}

func (b *Batcher) attestBatch(seq uint64, primary arma.PartyID, shard arma.ShardID, digest []byte) arma.BatchAttestationFragment {
	baf, err := createBAF(b.sk, b.config.PartyId, uint16(shard), digest, uint16(primary), seq)
	if err != nil {
		b.logger.Panicf("Failed creating batch attestation fragment: %v", err)
	}

	return baf
}

func (b *Batcher) setupPrimaryEndpoint(logger arma.Logger, config BatcherNodeConfig, batcher *arma.Batcher) {
	batchers := batchersFromConfig(logger, config)

	primary := b.getPrimary(batcher, config)

	for _, batcher := range batchers {
		if arma.PartyID(batcher.PartyID) == primary {
			b.primaryEndpoint = batcher.Endpoint
			b.primaryTLSCA = batcher.TLSCACerts
			return

		}
	}

	logger.Panicf("Could not find primary of shard %d within %v", config.ShardId, batchers)
}

func (b *Batcher) getPrimary(batcher *arma.Batcher, config BatcherNodeConfig) arma.PartyID {
	term := uint64(math.MaxUint64)
	for _, shard := range batcher.State.Shards {
		if shard.Shard == batcher.Shard {
			term = shard.Term
		}
	}

	if term == math.MaxUint64 {
		b.logger.Panicf("Could not find our shard (%d) within the shards: %v", batcher.Shard, batcher.State.Shards)
	}

	primary := arma.PartyID((uint64(config.ShardId) + term) % uint64(batcher.State.N))

	return primary
}

func computeZeroState(config BatcherNodeConfig) arma.State {
	var state arma.State
	for _, shard := range config.Shards {
		state.Shards = append(state.Shards, arma.ShardTerm{
			Shard: arma.ShardID(shard.ShardId),
		})
	}

	state.N = uint16(len(config.Consenters))

	return state
}

func batchersFromConfig(logger arma.Logger, config BatcherNodeConfig) []BatcherInfo {
	var batchers []BatcherInfo
	for _, shard := range config.Shards {
		if shard.ShardId == config.ShardId {
			batchers = shard.Batchers
		}
	}

	if len(batchers) == 0 {
		logger.Panicf("Failed locating the configuration of our shard (%d) among %v", config.ShardId, config.Shards)
	}
	return batchers
}

func (b *Batcher) sendAck(seq uint64, to arma.PartyID) {
	for b.primaryConn == nil {
		cc := b.clientConfig(b.primaryTLSCA)

		conn, err := cc.Dial(b.primaryEndpoint)
		if err != nil {
			b.logger.Errorf("Failed connecting to %s: %v", b.primaryEndpoint, err)
			continue
		}

		b.primaryConn = conn
		b.primaryClient = protos.NewAckServiceClient(conn)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := b.primaryClient.NotifyAck(ctx, &protos.Ack{Shard: uint32(b.config.ShardId), Seq: seq})
	if err != nil {
		b.logger.Errorf("Failed sending ack to %s", b.primaryEndpoint)
		b.primaryConn.Close()
		b.primaryConn = nil
		b.primaryClient = nil
	}

}

func (b *Batcher) broadcastEvent(baf arma.BatchAttestationFragment) {
	b.initClients()

	var wg sync.WaitGroup
	wg.Add(len(b.consensusClients))

	for index := range b.consensusClients {
		b.initConsenterConnIfNeeded(index)
	}

	for index, cl := range b.consensusClients {
		go func(baf arma.BatchAttestationFragment, cl protos.ConsensusClient, index int) {
			defer wg.Done()
			b.sendBAF(baf, cl, index)
		}(baf, cl, index)
	}

	wg.Wait()
}

func (b *Batcher) sendBAF(baf arma.BatchAttestationFragment, cl protos.ConsensusClient, index int) {
	if b.connections[index] == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var ce arma.ControlEvent
	ce.BAF = baf

	resp, err := cl.NotifyEvent(ctx, &protos.Event{
		Payload: ce.Bytes(),
	})

	if err != nil {
		b.connections[index].Close()
		b.logger.Errorf("Failed sending batch attestation fragment to %d (%s): %v", b.config.Consenters[index].PartyID, b.config.Consenters[index].Endpoint, err)
		b.connections[index] = nil
		b.consensusClients[index] = nil
		return
	}

	if resp.Error != "" {
		b.logger.Errorf("Remote consensus node %d failed receiving the batch attestation share: %v", b.config.Consenters[index].PartyID, resp.Error)
	}
}

func (b *Batcher) initClients() {
	if len(b.connections) == 0 {
		b.connections = make([]*grpc.ClientConn, len(b.config.Consenters))
		b.consensusClients = make([]protos.ConsensusClient, len(b.config.Consenters))
	}
}

func (b *Batcher) initConsenterConnIfNeeded(index int) {
	if b.connections[index] != nil {
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
	b.consensusClients[index] = protos.NewConsensusClient(conn)
}

func (b *Batcher) clientConfig(TlsCACert []RawBytes) comm.ClientConfig {
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

func createBAF(sk *ecdsa.PrivateKey, id uint16, shard uint16, digest []byte, primary uint16, seq uint64) (arma.BatchAttestationFragment, error) {
	baf := &arma.SimpleBatchAttestationFragment{
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

type BatchLedger interface {
	arma.BatchLedger
	Height() uint64
}

func CreateBatcher(conf BatcherNodeConfig, logger arma.Logger) *Batcher {

	provider, err := blkstorage.NewProvider(
		blkstorage.NewConf(conf.Directory, -1),
		&blkstorage.IndexConfig{
			AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrBlockNum},
		}, &disabled.Provider{})
	if err != nil {
		logger.Panicf("Failed creating block provider: %v", err)
	}

	ds := make(DeliverService)

	name := fmt.Sprintf("shard%d", conf.ShardId)

	ledger, err := provider.Open(name)
	if err != nil {
		logger.Panicf("Failed opening shard %s")
	}

	fl := fileledger.NewFileLedger(ledger)

	ds[name] = fl

	cert := conf.TLSCertificateFile

	tlsKey := conf.TLSPrivateKeyFile

	batcherLedger := &BatcherLedger{Ledger: fl, Logger: logger}

	bp := &BatchPuller{
		getHeight: batcherLedger.Height,
		logger:    logger,
		config:    conf,
		ledger:    batcherLedger,
		tlsCert:   cert,
		tlsKey:    tlsKey,
	}

	batcher := NewBatcher(logger, conf, batcherLedger, bp, ds)
	return batcher
}
