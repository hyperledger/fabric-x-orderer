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

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node"
	node_config "github.ibm.com/decentralized-trust-research/arma/node/config"
	"github.ibm.com/decentralized-trust-research/arma/node/crypto"
	node_ledger "github.ibm.com/decentralized-trust-research/arma/node/ledger"
	protos "github.ibm.com/decentralized-trust-research/arma/node/protos/comm"
	"github.ibm.com/decentralized-trust-research/arma/request"

	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/pkg/errors"
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

// Signer signs messages
type Signer interface {
	Sign([]byte) ([]byte, error)
}

type Net interface {
	Stop()
}

type Batcher struct {
	batcherDeliverService *BatcherDeliverService
	stateReplicator       StateReplicator
	logger                types.Logger
	batcher               *core.Batcher
	batcherCerts2IDs      map[string]types.PartyID
	controlEventSenders   []ConsenterControlEventSender
	Net                   Net
	Ledger                *node_ledger.BatchLedgerArray
	config                *node_config.BatcherNodeConfig
	batchers              []node_config.BatcherInfo
	privateKey            *ecdsa.PrivateKey
	signer                Signer

	stateRef  atomic.Value
	stateChan chan *core.State

	running  sync.WaitGroup
	stopOnce sync.Once
	stopChan chan struct{}

	primaryLock sync.Mutex
	term        uint64
	primaryID   types.PartyID
	ackStream   protos.BatcherControlService_NotifyAckClient
	reqStream   protos.BatcherControlService_FwdRequestStreamClient
	cancelFunc  context.CancelFunc
}

func (b *Batcher) Run() {
	b.stopChan = make(chan struct{})

	b.stateChan = make(chan *core.State, 1)

	go b.replicateState()

	b.logger.Infof("Starting batcher")
	b.batcher.Start()
}

func (b *Batcher) Stop() {
	b.logger.Infof("Stopping batcher node")
	b.stopOnce.Do(func() { close(b.stopChan) })
	b.batcher.Stop()
	for len(b.stateChan) > 0 {
		<-b.stateChan // drain state channel
	}
	b.Net.Stop()
	b.cleanupPrimaryStreams()
	b.Ledger.Close()
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
			primaryID, term := b.getPrimaryIDAndTerm(state)
			b.primaryLock.Lock()
			if b.primaryID != primaryID { // TODO better to check term change?
				b.logger.Infof("Primary changed from %d to %d", b.primaryID, primaryID)
				b.primaryID = primaryID
				b.term = term
				b.cleanupPrimaryStreams()
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

func (b *Batcher) extractBatcherFromContext(c context.Context) (types.PartyID, error) {
	cert := node.ExtractCertificateFromContext(c)
	if cert == nil {
		return 0, errors.New("access denied; could not extract certificate from context")
	}

	from, exists := b.batcherCerts2IDs[string(cert.Raw)]
	if !exists {
		return 0, errors.Errorf("access denied; unknown certificate; %s", node.CertificateToString(cert))
	}

	return from, nil
}

func (b *Batcher) FwdRequestStream(stream protos.BatcherControlService_FwdRequestStreamServer) error {
	from, err := b.extractBatcherFromContext(stream.Context())
	if err != nil {
		return errors.Errorf("Could not extract batcher from context; err %v", err)
	}
	b.logger.Infof("Starting to handle fwd requests from batcher %d", from)

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		b.logger.Debugf("Calling submit request from batcher %d", from)
		// TODO verify request before submitting
		if err := b.batcher.Submit(msg.Request); err != nil {
			b.logger.Infof("Failed to submit request from batcher %d; err: %v", from, err)
		}
	}
}

func (b *Batcher) NotifyAck(stream protos.BatcherControlService_NotifyAckServer) error {
	from, err := b.extractBatcherFromContext(stream.Context())
	if err != nil {
		return errors.Errorf("Could not extract batcher from context; err %v", err)
	}

	b.logger.Infof("Starting to handle acks from batcher %d", from)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		b.logger.Debugf("Calling handle ack with seq %d from batcher %d", msg.Seq, from)
		b.batcher.HandleAck(types.BatchSequence(msg.Seq), from)
	}
}

func (b *Batcher) indexTLSCerts() {
	for _, batcher := range b.batchers {
		rawTLSCert := batcher.TLSCert
		bl, _ := pem.Decode(rawTLSCert)
		if bl == nil || bl.Bytes == nil {
			b.logger.Panicf("Failed decoding TLS certificate of batcher %d from PEM", batcher.PartyID)
		}

		b.batcherCerts2IDs[string(bl.Bytes)] = batcher.PartyID
	}
}

func NewBatcher(logger types.Logger, config *node_config.BatcherNodeConfig, ledger *node_ledger.BatchLedgerArray, bp core.BatchPuller, ds *BatcherDeliverService, sr StateReplicator, senderCreator ConsenterControlEventSenderCreator, net Net) *Batcher {
	privateKey := createPrivateKey(logger, config.SigningPrivateKey)

	b := &Batcher{
		batcherDeliverService: ds,
		stateReplicator:       sr,
		privateKey:            privateKey,
		signer:                crypto.ECDSASigner(*privateKey),
		logger:                logger,
		Net:                   net,
		Ledger:                ledger,
		batcherCerts2IDs:      make(map[string]types.PartyID),
		config:                config,
	}

	b.controlEventSenders = make([]ConsenterControlEventSender, len(config.Consenters))
	for i, consenterInfo := range config.Consenters {
		b.controlEventSenders[i] = senderCreator.CreateConsenterControlEventSender(config.TLSPrivateKeyFile, config.TLSCertificateFile, consenterInfo)
	}

	b.batchers = batchersFromConfig(config)
	if len(b.batchers) == 0 {
		logger.Panicf("Failed locating the configuration of our shard (%d) among %v", config.ShardId, config.Shards)
	}

	initState := computeZeroState(config)
	b.stateRef.Store(&initState)

	b.primaryID, b.term = b.getPrimaryIDAndTerm(&initState)

	b.indexTLSCerts()

	f := (initState.N - 1) / 3

	b.batcher = &core.Batcher{
		Batchers:     getBatchersIDs(b.batchers),
		BatchPuller:  bp,
		Threshold:    int(f + 1),
		N:            initState.N,
		BatchTimeout: time.Millisecond * 500,
		Ledger:       ledger,
		MemPool:      b.createMemPool(config),
		ID:           config.PartyId,
		Shard:        config.ShardId,
		Logger:       logger,
		Digest: func(data [][]byte) []byte {
			batch := types.BatchedRequests(data)
			digest := sha256.Sum256(batch.Serialize())
			return digest[:]
		},
		StateProvider:    b,
		RequestInspector: b,
		BAFCreator:       b,
		BAFSender:        b,
		BatchAcker:       b,
		Complainer:       b,
	}

	return b
}

func getBatchersIDs(batchers []node_config.BatcherInfo) []types.PartyID {
	var parties []types.PartyID
	for _, batcher := range batchers {
		parties = append(parties, batcher.PartyID)
	}

	return parties
}

func (b *Batcher) createMemPool(config *node_config.BatcherNodeConfig) core.MemPool {
	opts := defaultBatcherMemPoolOpts
	opts.BatchMaxSizeBytes = config.BatchMaxBytes
	opts.MaxSize = config.MemPoolMaxSize
	opts.BatchMaxSize = config.BatchMaxSize
	opts.RequestMaxBytes = config.RequestMaxBytes
	opts.SubmitTimeout = config.BatchTimeout
	opts.Striker = b
	return request.NewPool(b.logger, b, opts)
}

func (b *Batcher) OnFirstStrikeTimeout(req []byte) {
	b.logger.Debugf("First strike timeout occurred on request %s", b.RequestID(req))
	b.sendReq(req)
}

func (b *Batcher) OnSecondStrikeTimeout() {
	b.logger.Warnf("Second strike timeout occurred")
	b.Complain(fmt.Sprintf("batcher %d (shard %d) complaining; second strike timeout occurred", b.config.PartyId, b.config.ShardId))
}

func createPrivateKey(logger types.Logger, signingPrivateKey node_config.RawBytes) *ecdsa.PrivateKey {
	bl, _ := pem.Decode(signingPrivateKey)

	if bl == nil || bl.Bytes == nil {
		logger.Panicf("Signing key is not a valid PEM")
	}

	sk, err := x509.ParsePKCS8PrivateKey(bl.Bytes)
	if err != nil {
		logger.Panicf("Signing key is not a valid PKCS8 private key: %v", err)
	}

	return sk.(*ecdsa.PrivateKey)
}

func (b *Batcher) CreateBAF(seq types.BatchSequence, primary types.PartyID, shard types.ShardID, digest []byte) core.BatchAttestationFragment {
	baf, err := CreateBAF(b.signer, b.config.PartyId, shard, digest, primary, seq)
	if err != nil {
		b.logger.Panicf("Failed creating batch attestation fragment: %v", err)
	}

	return baf
}

func (b *Batcher) GetPrimaryID() types.PartyID {
	b.primaryLock.Lock()
	defer b.primaryLock.Unlock()
	return b.primaryID
}

func (b *Batcher) getPrimaryIDAndTerm(state *core.State) (types.PartyID, uint64) {
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

	return primaryID, term
}

func (b *Batcher) createComplaint(reason string) *core.Complaint {
	b.primaryLock.Lock()
	term := b.term
	b.primaryLock.Unlock()
	c, err := CreateComplaint(b.signer, b.config.PartyId, b.config.ShardId, term, reason)
	if err != nil {
		b.logger.Panicf("Failed creating complaint: %v", err)
	}

	return c
}

func computeZeroState(config *node_config.BatcherNodeConfig) core.State {
	var state core.State
	for _, shard := range config.Shards {
		state.Shards = append(state.Shards, core.ShardTerm{
			Shard: shard.ShardId,
		})
	}

	state.N = uint16(len(config.Consenters))

	return state
}

func batchersFromConfig(config *node_config.BatcherNodeConfig) []node_config.BatcherInfo {
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

func (b *Batcher) sendReq(req []byte) {
	t1 := time.Now()

	defer func() {
		b.logger.Debugf("Sending req took %v", time.Since(t1))
	}()

	_, reqStream, err := b.getStreamsToPrimary()
	if err != nil {
		b.primaryLock.Lock()
		b.logger.Errorf("Failed getting stream to primary %d for reqs; error: %v", b.primaryID, err)
		b.cleanupPrimaryStreams()
		b.primaryLock.Unlock()
		return
	}

	if err := reqStream.Send(&protos.FwdRequest{Request: req}); err != nil {
		b.primaryLock.Lock()
		b.logger.Errorf("Failed sending req (ID: %s) to primary %d; error: %v", b.RequestID(req), b.primaryID, err)
		b.cleanupPrimaryStreams()
		b.primaryLock.Unlock()
	}
}

func (b *Batcher) Ack(seq types.BatchSequence, to types.PartyID) {
	t1 := time.Now()

	defer func() {
		b.logger.Debugf("Sending ack took %v", time.Since(t1))
	}()

	b.primaryLock.Lock()
	if to != b.primaryID {
		b.logger.Warnf("Trying to send ack to %d while primary is %d", to, b.primaryID)
		b.primaryLock.Unlock()
		return
	}
	b.primaryLock.Unlock()

	ackStream, _, err := b.getStreamsToPrimary()
	if err != nil {
		b.primaryLock.Lock()
		b.logger.Errorf("Failed getting stream to primary %d for ack; error: %v", b.primaryID, err)
		b.cleanupPrimaryStreams()
		b.primaryLock.Unlock()
		return
	}

	if err := ackStream.Send(&protos.Ack{Shard: uint32(b.config.ShardId), Seq: uint64(seq)}); err != nil {
		b.primaryLock.Lock()
		b.logger.Errorf("Failed sending ack to primary %d; error: %v", b.primaryID, err)
		b.cleanupPrimaryStreams()
		b.primaryLock.Unlock()
	}
}

func (b *Batcher) getStreamsToPrimary() (protos.BatcherControlService_NotifyAckClient, protos.BatcherControlService_FwdRequestStreamClient, error) {
	b.primaryLock.Lock()
	for b.ackStream == nil || b.reqStream == nil {
		primary := b.primaryID
		var c context.Context
		c, b.cancelFunc = context.WithCancel(context.Background())
		b.primaryLock.Unlock()

		ackStream, reqStream, err := b.primaryStreamsFactory(primary, c)
		if err != nil {
			b.primaryLock.Lock()
			b.cleanupPrimaryStreams()
			b.primaryLock.Unlock()
			return nil, nil, errors.Errorf("Failed connecting to primary %d; error: %v", primary, err)
		}

		b.primaryLock.Lock()
		if primary != b.primaryID { // if primary changed again then reconnect
			b.cleanupPrimaryStreams()
			continue
		}
		b.ackStream = ackStream
		b.reqStream = reqStream
	}
	ackStream := b.ackStream
	reqStream := b.reqStream
	b.primaryLock.Unlock()
	return ackStream, reqStream, nil
}

func (b *Batcher) primaryStreamsFactory(primary types.PartyID, c context.Context) (protos.BatcherControlService_NotifyAckClient, protos.BatcherControlService_FwdRequestStreamClient, error) {
	primaryEndpoint := ""
	var primaryTLSCACerts []node_config.RawBytes
	for _, batcher := range b.batchers {
		if batcher.PartyID == primary {
			b.logger.Debugf("Primary for shard %d is: %d (endpoint: %s)", b.config.ShardId, primary, batcher.Endpoint)
			primaryEndpoint = batcher.Endpoint
			primaryTLSCACerts = batcher.TLSCACerts
			break
		}
	}

	if primaryEndpoint == "" {
		b.logger.Panicf("Could not find primaryID %d of shard %d within %v", primary, b.config.ShardId, b.batchers)
		return nil, nil, errors.Errorf("Could not find primaryID %d of shard %d within %v", primary, b.config.ShardId, b.batchers)
	}

	cc := clientConfig(b.config.TLSPrivateKeyFile, b.config.TLSCertificateFile, primaryTLSCACerts)

	conn, err := cc.Dial(primaryEndpoint)
	if err != nil {
		return nil, nil, errors.Errorf("Failed connecting to primary %d (endpoint %s); error: %v", primary, primaryEndpoint, err)
	}

	primaryClient := protos.NewBatcherControlServiceClient(conn)

	ackStream, err := primaryClient.NotifyAck(c)
	if err != nil {
		return nil, nil, errors.Errorf("Failed creating ack stream to primary %d (endpoint %s); error: %v", primary, primaryEndpoint, err)
	}

	reqStream, err := primaryClient.FwdRequestStream(c)
	if err != nil {
		return nil, nil, errors.Errorf("Failed creating req stream to primary %d (endpoint %s); error: %v", primary, primaryEndpoint, err)
	}

	b.logger.Debugf("Created streams to primary (party ID = %d)", primary)
	return ackStream, reqStream, nil
}

func (b *Batcher) cleanupPrimaryStreams() {
	if b.cancelFunc != nil {
		b.cancelFunc()
	}
	b.ackStream = nil
	b.reqStream = nil
}

func (b *Batcher) Complain(reason string) {
	b.broadcastControlEvent(core.ControlEvent{Complaint: b.createComplaint(reason)})
}

func (b *Batcher) SendBAF(baf core.BatchAttestationFragment) {
	b.logger.Infof("Sending batch attestation fragment for seq %d with digest %x", baf.Seq(), baf.Digest())
	b.broadcastControlEvent(core.ControlEvent{BAF: baf})
}

func (b *Batcher) broadcastControlEvent(ce core.ControlEvent) {
	for _, sender := range b.controlEventSenders {
		b.sendControlEvent(ce, sender) // TODO make sure somehow we sent to 2f+1, and retry if not
	}
}

func (b *Batcher) sendControlEvent(ce core.ControlEvent, sender ConsenterControlEventSender) {
	t1 := time.Now()

	defer func() {
		b.logger.Infof("Sending control event took %v", time.Since(t1))
	}()

	if err := sender.SendControlEvent(ce); err != nil {
		b.logger.Errorf("Failed sending control event; err: %v", err)
		// TODO do something when sending fails
	}
}

func (b *Batcher) RequestID(req []byte) string {
	// TODO maybe calculate the request ID differently
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}

func CreateComplaint(signer Signer, id types.PartyID, shard types.ShardID, term uint64, reason string) (*core.Complaint, error) {
	c := &core.Complaint{
		ShardTerm: core.ShardTerm{Shard: shard, Term: term},
		Signer:    id,
		Signature: nil,
		Reason:    reason,
	}
	sig, err := signer.Sign(c.ToBeSigned())
	if err != nil {
		return nil, err
	}
	c.Signature = sig

	return c, nil
}

func CreateBAF(signer Signer, id types.PartyID, shard types.ShardID, digest []byte, primary types.PartyID, seq types.BatchSequence) (core.BatchAttestationFragment, error) {
	baf := types.NewSimpleBatchAttestationFragment(shard, primary, seq, digest, id, nil, 0, nil)
	sig, err := signer.Sign(baf.ToBeSigned())
	if err != nil {
		return nil, err
	}
	baf.SetSignature(sig)

	return baf, nil
}

func CreateBatcher(conf *node_config.BatcherNodeConfig, logger types.Logger, net Net, csrc ConsensusStateReplicatorCreator, senderCreator ConsenterControlEventSenderCreator) *Batcher {
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

	batcher := NewBatcher(logger, conf, ledgerArray, bp, deliveryService, csrc.CreateStateConsensusReplicator(conf, logger), senderCreator, net)

	return batcher
}

func maybeSetDefaultConfig(conf *node_config.BatcherNodeConfig) *node_config.BatcherNodeConfig {
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
