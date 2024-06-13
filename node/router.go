package node

import (
	arma "arma/pkg"
	"context"
	rand3 "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	rand2 "math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.ibm.com/Yacov-Manevich/ARMA/node/comm"
	protos "github.ibm.com/Yacov-Manevich/ARMA/node/protos/comm"
	"google.golang.org/grpc"
)

type Router struct {
	router       arma.Router
	shardRouters map[uint16]*ShardRouter
	TLSCert      []byte
	TLSKey       []byte
	logger       arma.Logger
	shards       []uint16
	incoming     uint64
}

func (r *Router) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	clientHost := ExtractCertificateFromContext(stream.Context())
	r.logger.Debugf("Client %s connected", clientHost)

	r.init()

	exit := make(chan struct{})
	defer func() {
		close(exit)
	}()

	feedbackChan := make(chan *orderer.BroadcastResponse, 1000)

	go func() {
		for {
			select {
			case <-exit:
				return
			case response := <-feedbackChan:
				stream.Send(response)
			}
		}
	}()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		atomic.AddUint64(&r.incoming, 1)

		reqID, router := r.getRouterAndReqID(&protos.Request{Payload: req.Payload, Signature: req.Signature})

		if err := router.forwardBestEffort(reqID, req.Payload); err != nil {
			feedbackChan <- &orderer.BroadcastResponse{Status: common.Status_INTERNAL_SERVER_ERROR, Info: err.Error()}
		} else {
			feedbackChan <- &orderer.BroadcastResponse{Status: common.Status_SUCCESS}
		}
	}
}

func (r *Router) init() {
	for _, shard := range r.shards {
		r.shardRouters[shard].maybeInit()
	}
}

func (r *Router) Deliver(server orderer.AtomicBroadcast_DeliverServer) error {
	return fmt.Errorf("not implemented")
}

func NewRouter(shards []uint16,
	batcherEndpoints []string,
	batcherRootCAs [][][]byte,
	tlsCert []byte,
	tlsKey []byte,
	logger arma.Logger,
	numOfConnectionsForBatcher int,
	numOfgRPCStreamsPerConnection int,
) *Router {
	if numOfConnectionsForBatcher == 0 {
		numOfConnectionsForBatcher = defauultRouter2batcherConnPoolSize
	}

	if numOfgRPCStreamsPerConnection == 0 {
		numOfgRPCStreamsPerConnection = defaultRouter2batcherStreamsPerConn
	}

	r := &Router{shards: shards, shardRouters: make(map[uint16]*ShardRouter), logger: logger, router: arma.Router{Logger: logger, ShardCount: uint16(len(shards))}}
	for i, shard := range shards {
		r.shardRouters[shard] = NewShardRouter(logger, batcherEndpoints[i], batcherRootCAs[i], tlsCert, tlsKey, numOfConnectionsForBatcher, numOfgRPCStreamsPerConnection)
	}

	go func() {
		for {
			time.Sleep(time.Second * 10)
			tps := atomic.LoadUint64(&r.incoming)
			r.logger.Infof("Received %d transactions", tps/10)
			atomic.StoreUint64(&r.incoming, 0)
		}
	}()

	return r
}

type response struct {
	err   error
	reqID []byte
	*protos.SubmitResponse
}

func (r *Router) SubmitStream(stream protos.RequestTransmit_SubmitStreamServer) error {
	r.init()

	rand := r.initRand()

	exit := make(chan struct{})
	defer func() {
		close(exit)
	}()

	feedbackChan := make(chan response, 100)
	go r.sendFeedbackRequestStream(stream, exit, feedbackChan)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		reqID, router := r.getRouterAndReqID(req)

		trace := createTraceID(rand)

		router.forward(reqID, req.Payload, feedbackChan, trace)
	}
}

func (r *Router) initRand() *rand2.Rand {
	seed := make([]byte, 8)
	if _, err := rand3.Read(seed); err != nil {
		panic(err)
	}

	src := rand2.NewSource(int64(binary.BigEndian.Uint64(seed)))
	rand := rand2.New(src)
	return rand
}

func (r *Router) getRouterAndReqID(req *protos.Request) ([]byte, *ShardRouter) {
	shardIndex, reqID := r.router.Map(req.Payload)
	shard := r.shards[shardIndex]
	router, exists := r.shardRouters[shard]
	if !exists {
		r.logger.Panicf("Mapped request %s to a non existent shard", shard)
	}
	return reqID, router
}

func (r *Router) Submit(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	for _, shard := range r.shards {
		r.shardRouters[shard].maybeInit()
	}

	reqID, router := r.getRouterAndReqID(request)

	trace := createTraceID(nil)

	feedbackChan := make(chan response, 1)
	router.forward(reqID, request.Payload, feedbackChan, trace)

	r.logger.Debugf("Forwarded request %x", request.Payload)

	response := <-feedbackChan
	return prepareRequestResponse(&response), nil
}

func (r *Router) sendFeedbackAtomicBroadcast(stream orderer.AtomicBroadcast_BroadcastServer, exit chan struct{}, errors chan response) {
	for {
		select {
		case <-exit:
			return
		case response := <-errors:
			resp := prepareAtomicBroadcastResponse(&response)
			stream.Send(resp)
		}
	}
}

func (r *Router) sendFeedbackRequestStream(stream protos.RequestTransmit_SubmitStreamServer, exit chan struct{}, errors chan response) {
	for {
		select {
		case <-exit:
			return
		case response := <-errors:
			resp := prepareRequestResponse(&response)
			stream.Send(resp)
		}
	}
}

func prepareAtomicBroadcastResponse(response *response) *orderer.BroadcastResponse {
	resp := &orderer.BroadcastResponse{}
	if response.SubmitResponse != nil {
		resp.Status = common.Status_SUCCESS
	} else { // It's an error
		resp.Status = common.Status_INTERNAL_SERVER_ERROR
		resp.Info = response.Error
	}
	return resp
}

func prepareRequestResponse(response *response) *protos.SubmitResponse {
	resp := &protos.SubmitResponse{
		ReqID: response.reqID,
	}
	if response.SubmitResponse != nil {
		resp = response.SubmitResponse
	} else { // It's an error
		resp.Error = response.err.Error()
	}
	return resp
}

type ShardRouter struct {
	router2batcherConnPoolSize   int
	router2batcherStreamsPerConn int
	logger                       arma.Logger
	batcherEndpoint              string
	batcherRootCAs               [][]byte
	once                         sync.Once
	lock                         sync.RWMutex
	connPool                     []*grpc.ClientConn
	streams                      [][]*stream
	tlsCert                      []byte
	tlsKey                       []byte
}

const (
	defauultRouter2batcherConnPoolSize  = 10
	defaultRouter2batcherStreamsPerConn = 20
)

type stream struct {
	endpoint string
	logger   arma.Logger
	protos.RequestTransmit_SubmitStreamClient
	ctx              context.Context
	once             sync.Once
	cancelThisStream func()
	requests         chan *protos.Request
	lock             sync.Mutex
	m                map[string]chan response
}

func (s *stream) readResponses() {
	for {
		resp, err := s.Recv()
		if err != nil {
			s.cancel()
			return
		}

		s.lock.Lock()
		ch, exists := s.m[string(resp.TraceId)]
		delete(s.m, string(resp.TraceId))
		s.lock.Unlock()
		if exists {
			ch <- response{
				SubmitResponse: resp,
			}
		}
	}
}

func (s *stream) sendRequests() {
	for {
		msg := <-s.requests
		err := s.Send(msg)
		if err != nil {
			s.logger.Errorf("Failed sending to %s", s.endpoint)
		}
	}
}

func (s *stream) cancel() {
	s.once.Do(s.cancelThisStream)
}

func (s *stream) faulty() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

func NewShardRouter(l arma.Logger,
	batcherEndpoint string,
	batcherRootCAs [][]byte,
	tlsCert []byte,
	tlsKey []byte,
	numOfConnectionsForBatcher int,
	numOfgRPCStreamsPerConnection int,
) *ShardRouter {
	sr := &ShardRouter{
		tlsCert:                      tlsCert,
		tlsKey:                       tlsKey,
		logger:                       l,
		batcherEndpoint:              batcherEndpoint,
		batcherRootCAs:               batcherRootCAs,
		router2batcherStreamsPerConn: numOfConnectionsForBatcher,
		router2batcherConnPoolSize:   numOfgRPCStreamsPerConnection,
	}

	return sr
}

func (sr *ShardRouter) forwardBestEffort(reqID, request []byte) error {
	connIndex := int(binary.BigEndian.Uint16(reqID)) % len(sr.connPool)
	streamInConnIndex := int(binary.BigEndian.Uint16(reqID)) % sr.router2batcherStreamsPerConn

	sr.lock.RLock()
	stream := sr.streams[connIndex][streamInConnIndex]
	sr.lock.RUnlock()

	if stream == nil || stream.faulty() {
		stream = sr.maybeInitStream(stream, connIndex, streamInConnIndex)
	}

	if stream == nil || stream.faulty() {
		return fmt.Errorf("could not establish stream to %s", sr.batcherEndpoint)
	}

	stream.requests <- &protos.Request{
		Payload: request,
	}
	return nil
}

func (sr *ShardRouter) forward(reqID, request []byte, responses chan response, trace []byte) {
	connIndex := int(binary.BigEndian.Uint16(reqID)) % len(sr.connPool)
	streamInConnIndex := int(binary.BigEndian.Uint16(reqID)) % sr.router2batcherStreamsPerConn

	sr.lock.RLock()
	stream := sr.streams[connIndex][streamInConnIndex]
	sr.lock.RUnlock()

	if stream == nil || stream.faulty() {
		stream = sr.maybeInitStream(stream, connIndex, streamInConnIndex)
	}

	if stream == nil || stream.faulty() {
		responses <- response{
			err:   fmt.Errorf("could not establish stream to %s", sr.batcherEndpoint),
			reqID: reqID,
		}
		return
	}

	stream.registerReply(trace, responses)

	stream.requests <- &protos.Request{
		TraceId: trace,
		Payload: request,
	}
}

func (sr *ShardRouter) maybeInitStream(stream *stream, connIndex int, streamInConnIndex int) *stream {
	sr.lock.Lock()
	defer sr.lock.Unlock()

	stream = sr.streams[connIndex][streamInConnIndex]
	if stream == nil || stream.faulty() {
		sr.initStream(connIndex, streamInConnIndex)
	}
	stream = sr.streams[connIndex][streamInConnIndex]

	return stream
}

func (s *stream) registerReply(traceID []byte, responses chan response) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.m[string(traceID)] = responses
}

func createTraceID(rand *rand2.Rand) []byte {
	var n1, n2 int64
	if rand == nil {
		n1 = rand2.Int63n(math.MaxInt64)
		n2 = rand2.Int63n(math.MaxInt64)
	} else {
		n1 = rand.Int63n(math.MaxInt64)
		n2 = rand.Int63n(math.MaxInt64)
	}

	trace := make([]byte, 16)
	binary.BigEndian.PutUint64(trace, uint64(n1))
	binary.BigEndian.PutUint64(trace[8:], uint64(n2))
	return trace
}

func (sr *ShardRouter) getConnByIndex(connIndex int) *grpc.ClientConn {
	sr.lock.RLock()
	defer sr.lock.RUnlock()

	conn := sr.connPool[connIndex]
	if conn == nil {
		return nil
	}
	return conn
}

func (sr *ShardRouter) maybeInit() {
	sr.initConnPoolAndStreamsOnce()
	sr.maybeConnect()
}

func (sr *ShardRouter) maybeConnect() {
	if !sr.replenishNeeded() {
		return
	}

	sr.replenishConnPool()
	sr.initStreams()
}

func (sr *ShardRouter) replenishNeeded() bool {
	var createConnNeeded bool
	sr.lock.RLock()
	for _, conn := range sr.connPool {
		createConnNeeded = createConnNeeded || conn == nil
	}
	sr.lock.RUnlock()
	return createConnNeeded
}

func (sr *ShardRouter) replenishConnPool() {
	sr.lock.Lock()
	defer sr.lock.Unlock()

	cc := comm.ClientConfig{
		AsyncConnect: true,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: time.Hour,
			ClientTimeout:  time.Hour,
		},
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			ServerRootCAs:     sr.batcherRootCAs,
			Key:               sr.tlsKey,
			Certificate:       sr.tlsCert,
			RequireClientCert: true,
		},
		DialTimeout: time.Second * 5,
	}

	for i, conn := range sr.connPool {
		if conn == nil {
			conn, err := cc.Dial(sr.batcherEndpoint)
			if err != nil {
				sr.logger.Errorf("Failed connecting to %s: %v", sr.batcherEndpoint, err)
			} else {
				sr.connPool[i] = conn
			}
		}
	}
}

func (sr *ShardRouter) initStreams() {
	sr.lock.Lock()
	defer sr.lock.Unlock()

	for i := range sr.connPool {
		sr.initStreamsForConn(i)
	}
}

func (sr *ShardRouter) initStreamsForConn(i int) {
	for j := 0; j < len(sr.streams[i]); j++ {
		sr.initStream(i, j)
	}
}

func (sr *ShardRouter) initStream(i int, j int) {
	if sr.connPool[i] == nil {
		return
	}

	client := protos.NewRequestTransmitClient(sr.connPool[i])
	ctx, cancel := context.WithCancel(context.Background())
	newStream, err := client.SubmitStream(context.Background())
	if err == nil {
		s := &stream{
			endpoint:                           sr.batcherEndpoint,
			logger:                             sr.logger,
			m:                                  make(map[string]chan response),
			requests:                           make(chan *protos.Request, 1000),
			RequestTransmit_SubmitStreamClient: newStream,
			cancelThisStream:                   cancel,
			ctx:                                ctx,
		}
		go s.sendRequests()
		go s.readResponses()
		sr.streams[i][j] = s

	} else {
		sr.logger.Errorf("Failed establishing stream %d to %s: %v", i*sr.router2batcherStreamsPerConn+j, sr.batcherEndpoint, err)
	}
}

func (sr *ShardRouter) initConnPoolAndStreamsOnce() {
	sr.once.Do(func() {
		sr.connPool = make([]*grpc.ClientConn, sr.router2batcherConnPoolSize)
		sr.streams = make([][]*stream, sr.router2batcherConnPoolSize)
		for i := 0; i < len(sr.connPool); i++ {
			sr.streams[i] = make([]*stream, sr.router2batcherStreamsPerConn)
		}
	})
}

type BackendError error

type RoutingError struct {
	BackendErr    string
	TransmitError string
	ReqID         string
}

func (f RoutingError) Error() string {
	return fmt.Sprintf(`{"BackendErr": "%s", "TransmitError": "%s", "ReqID": "%s"}`, f.BackendErr, f.TransmitError, f.ReqID)
}

func CreateRouter(config RouterNodeConfig, logger arma.Logger) *Router {
	var shards []uint16
	var endpoints []string
	var tlsCAs [][][]byte
	for _, shard := range config.Shards {
		shards = append(shards, shard.ShardId)
		for _, batcher := range shard.Batchers {
			if config.PartyID != batcher.PartyID {
				continue
			}
			endpoints = append(endpoints, batcher.Endpoint)
			var tlsCAsOfBatcher [][]byte
			for _, rawTLSCA := range batcher.TLSCACerts {
				tlsCAsOfBatcher = append(tlsCAsOfBatcher, rawTLSCA)
			}

			tlsCAs = append(tlsCAs, tlsCAsOfBatcher)
		}
	}
	r := NewRouter(shards, endpoints, tlsCAs, config.TLSCertificateFile, config.TLSPrivateKeyFile, logger, config.NumOfConnectionsForBatcher, config.NumOfgRPCStreamsPerConnection)
	r.init()
	return r
}
