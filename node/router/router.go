package router

import (
	"context"
	rand3 "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	rand2 "math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/config"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node"
	nodeconfig "github.ibm.com/decentralized-trust-research/arma/node/config"
	protos "github.ibm.com/decentralized-trust-research/arma/node/protos/comm"
)

type Net interface {
	Stop()
	Address() string
}

type Router struct {
	router           core.Router
	net              Net
	shardRouters     map[types.ShardID]*ShardRouter
	logger           types.Logger
	shardIDs         []types.ShardID
	incoming         uint64
	routerNodeConfig *nodeconfig.RouterNodeConfig
}

func NewRouter(config *nodeconfig.RouterNodeConfig, logger types.Logger) *Router {
	// shardIDs is an array of all shard ids
	var shardIDs []types.ShardID
	// batcherEndpoints are the endpoints of all batchers from the router's party by shard id
	batcherEndpoints := make(map[types.ShardID]string)
	tlsCAsOfBatchers := make(map[types.ShardID][][]byte)
	for _, shard := range config.Shards {
		shardIDs = append(shardIDs, shard.ShardId)
		for _, batcher := range shard.Batchers {
			if config.PartyID != batcher.PartyID {
				continue
			}
			batcherEndpoints[shard.ShardId] = batcher.Endpoint
			var tlsCAsOfBatcher [][]byte
			for _, rawTLSCA := range batcher.TLSCACerts {
				tlsCAsOfBatcher = append(tlsCAsOfBatcher, rawTLSCA)
			}

			tlsCAsOfBatchers[shard.ShardId] = tlsCAsOfBatcher
		}
	}

	sort.Slice(shardIDs, func(i, j int) bool {
		return int(shardIDs[i]) < int(shardIDs[j])
	})

	r := createRouter(shardIDs, batcherEndpoints, tlsCAsOfBatchers, config, logger)
	r.init()
	return r
}

func (r *Router) StartRouterService() chan struct{} {
	srv := node.CreateGRPCRouter(r.routerNodeConfig)

	protos.RegisterRequestTransmitServer(srv.Server(), r)
	orderer.RegisterAtomicBroadcastServer(srv.Server(), r)

	var waitForGRPCServerStarted sync.WaitGroup

	stop := make(chan struct{})
	waitForGRPCServerStarted.Add(1)

	go func() {
		waitForGRPCServerStarted.Done()
		err := srv.Start()
		if err != nil {
			panic(err)
		}
		close(stop)
	}()

	waitForGRPCServerStarted.Wait()

	r.net = srv

	return stop
}

func (r *Router) Address() string {
	if r.net == nil {
		return ""
	}

	return r.net.Address()
}

func (r *Router) Stop() {
	r.logger.Infof("Stopping router listening on %s, PartyID: %d", r.net.Address(), r.routerNodeConfig.PartyID)

	r.net.Stop()

	if r.shardRouters == nil {
		return
	}

	for _, sr := range r.shardRouters {
		if sr.connPool != nil {
			for _, con := range sr.connPool {
				sr.lock.RLock()
				con.Close()
				sr.lock.RUnlock()
			}
		}
	}
}

func (r *Router) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	clientHost := node.ExtractCertificateFromContext(stream.Context())
	if clientHost != nil {
		r.logger.Debugf("Client %s connected", clientHost.Raw)
	}

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

		if err := router.ForwardBestEffort(reqID, req.Payload); err != nil {
			feedbackChan <- &orderer.BroadcastResponse{Status: common.Status_INTERNAL_SERVER_ERROR, Info: err.Error()}
		} else {
			feedbackChan <- &orderer.BroadcastResponse{Status: common.Status_SUCCESS}
		}
	}
}

func (r *Router) init() {
	for _, shardId := range r.shardIDs {
		r.shardRouters[shardId].MaybeInit()
	}
}

func (r *Router) Deliver(server orderer.AtomicBroadcast_DeliverServer) error {
	return fmt.Errorf("not implemented")
}

func createRouter(shardIDs []types.ShardID, batcherEndpoints map[types.ShardID]string, batcherRootCAs map[types.ShardID][][]byte, rconfig *nodeconfig.RouterNodeConfig, logger types.Logger) *Router {
	if rconfig.NumOfConnectionsForBatcher == 0 {
		rconfig.NumOfConnectionsForBatcher = config.DefaultRouterParams.NumberOfConnectionsPerBatcher
	}

	if rconfig.NumOfgRPCStreamsPerConnection == 0 {
		rconfig.NumOfgRPCStreamsPerConnection = config.DefaultRouterParams.NumberOfStreamsPerConnection
	}

	r := &Router{
		router: core.Router{
			Logger:     logger,
			ShardCount: uint16(len(shardIDs)),
		},
		shardRouters:     make(map[types.ShardID]*ShardRouter),
		logger:           logger,
		shardIDs:         shardIDs,
		routerNodeConfig: rconfig,
	}

	for _, shardId := range shardIDs {
		r.shardRouters[shardId] = NewShardRouter(logger, batcherEndpoints[shardId], batcherRootCAs[shardId], rconfig.TLSCertificateFile, rconfig.TLSPrivateKeyFile, rconfig.NumOfConnectionsForBatcher, rconfig.NumOfgRPCStreamsPerConnection)
	}

	go func() {
		for {
			time.Sleep(time.Second * 10)
			tps := atomic.LoadUint64(&r.incoming)
			r.logger.Infof("Received %d transactions per second", tps/10)
			atomic.StoreUint64(&r.incoming, 0)
		}
	}()

	return r
}

type Response struct {
	err   error
	reqID []byte
	*protos.SubmitResponse
}

func (resp *Response) GetResponseError() error {
	return resp.err
}

func (r *Router) SubmitStream(stream protos.RequestTransmit_SubmitStreamServer) error {
	r.init()

	rand := r.initRand()

	exit := make(chan struct{})
	defer func() {
		close(exit)
	}()

	feedbackChan := make(chan Response, 100)
	go r.sendFeedbackRequestStream(stream, exit, feedbackChan)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		atomic.AddUint64(&r.incoming, 1)

		reqID, router := r.getRouterAndReqID(req)

		trace := createTraceID(rand)

		router.Forward(reqID, req.Payload, feedbackChan, trace)
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
	shardId := r.shardIDs[shardIndex]
	r.logger.Debugf("request %x is mapped to shard %d", req.Payload, shardId)
	router, exists := r.shardRouters[shardId]
	if !exists {
		r.logger.Panicf("Mapped request %d to a non existent shard", shardId)
	}
	return reqID, router
}

func (r *Router) Submit(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	atomic.AddUint64(&r.incoming, 1)
	for _, shardId := range r.shardIDs {
		r.shardRouters[shardId].MaybeInit()
	}

	reqID, router := r.getRouterAndReqID(request)

	trace := createTraceID(nil)

	feedbackChan := make(chan Response, 1)
	router.Forward(reqID, request.Payload, feedbackChan, trace)

	r.logger.Debugf("Forwarded request %x", request.Payload)

	response := <-feedbackChan
	return prepareRequestResponse(&response), nil
}

func (r *Router) sendFeedbackRequestStream(stream protos.RequestTransmit_SubmitStreamServer, exit chan struct{}, errors chan Response) {
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

func prepareRequestResponse(response *Response) *protos.SubmitResponse {
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
