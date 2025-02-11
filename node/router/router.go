package router

import (
	"context"
	rand3 "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	rand2 "math/rand"
	"sync/atomic"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/core"
	"github.ibm.com/decentralized-trust-research/arma/node"
	"github.ibm.com/decentralized-trust-research/arma/node/config"
	protos "github.ibm.com/decentralized-trust-research/arma/node/protos/comm"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
)

const (
	defaultRouter2batcherConnPoolSize   = 10
	defaultRouter2batcherStreamsPerConn = 20
)

type Router struct {
	router       core.Router
	shardRouters map[types.ShardID]*ShardRouter
	TLSCert      []byte
	TLSKey       []byte
	logger       types.Logger
	shardIDs     []types.ShardID
	incoming     uint64
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

func createRouter(shardIDs []types.ShardID,
	batcherEndpoints []string,
	batcherRootCAs map[types.ShardID][][]byte,
	tlsCert []byte,
	tlsKey []byte,
	logger types.Logger,
	numOfConnectionsForBatcher int,
	numOfgRPCStreamsPerConnection int,
) *Router {
	if numOfConnectionsForBatcher == 0 {
		numOfConnectionsForBatcher = defaultRouter2batcherConnPoolSize
	}

	if numOfgRPCStreamsPerConnection == 0 {
		numOfgRPCStreamsPerConnection = defaultRouter2batcherStreamsPerConn
	}

	r := &Router{
		router: core.Router{
			Logger:     logger,
			ShardCount: uint16(len(shardIDs)),
		},
		shardRouters: make(map[types.ShardID]*ShardRouter),
		logger:       logger,
		shardIDs:     shardIDs,
	}

	for i, shardId := range shardIDs {
		r.shardRouters[shardId] = NewShardRouter(logger, batcherEndpoints[i], batcherRootCAs[shardId], tlsCert, tlsKey, numOfConnectionsForBatcher, numOfgRPCStreamsPerConnection)
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

func NewRouter(config *config.RouterNodeConfig, logger types.Logger) *Router {
	var shardIDs []types.ShardID
	var batcherEndpoints []string
	tlsCAsOfBatchers := make(map[types.ShardID][][]byte)
	for _, shard := range config.Shards {
		shardIDs = append(shardIDs, shard.ShardId)
		for _, batcher := range shard.Batchers {
			if config.PartyID != batcher.PartyID {
				continue
			}
			batcherEndpoints = append(batcherEndpoints, batcher.Endpoint)
			var tlsCAsOfBatcher [][]byte
			for _, rawTLSCA := range batcher.TLSCACerts {
				tlsCAsOfBatcher = append(tlsCAsOfBatcher, rawTLSCA)
			}

			tlsCAsOfBatchers[shard.ShardId] = tlsCAsOfBatcher
		}
	}
	r := createRouter(shardIDs, batcherEndpoints, tlsCAsOfBatchers, config.TLSCertificateFile, config.TLSPrivateKeyFile, logger, config.NumOfConnectionsForBatcher, config.NumOfgRPCStreamsPerConnection)
	r.init()
	return r
}
