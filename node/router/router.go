/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

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
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/core"
	"github.com/hyperledger/fabric-x-orderer/node"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
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

func (r *Router) StartRouterService() <-chan struct{} {
	srv := node.CreateGRPCRouter(r.routerNodeConfig)

	protos.RegisterRequestTransmitServer(srv.Server(), r)
	orderer.RegisterAtomicBroadcastServer(srv.Server(), r)

	stop := make(chan struct{})

	go func() {
		err := srv.Start()
		if err != nil {
			panic(err)
		}
		close(stop)
	}()

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

	for _, sr := range r.shardRouters {
		sr.Stop()
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

		reqID, shardRouter := r.getShardRouterAndReqID(&protos.Request{Payload: req.Payload, Signature: req.Signature})

		if err := shardRouter.ForwardBestEffort(reqID, req.Payload); err != nil {
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

	verifier := r.createRequestVerifier()

	for _, shardId := range shardIDs {
		r.shardRouters[shardId] = NewShardRouter(logger, batcherEndpoints[shardId], batcherRootCAs[shardId], rconfig.TLSCertificateFile, rconfig.TLSPrivateKeyFile, rconfig.NumOfConnectionsForBatcher, rconfig.NumOfgRPCStreamsPerConnection, verifier)
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

		reqID, shardRouter := r.getShardRouterAndReqID(req)

		trace := createTraceID(rand)

		shardRouter.Forward(reqID, req.Payload, feedbackChan, trace)
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

func (r *Router) getShardRouterAndReqID(req *protos.Request) ([]byte, *ShardRouter) {
	shardIndex, reqID := r.router.Map(req.Payload)
	shardId := r.shardIDs[shardIndex]
	r.logger.Debugf("request %x is mapped to shard %d", req.Payload, shardId)
	shardRouter, exists := r.shardRouters[shardId]
	if !exists {
		r.logger.Panicf("Mapped request %d to a non existent shard", shardId)
	}
	return reqID, shardRouter
}

func (r *Router) Submit(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	atomic.AddUint64(&r.incoming, 1)
	r.init()

	reqID, shardRouter := r.getShardRouterAndReqID(request)

	trace := createTraceID(nil)

	feedbackChan := make(chan Response, 1)
	shardRouter.Forward(reqID, request.Payload, feedbackChan, trace)

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

// this struct implements the interface MaxSizeRuleSupport for the maxSize filter
type routerFilterSupport struct {
	router *Router
}

func (rfs *routerFilterSupport) RouterNodeConfig() (*nodeconfig.RouterNodeConfig, error) {
	if rfs.router.routerNodeConfig == nil {
		return nil, fmt.Errorf("error: bad RouterNodeConfnig")
	}
	return rfs.router.routerNodeConfig, nil
}

func (r *Router) createRequestVerifier() Verifier {
	var requestVerifier Verifier

	requestVerifier.AddRule(PayloadNotEmptyRule)

	requestVerifier.AddRule(AcceptRule) // just for fun

	requestVerifier.AddRule(NewSigVerifier()) // not implemented for now (will accept)

	rfs := routerFilterSupport{router: r}
	requestVerifier.AddRule(NewMaxSizeRule(&rfs))

	return requestVerifier
}

// IsAllStreamsOK checks that all the streams accross all shard-routers are non-faulty.
// Use for testing only.
func (r *Router) IsAllStreamsOK() bool {
	for _, sr := range r.shardRouters {
		if !sr.IsAllStreamsOKinSR() {
			return false
		}
	}
	return true
}

// IsAllConnectionsDown checks that all streams across all shard-routers are disconnected from a batcher.
// Use for testing only.
func (r *Router) IsAllConnectionsDown() bool {
	for _, sr := range r.shardRouters {
		if !sr.IsConnectionsToBatcherDown() {
			return false
		}
	}
	return true
}
