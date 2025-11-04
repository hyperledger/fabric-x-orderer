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

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
)

type Net interface {
	Stop()
	Address() string
}

type ConfigPuller interface {
	PullConfigBlocks() <-chan *common.Block
	Stop()
	Update() // TODO - implement thread-safe update method.
}

type Router struct {
	mapper           ShardMapper
	net              Net
	shardRouters     map[types.ShardID]*ShardRouter
	logger           types.Logger
	shardIDs         []types.ShardID
	routerNodeConfig *nodeconfig.RouterNodeConfig
	verifier         *requestfilter.RulesVerifier
	configStore      *configstore.Store
	configSubmitter  ConfigurationSubmitter
	configPuller     ConfigPuller
	metrics          *RouterMetrics
	stopChan         chan struct{}
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

	var tlsCAsOfConsenter [][]byte
	for _, rawTLSCA := range config.Consenter.TLSCACerts {
		tlsCAsOfConsenter = append(tlsCAsOfConsenter, rawTLSCA)
	}
	configSubmitter := NewConfigSubmitter(config.Consenter.Endpoint, tlsCAsOfConsenter,
		config.TLSCertificateFile, config.TLSPrivateKeyFile, logger)

	configStore, err := configstore.NewStore(config.ConfigStorePath)
	if err != nil {
		logger.Panicf("Failed creating router config store: %s", err)
	}

	seekInfo := NextSeekInfoFromConfigStore(configStore, logger)

	// TODO - pull config blocks from all consenter nodes, not only the one in party
	configPuller := delivery.NewConsensusConfigPuller(config, logger, seekInfo)

	verifier := createVerifier(config)
	metrics := NewRouterMetrics(config, logger, config.MetricsLogInterval)

	r := createRouter(shardIDs, batcherEndpoints, tlsCAsOfBatchers, metrics, config, logger, verifier, configStore, configSubmitter, configPuller)
	r.init()
	r.metrics.Start()
	return r
}

// NextSeekInfoFromConfigStore creates a SeekInfo to start pulling config blocks from consensus, based on the last connfig block stored in the config store.
func NextSeekInfoFromConfigStore(configStore *configstore.Store, logger types.Logger) *orderer.SeekInfo {
	lastBlock, err := configStore.Last()
	if err != nil {
		logger.Panicf("Failed getting last config block from config store: %s", err)
	}

	// check if last block is genesis block
	if lastBlock.GetHeader().GetNumber() == 0 {
		// skip genesis block that is placed in decision 0
		return delivery.NextSeekInfo(1)
	}

	ordererBlockMetadata := lastBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER]
	_, _, _, lastDecisionNumber, _, _, _, err := ledger.AssemblerBlockMetadataFromBytes(ordererBlockMetadata)
	if err != nil {
		logger.Panicf("Failed extracting decision number from last config block: %s", err)
	}
	return delivery.NextSeekInfo(uint64(lastDecisionNumber) + 1)
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

	r.configSubmitter.Start()

	go r.pullAndProcessConfigBlocks()

	return stop
}

func (r *Router) MonitoringServiceAddress() string {
	return r.metrics.monitor.Address()
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
	r.metrics.Stop()

	r.configSubmitter.Stop()
	close(r.stopChan) // stop config puller goroutine

	for _, sr := range r.shardRouters {
		sr.Stop()
	}
}

func (r *Router) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	clientAddr, err := node.ExtractClientAddressFromContext(stream.Context())
	if err == nil {
		r.logger.Infof("Client connected: %s", clientAddr)
	}
	if clientCert := node.ExtractCertificateFromContext(stream.Context()); clientCert != nil {
		r.logger.Infof("Client's certificate: \n%s", node.CertificateToString(clientCert))
	}

	r.init()

	exit := make(chan struct{})
	defer func() {
		close(exit)
	}()

	feedbackChan := make(chan Response, 1000)
	go r.sendFeedbackOnBroadcastStream(stream, exit, feedbackChan)

	for {
		reqEnv, err := stream.Recv()
		if err == io.EOF {
			r.logger.Infof("Received EOF from stream, closing broadcast from client %s", clientAddr)
			return nil
		}
		if err != nil {
			r.logger.Infof("Received error from stream: %v, closing broadcastfrom client %s", err, clientAddr)
			return err
		}

		r.metrics.incomingTxs.Add(1)

		request := &protos.Request{Payload: reqEnv.Payload, Signature: reqEnv.Signature}
		reqID, shardRouter := r.getShardRouterAndReqID(request)

		// creating a routing request with nil trace - request is not trce in router.
		tr := &TrackedRequest{request: request, responses: feedbackChan, reqID: reqID}
		shardRouter.Forward(tr)
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

func createRouter(shardIDs []types.ShardID, batcherEndpoints map[types.ShardID]string, batcherRootCAs map[types.ShardID][][]byte, metrics *RouterMetrics, rconfig *nodeconfig.RouterNodeConfig, logger types.Logger, verifier *requestfilter.RulesVerifier, configStore *configstore.Store, configSubmitter ConfigurationSubmitter, configPuller ConfigPuller) *Router {
	if rconfig.NumOfConnectionsForBatcher == 0 {
		rconfig.NumOfConnectionsForBatcher = config.DefaultRouterParams.NumberOfConnectionsPerBatcher
	}

	if rconfig.NumOfgRPCStreamsPerConnection == 0 {
		rconfig.NumOfgRPCStreamsPerConnection = config.DefaultRouterParams.NumberOfStreamsPerConnection
	}

	r := &Router{
		mapper: MapperCRC64{
			Logger:     logger,
			ShardCount: uint16(len(shardIDs)),
		},
		shardRouters:     make(map[types.ShardID]*ShardRouter),
		logger:           logger,
		shardIDs:         shardIDs,
		routerNodeConfig: rconfig,
		verifier:         verifier,
		configStore:      configStore,
		configSubmitter:  configSubmitter,
		configPuller:     configPuller,
		stopChan:         make(chan struct{}),
		metrics:          metrics,
	}

	for _, shardId := range shardIDs {
		r.shardRouters[shardId] = NewShardRouter(logger, batcherEndpoints[shardId], batcherRootCAs[shardId], rconfig.TLSCertificateFile, rconfig.TLSPrivateKeyFile, rconfig.NumOfConnectionsForBatcher, rconfig.NumOfgRPCStreamsPerConnection, verifier, configSubmitter)
	}

	return r
}

func (r *Router) SubmitStream(stream protos.RequestTransmit_SubmitStreamServer) error {
	r.init()

	rand := r.initRand()

	exit := make(chan struct{})
	defer func() {
		close(exit)
	}()

	feedbackChan := make(chan Response, 100)
	go r.sendFeedbackOnSubmitStream(stream, exit, feedbackChan)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		r.metrics.incomingTxs.Add(1)

		reqID, shardRouter := r.getShardRouterAndReqID(req)

		trace := createTraceID(rand)
		tr := &TrackedRequest{request: req, responses: feedbackChan, reqID: reqID, trace: trace}
		shardRouter.Forward(tr)
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
	shardIndex, reqID := r.mapper.Map(req.Payload)
	shardId := r.shardIDs[shardIndex]
	r.logger.Debugf("request %x is mapped to shard %d", req.Payload, shardId)
	shardRouter, exists := r.shardRouters[shardId]
	if !exists {
		r.logger.Panicf("Mapped request %d to a non existent shard", shardId)
	}
	return reqID, shardRouter
}

func (r *Router) Submit(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	r.metrics.incomingTxs.Add(1)
	r.init()

	reqID, shardRouter := r.getShardRouterAndReqID(request)

	trace := createTraceID(nil)

	feedbackChan := make(chan Response, 1)

	tr := &TrackedRequest{request: request, responses: feedbackChan, reqID: reqID, trace: trace}
	shardRouter.Forward(tr)

	r.logger.Debugf("Forwarded request %x", request.Payload)

	response := <-feedbackChan // TODO: add select on shutdwon or timeout here
	r.metrics.increaseErrorCount(response.err)
	return responseToSubmitResponse(&response), nil
}

func (r *Router) sendFeedbackOnSubmitStream(stream protos.RequestTransmit_SubmitStreamServer, exit chan struct{}, feedbackChan chan Response) {
	for {
		select {
		case <-exit:
			return
		case response := <-feedbackChan:
			r.metrics.increaseErrorCount(response.err)
			resp := responseToSubmitResponse(&response)
			err := stream.Send(resp)
			if err != nil {
				r.logger.Errorf("error sending response to client: %v", err)
			}
		}
	}
}

func (r *Router) sendFeedbackOnBroadcastStream(stream orderer.AtomicBroadcast_BroadcastServer, exit chan struct{}, feedbackChan chan Response) {
	for {
		select {
		case <-exit:
			return
		case response := <-feedbackChan:
			err := stream.Send(responseToBroadcastResponse(&response))
			if err != nil {
				r.logger.Errorf("error sending response to client: %v", err)
			}
			r.metrics.increaseErrorCount(response.err)
		}
	}
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

func createVerifier(config *nodeconfig.RouterNodeConfig) *requestfilter.RulesVerifier {
	rv := requestfilter.NewRulesVerifier(nil)
	rv.AddRule(requestfilter.PayloadNotEmptyRule{})
	rv.AddRule(requestfilter.NewMaxSizeFilter(config))
	rv.AddStructureRule(requestfilter.NewSigFilter(config, policies.ChannelWriters))
	return rv
}

// pullAndProcessConfigBlocks pulls config blocks from consensus and processes them. this function should be run as a goroutine.
func (r *Router) pullAndProcessConfigBlocks() {
	configBlocksChan := r.configPuller.PullConfigBlocks()
	defer func() {
		r.configPuller.Stop()
		r.logger.Infof("Stopped config puller")
	}()

	for {
		select {
		case configBlock, ok := <-configBlocksChan:
			if !ok {
				r.logger.Infof("Config blocks channel closed, stopping config blocks processing")
				return
			}
			r.logger.Infof("Received new config block from consensus with block number %d", configBlock.GetHeader().GetNumber())

			// TODO process the config block. store in config store and apply.
			if err := r.configStore.Add(configBlock); err != nil {
				r.logger.Panicf("Failed adding config block to config store: %s", err)
			}
			r.logger.Infof("Added config block %d to config store", configBlock.GetHeader().GetNumber())

		case <-r.stopChan:
			r.logger.Infof("Stopping config blocks processing")
			return
		}
	}
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

// GetConfigStoreSize returns the number of config blocks stored in the config store.
// Use for testing only.
func (r *Router) GetConfigStoreSize() int {
	list, err := r.configStore.ListBlockNumbers()
	if err != nil {
		r.logger.Panicf("Failed listing config store block numbers: %s", err)
	}
	return len(list)
}
