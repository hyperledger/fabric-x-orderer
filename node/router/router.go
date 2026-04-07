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
	"path/filepath"
	"sort"
	"sync"

	"github.com/hyperledger-labs/SmartBFT/pkg/wal"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/configstore"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/config/verify"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
)

type Net interface {
	Stop()
	Address() string
}

type Router struct {
	mapper       ShardMapper
	net          Net
	logger       *flogging.FabricLogger
	shardIDs     []types.ShardID
	configStore  *configstore.Store
	mainExitChan chan struct{}
	feedbackWG   sync.WaitGroup
	wal          *wal.WriteAheadLogFile
	signer       identity.SignerSerializer

	lock             sync.RWMutex
	status           node_utils.NodeStatus
	configuration    *config.Configuration
	routerNodeConfig *nodeconfig.RouterNodeConfig
	configSeq        uint32
	verifier         *requestfilter.RulesVerifier
	configSubmitter  ConfigurationSubmitter
	shardRouters     map[types.ShardID]*ShardRouter
	decisionPuller   DecisionPuller
	metrics          *RouterMetrics
	stopChan         chan struct{}
	stopOnce         sync.Once
	drainChan        chan struct{}
	drainOnce        sync.Once
}

func NewRouter(config *nodeconfig.RouterNodeConfig, configuration *config.Configuration, logger *flogging.FabricLogger, signer identity.SignerSerializer, mainExitChan chan struct{}, configUpdateProposer policy.ConfigUpdateProposer, configRulesVerifier verify.OrdererRules) *Router {
	logger.Infof("Creating new router with PartyID: %d", config.PartyID)

	// Initialize the router components that remain unchanged after reconfig.

	var shardIDs []types.ShardID
	for _, shard := range config.Shards {
		shardIDs = append(shardIDs, shard.ShardId)
	}

	sort.Slice(shardIDs, func(i, j int) bool {
		return int(shardIDs[i]) < int(shardIDs[j])
	})

	r := &Router{
		logger:       logger,
		signer:       signer,
		mainExitChan: mainExitChan,
		shardIDs:     shardIDs,
		mapper:       CreateMapperCRC64(logger, uint16(len(shardIDs))),
		status:       node_utils.NodeStatus{},
	}

	configStore, err := configstore.NewStore(config.FileStorePath)
	if err != nil {
		r.logger.Panicf("Failed creating router config store: %s", err)
	}
	r.configStore = configStore

	walDir := filepath.Join(config.FileStorePath, "wal")
	routerWAL, walInitState, err := wal.InitializeAndReadAll(r.logger, walDir, wal.DefaultOptions())
	if err != nil {
		r.logger.Panicf("Failed initializing router WAL: %s", err)
	}
	r.wal = routerWAL

	seekInfo := delivery.NextSeekInfo(uint64(getNextDecisionNumber(r.configStore, walInitState, r.logger)))

	r.initFromConfig(config, configuration, configUpdateProposer, configRulesVerifier, seekInfo)

	return r
}

func (r *Router) initFromConfig(rconfig *nodeconfig.RouterNodeConfig, configuration *config.Configuration, configUpdateProposer policy.ConfigUpdateProposer, configRulesVerifier verify.OrdererRules, seekInfo *orderer.SeekInfo) {
	r.lock.Lock()
	defer r.lock.Unlock()

	configSeq := rconfig.Bundle.ConfigtxValidator().Sequence()
	r.logger.Infof("Initializing router with PartyID: %d from config with sequence: %d", rconfig.PartyID, configSeq)

	r.status.Set(node_utils.StateInitializing, configSeq)

	if rconfig.NumOfConnectionsForBatcher == 0 {
		rconfig.NumOfConnectionsForBatcher = config.DefaultRouterParams.NumberOfConnectionsPerBatcher
	}

	if rconfig.NumOfgRPCStreamsPerConnection == 0 {
		rconfig.NumOfgRPCStreamsPerConnection = config.DefaultRouterParams.NumberOfStreamsPerConnection
	}

	r.configuration = configuration
	r.routerNodeConfig = rconfig
	r.configSeq = uint32(configSeq)

	r.verifier = createVerifier(rconfig)

	r.configSubmitter = NewConfigSubmitter(rconfig, r.logger, r.verifier, r.signer, configUpdateProposer, configRulesVerifier)

	// create shard routers
	batcherEndpoints := make(map[types.ShardID]string)
	tlsCAsOfBatchers := make(map[types.ShardID][][]byte)
	for _, shard := range rconfig.Shards {
		for _, batcher := range shard.Batchers {
			if rconfig.PartyID != batcher.PartyID {
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
	r.shardRouters = make(map[types.ShardID]*ShardRouter)
	for _, shardId := range r.shardIDs {
		r.shardRouters[shardId] = NewShardRouter(r.logger, batcherEndpoints[shardId], tlsCAsOfBatchers[shardId], r.routerNodeConfig.TLSCertificateFile, r.routerNodeConfig.TLSPrivateKeyFile, r.routerNodeConfig.NumOfConnectionsForBatcher, r.routerNodeConfig.NumOfgRPCStreamsPerConnection, r.verifier, r.configSubmitter)
	}

	// TODO - pull decisions from all consenter nodes, not only the one in party
	r.decisionPuller = CreateConsensusDecisionReplicator(rconfig, seekInfo, r.logger)

	r.metrics = NewRouterMetrics(rconfig, r.logger)

	// initialize channels and once
	r.stopChan = make(chan struct{})
	r.drainChan = make(chan struct{})
	r.stopOnce = sync.Once{}
	r.drainOnce = sync.Once{}

	r.init()

	r.metrics.Start()

	r.logger.Infof("Router with PartyID: %d has been initialized from config with sequence: %d", rconfig.PartyID, r.configSeq)
}

// getNextDecisionNumber return the number of the next decision to be pulled from consensus, based on the last config block stored in config store and the decision stored in WAL.
func getNextDecisionNumber(configStore *configstore.Store, walInitState [][]byte, logger *flogging.FabricLogger) types.DecisionNum {
	if len(walInitState) > 0 {
		lastWalEntry := walInitState[len(walInitState)-1]
		decision := &state.Header{}
		err := decision.Deserialize(lastWalEntry)
		if err != nil {
			logger.Panicf("Failed deserializing last decision header from router WAL: %s", err)
		}
		logger.Infof("Last decision number in router's WAL is %d", decision.Num)
		// we pull the same decision again, in case the router failed before storing the config block in that decision
		logger.Infof("Router will start pulling consensus decisions from decision number %d", decision.Num)
		return decision.Num
	}

	logger.Infof("No entries in router's WAL")

	// get last config block from config store
	lastBlock, err := configStore.Last()
	if err != nil {
		logger.Panicf("Failed getting last config block from config store: %s", err)
	}

	if lastBlock.Header.Number == 0 {
		logger.Infof("Last config block is genesis block. Router will start pulling consensus decisions from decision number 1")
		return 1
	}

	// last config block is not genesis block, extract decision number from its metadata
	lastConfigBlockDecisionNumber := getDecisionNumberFromConfigBlock(lastBlock, logger)
	logger.Infof("Last config block decision number in router's config store is %d. Router will start pulling consensus decisions from decision number %d", lastConfigBlockDecisionNumber, lastConfigBlockDecisionNumber+1)
	return lastConfigBlockDecisionNumber + 1
}

func getDecisionNumberFromConfigBlock(configBlock *common.Block, logger *flogging.FabricLogger) types.DecisionNum {
	ordererBlockMetadata := configBlock.Metadata.Metadata[common.BlockMetadataIndex_ORDERER]
	_, _, _, lastConfigBlockDecisionNumber, _, _, _, err := ledger.AssemblerBlockMetadataFromBytes(ordererBlockMetadata)
	if err != nil {
		logger.Panicf("Failed extracting decision number from last config block: %s", err)
	}
	return lastConfigBlockDecisionNumber
}

func (r *Router) StartRouterService() {
	r.lock.Lock()
	defer r.lock.Unlock()

	srv := node_utils.CreateGRPCRouter(r.routerNodeConfig)
	r.net = srv

	protos.RegisterRequestTransmitServer(srv.Server(), r)
	orderer.RegisterAtomicBroadcastServer(srv.Server(), r)

	go func() {
		r.logger.Infof("Router network service is starting on %s", srv.Address())
		err := srv.Start()
		if err != nil {
			panic(err)
		}
		r.logger.Infof("Router network service was stopped")
	}()

	r.configSubmitter.Start()

	go r.pullAndProcessDecisions()

	// update the router state.
	r.status.SetState(node_utils.StateRunning)
}

func (r *Router) MonitoringServiceAddress() string {
	return r.metrics.monitor.Address()
}

func (r *Router) Address() string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	if r.net == nil {
		return ""
	}

	return r.net.Address()
}

func (r *Router) Stop() {
	r.lock.Lock()
	defer r.lock.Unlock()

	state := r.status.GetState()
	if state == node_utils.StateStopped {
		return
	}

	r.logger.Infof("Stopping router listening on %s, PartyID: %d", r.net.Address(), r.routerNodeConfig.PartyID)

	if state != node_utils.StateSoftStopped && state != node_utils.StatePendingAdmin {
		r.net.Stop()
		r.metrics.Stop()

		// stop config submitter goroutine
		r.configSubmitter.Stop()

		// stop decision puller goroutine
		r.stopOnce.Do(func() {
			close(r.stopChan)
		})

		for _, sr := range r.shardRouters {
			sr.Stop()
		}
	}

	r.wal.Close()

	r.status.SetState(node_utils.StateStopped)

	r.logger.Infof("Router on %s, PartyID: %d, has been stopped", r.net.Address(), r.routerNodeConfig.PartyID)
	// close the whole process.
	close(r.mainExitChan)
}

func (r *Router) SoftStop() error {
	r.logger.Warnf("Soft stop")
	r.lock.Lock()
	defer r.lock.Unlock()

	state := r.status.GetState()
	if state == node_utils.StateStopped || state == node_utils.StateSoftStopped {
		return fmt.Errorf("soft stop failed: router is already in state: %s", state.String())
	}

	routerAddress := r.net.Address()
	partyID := r.routerNodeConfig.PartyID

	r.logger.Infof("Initiating soft stop of router listening on %s, PartyID: %d", routerAddress, partyID)

	// stop accepting new requests in broadcast and submit handlers
	// closing the stop chan will also stop the decision puller, if needed.
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})

	// next, we stop the shard routers, which will be responsible for sending responses to pending requests
	for _, sr := range r.shardRouters {
		sr.SoftStop(fmt.Errorf("router is stopping, cannot process request"))
	}

	// wait until all feedback channels are drained and all responses are sent
	r.drainOnce.Do(func() {
		close(r.drainChan)
	})
	r.feedbackWG.Wait()

	// then, we stop other components
	r.configSubmitter.Stop()
	r.net.Stop() // this will close all client connections, so some (immediate) responses may not be sent.
	r.metrics.Stop()

	r.status.SetState(node_utils.StateSoftStopped)

	r.logger.Warnf("Router on %s, PartyID: %d, has been soft stopped", routerAddress, partyID)

	return nil
}

func (r *Router) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	clientAddr, err := utils.ExtractClientAddressFromContext(stream.Context())
	if err == nil {
		r.logger.Infof("Client connected: %s", clientAddr)
	}
	if clientCert := utils.ExtractCertificateFromContext(stream.Context()); clientCert != nil {
		r.logger.Infof("Client's certificate: \n%s", utils.CertificateToString(clientCert))
	}

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

		request := &protos.Request{Payload: reqEnv.Payload, Signature: reqEnv.Signature, ConfigSeq: r.configSeq}
		reqID, shardRouter := r.getShardRouterAndReqID(request)

		select {
		case <-r.stopChan:
			r.sendBroadcastResponse(stream, Response{
				err:   fmt.Errorf("router is stopping, cannot process request %x", reqID),
				reqID: reqID,
			})
		default:
			// create a routing request with nil trace. the request is not traced in router.
			tr := &TrackedRequest{request: request, responses: feedbackChan, reqID: reqID}
			shardRouter.Forward(tr)
		}
	}
}

func (r *Router) init() {
	for _, shardId := range r.shardIDs {
		r.shardRouters[shardId].InitShardRouter()
	}
}

func (r *Router) Deliver(server orderer.AtomicBroadcast_DeliverServer) error {
	return fmt.Errorf("not implemented")
}

func (r *Router) SubmitStream(stream protos.RequestTransmit_SubmitStreamServer) error {
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

		select {
		case <-r.stopChan:
			r.sendSubmitResponse(stream, Response{
				err:   fmt.Errorf("router is stopping, cannot process request %x", reqID),
				reqID: reqID,
			})
		default:
			trace := createTraceID(rand)
			tr := &TrackedRequest{request: req, responses: feedbackChan, reqID: reqID, trace: trace}
			tr.request.ConfigSeq = r.configSeq
			shardRouter.Forward(tr)
		}
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

	reqID, shardRouter := r.getShardRouterAndReqID(request)

	trace := createTraceID(nil)

	feedbackChan := make(chan Response, 1)

	tr := &TrackedRequest{request: request, responses: feedbackChan, reqID: reqID, trace: trace}
	tr.request.ConfigSeq = r.configSeq
	shardRouter.Forward(tr)

	r.logger.Debugf("Forwarded request %x", request.Payload)

	var response Response
	select {
	case res := <-feedbackChan:
		response = res
	case <-r.stopChan:
		response = Response{
			err:   fmt.Errorf("router is stopping, cannot process request %x", reqID),
			reqID: reqID,
		}
	case <-ctx.Done():
		response = Response{
			err:   fmt.Errorf("context done before receiving response for request %x: %v", reqID, ctx.Err()),
			reqID: reqID,
		}
	}

	r.metrics.increaseErrorCount(response.err)
	return responseToSubmitResponse(&response), nil
}

func (r *Router) sendFeedbackOnSubmitStream(stream protos.RequestTransmit_SubmitStreamServer, exit chan struct{}, feedbackChan chan Response) {
	r.feedbackWG.Add(1)
	defer r.feedbackWG.Done()
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
		case <-r.drainChan:
			if len(feedbackChan) == 0 {
				return
			}
		}
	}
}

func (r *Router) sendSubmitResponse(stream protos.RequestTransmit_SubmitStreamServer, response Response) {
	err := stream.Send(responseToSubmitResponse(&response))
	if err != nil {
		r.logger.Errorf("error sending response to client: %v", err)
	}
	r.metrics.increaseErrorCount(response.err)
}

func (r *Router) sendFeedbackOnBroadcastStream(stream orderer.AtomicBroadcast_BroadcastServer, exit chan struct{}, feedbackChan chan Response) {
	r.feedbackWG.Add(1)
	defer r.feedbackWG.Done()
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
		case <-r.drainChan:
			if len(feedbackChan) == 0 {
				return
			}
		}
	}
}

func (r *Router) sendBroadcastResponse(stream orderer.AtomicBroadcast_BroadcastServer, response Response) {
	err := stream.Send(responseToBroadcastResponse(&response))
	if err != nil {
		r.logger.Errorf("error sending response to client: %v", err)
	}
	r.metrics.increaseErrorCount(response.err)
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

// pullAndProcessDecisions pulls decisions from consensus and processes them.
// it store the last decision in wal, and config blocks in config store.
// this function should be run as a goroutine.
func (r *Router) pullAndProcessDecisions() {
	decisionsChan := r.decisionPuller.ReplicateDecision()
	defer func() {
		r.decisionPuller.Stop()
		r.logger.Infof("Stopped decision puller")
	}()

	for {
		select {
		case decision, ok := <-decisionsChan:
			if !ok {
				r.logger.Infof("Decisions channel closed, stopping decisions processing")
				return
			}

			// store the decision in WAL, keeping only the last decision
			err := r.wal.Append(decision.Serialize(), true)
			if err != nil {
				r.logger.Panicf("Failed storing decision in router WAL: %s", err)
			}

			// check if the header contains a config block
			if decision.Num != decision.DecisionNumOfLastConfigBlock {
				continue
			}
			block := decision.AvailableCommonBlocks[len(decision.AvailableCommonBlocks)-1]
			blockNum := block.GetHeader().GetNumber()
			if !protoutil.IsConfigBlock(block) {
				r.logger.Errorf("Expected config block but got non-config block number %d", blockNum)
				continue
			}

			r.logger.Infof("Pulled config block number %d from consensus", blockNum)

			// check if the config block should be stored
			lastBlockInStore, err := r.configStore.Last()
			if err != nil {
				r.logger.Panicf("Failed getting last config block from config store: %s", err)
			}
			if lastBlockInStore.Header.Number >= blockNum {
				r.logger.Infof("Config block number %d is not newer than last config block number %d in config store, skipping", blockNum, lastBlockInStore.Header.Number)
				continue
			}

			// store the config block in config store
			if err := r.configStore.Add(block); err != nil {
				r.logger.Panicf("Failed adding config block to config store: %s", err)
			}
			r.logger.Infof("Added config block %d to config store", blockNum)

			go r.processNewConfigBlock(block)

			// do not pull additional decisions, until the router is restarted.
			r.logger.Infof("Stopping decisions pulling from consensus")
			return

		case <-r.stopChan:
			r.logger.Infof("Stopping decisions pulling from consensus")
			return
		}
	}
}

// processNewConfigBlock processes the new config block. First it will soft-stop the router. Then, we try to apply the new config block.
// If the new config contains changes that require admin restart, it will log a warning and return.
// Otherwise, the router will be restarted with the new config dynamically.
func (r *Router) processNewConfigBlock(configBlock *common.Block) {
	r.logger.Infof("Processing new config block number %d", configBlock.Header.Number)

	err := r.SoftStop()
	if err != nil {
		r.logger.Warnf("The router was not Soft-Stopped properly: %v. Admin's action is required", err)
		// TODO - update router status to "pending admin"?
		return
	}

	adminRequired, err := r.ApplyConfig(configBlock)
	if err != nil {
		r.logger.Panicf("Failed to apply last config: %v", err)
	}
	if adminRequired {
		r.logger.Warnf("Pending admin action to apply new config")
		r.lock.Lock()
		r.status.SetState(node_utils.StatePendingAdmin)
		r.lock.Unlock()
	}
}

// ApplyConfig applies the new configuration extracted from the config block, and returns whether admin's action is required and error if exists.
func (r *Router) ApplyConfig(configBlock *common.Block) (bool, error) {
	// extract new router node config from the last config block and configuration.
	newConfiguration, err := r.configuration.NewUpdatedConfigurationFromBlock(configBlock)
	if err != nil {
		return false, fmt.Errorf("failed to extract new configuration from last config block: %v", err)
	}

	// first, check if party is evicted in the new configuration. If yes, an admin action is required.
	evicted, err := config.IsPartyEvicted(r.routerNodeConfig.PartyID, newConfiguration)
	if err != nil {
		return false, fmt.Errorf("failed to check if router's party was evicted in the new configuration: %v", err)
	}
	if evicted {
		r.logger.Warnf("Router's party %d was evicted in the new configuration", r.routerNodeConfig.PartyID)
		return true, nil
	}

	// check if there is a change that requires admin's action.
	currPartyConfig, _ := config.FindParty(r.routerNodeConfig.PartyID, r.configuration)
	newPartyConfig, _ := config.FindParty(r.routerNodeConfig.PartyID, newConfiguration)
	requireRestart, err := config.IsNodeConfigChangeRestartRequired(currPartyConfig.RouterConfig, newPartyConfig.RouterConfig, r.logger)
	if err != nil {
		return false, fmt.Errorf("failed to check if node config change requires restart: %v", err)
	}
	if requireRestart {
		r.logger.Warnf("Router's identity was changed in the new configuration")
		return true, nil
	}

	// extract the new router node config.
	newRouterNodeConfig := newConfiguration.ExtractRouterConfig(configBlock)

	newConfigSeq := newRouterNodeConfig.Bundle.ConfigtxValidator().Sequence()
	r.logger.Infof("Applying new config with sequence %d (current: %d), router will be restarted dynamically", newConfigSeq, r.configSeq)

	seekInfo := delivery.NextSeekInfo(uint64(getDecisionNumberFromConfigBlock(configBlock, r.logger)))

	r.initFromConfig(newRouterNodeConfig, newConfiguration, &policy.DefaultConfigUpdateProposer{}, &verify.DefaultOrdererRules{}, seekInfo)
	r.StartRouterService()
	r.logger.Infof("Router started with new config sequence %d, listening on %s, PartyID: %d", newConfigSeq, r.Address(), r.routerNodeConfig.PartyID)
	return false, nil
}

// IsAllStreamsOK checks that all the streams across all shard-routers are non-faulty.
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

func (r *Router) GetStatus() node_utils.NodeStatus {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.status
}

func (r *Router) IsRunning() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.status.GetState() == node_utils.StateRunning
}
