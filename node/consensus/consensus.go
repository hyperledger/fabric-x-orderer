/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"bytes"
	"context"
	"encoding/asn1"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"math/rand"
	"sync"

	"github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config/verify"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/badb"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/configrequest"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type Storage interface {
	Append([]byte)
	Close()
}

//go:generate counterfeiter -o mocks/net_stopper.go . NetStopper
type NetStopper interface {
	Stop()
}

//go:generate counterfeiter -o mocks/synchronizer_stopper.go . SynchronizerStopper
type SynchronizerStopper interface {
	Stop()
}

type Signer interface {
	Sign(message []byte) ([]byte, error)
	Serialize() ([]byte, error)
}

type SigVerifier interface {
	VerifySignature(id arma_types.PartyID, shardID arma_types.ShardID, msg, sig []byte) error
}

type Arma interface {
	SimulateStateTransition(prevState *state.State, configSeq arma_types.ConfigSequence, events [][]byte) (*state.State, [][]arma_types.BatchAttestationFragment, []*state.ConfigRequest)
	Index([][]byte)
}

type BFT interface {
	SubmitRequest(req []byte) error
	Start() error
	HandleMessage(targetID uint64, m *smartbftprotos.Message)
	HandleRequest(targetID uint64, request []byte)
	Stop()
}

type Consensus struct {
	delivery.DeliverService
	*comm.ClusterService
	Net                          NetStopper
	Config                       *config.ConsenterNodeConfig
	SigVerifier                  SigVerifier
	Signer                       Signer
	CurrentNodes                 []uint64
	BFTConfig                    smartbft_types.Configuration
	BFT                          *consensus.Consensus
	Storage                      Storage
	BADB                         *badb.BatchAttestationDB
	Arma                         Arma
	stateLock                    sync.Mutex
	State                        *state.State
	lastConfigBlockNum           uint64
	decisionNumOfLastConfigBlock arma_types.DecisionNum
	Logger                       arma_types.Logger
	Synchronizer                 SynchronizerStopper
	Metrics                      *ConsensusMetrics
	RequestVerifier              *requestfilter.RulesVerifier
	ConfigUpdateProposer         policy.ConfigUpdateProposer
	ConfigApplier                ConfigApplier
	ConfigRequestValidator       configrequest.ConfigRequestValidator
	ConfigRulesVerifier          verify.OrdererRules
	softStopCh                   chan struct{}
	softStopOnce                 sync.Once
}

func (c *Consensus) Start() error {
	c.softStopCh = make(chan struct{})
	c.Metrics.Start()
	return c.BFT.Start()
}

func (c *Consensus) Stop() {
	c.SoftStop()
	c.Storage.Close()
	c.Net.Stop()
}

func (c *Consensus) SoftStop() {
	c.softStopOnce.Do(func() {
		close(c.softStopCh)
		c.BFT.Stop()
		c.Synchronizer.Stop()
		c.BADB.Close()
		c.Metrics.Stop()
	})
}

func (c *Consensus) OnConsensus(channel string, sender uint64, request *orderer.ConsensusRequest) error {
	msg := &smartbftprotos.Message{}
	if err := proto.Unmarshal(request.Payload, msg); err != nil {
		c.Logger.Warnf("Malformed message: %v", err)
		return errors.Wrap(err, "malformed message")
	}
	c.BFT.HandleMessage(sender, msg)
	return nil
}

func (c *Consensus) OnSubmit(channel string, sender uint64, req *orderer.SubmitRequest) error {
	rawCE := req.Payload.Payload
	var ce state.ControlEvent
	bafd := &state.BAFDeserialize{}
	if err := ce.FromBytes(rawCE, bafd.Deserialize); err != nil {
		c.Logger.Errorf("Failed unmarshaling control event %s: %v", base64.StdEncoding.EncodeToString(rawCE), err)
		return errors.Wrap(err, "failed unmarshaling control event")
	}
	c.BFT.HandleRequest(sender, rawCE)
	return nil
}

func (c *Consensus) NotifyEvent(stream protos.Consensus_NotifyEventServer) error {
	for {
		select {
		case <-c.softStopCh:
			return errors.New("consensus is soft-stopped")
		default:
		}

		event, err := stream.Recv()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		c.Logger.Debugf("Received event %s", printEvent(event.GetPayload()))

		if err := c.SubmitRequest(event.GetPayload()); err != nil {
			c.Logger.Warnf("Failed submitting request: %v", err)
		}
	}
}

// SubmitConfig is used to submit a config request from the router in the consenter's party.
func (c *Consensus) SubmitConfig(ctx context.Context, request *protos.Request) (*protos.SubmitResponse, error) {
	select {
	case <-c.softStopCh:
		return nil, errors.New("consensus is soft-stopped")
	default:
	}

	if err := c.validateRouterFromContext(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to validate router from context")
	}

	c.Logger.Infof("Received config request from router %s", c.Config.Router.Endpoint)

	configRequest, err := c.verifyAndClassifyRequest(request)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to verify and classify request")
	}

	ce := &state.ControlEvent{
		ConfigRequest: &state.ConfigRequest{Envelope: &common.Envelope{
			Payload:   configRequest.Payload,
			Signature: configRequest.Signature,
		}},
	}
	c.BFT.SubmitRequest(ce.Bytes())

	return &protos.SubmitResponse{TraceId: request.TraceId}, nil
}

func (c *Consensus) SubmitRequest(req []byte) error {
	select {
	case <-c.softStopCh:
		return errors.New("consensus is soft-stopped")
	default:
	}

	_, ce, err := c.verifyCE(req)
	if err != nil {
		c.Logger.Warnf("Received bad request: %v", err)
		return err
	}

	// update metrics
	if ce.BAF != nil {
		c.Metrics.bafsCount.Add(1)
	}
	if ce.Complaint != nil {
		c.Metrics.complaintsCount.Add(1)
	}

	return c.BFT.SubmitRequest(req)
}

// VerifyProposal verifies the given proposal and returns the included requests' info
// (from SmartBFT API)
func (c *Consensus) VerifyProposal(proposal smartbft_types.Proposal) ([]smartbft_types.RequestInfo, error) {
	if proposal.Header == nil || proposal.Metadata == nil || proposal.Payload == nil {
		return nil, errors.New("proposal has a nil header or metadata or payload")
	}
	var requests arma_types.BatchedRequests
	if err := requests.Deserialize(proposal.Payload); err != nil {
		return nil, errors.Wrap(err, "failed to deserialize proposal payload")
	}
	var hdr state.Header
	if err := hdr.Deserialize(proposal.Header); err != nil {
		return nil, errors.Wrap(err, "failed to deserialize proposal header")
	}

	if hdr.State == nil {
		return nil, errors.New("state in proposal header is nil")
	}

	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(proposal.Metadata, md); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal proposal metadata")
	}

	if md.LatestSequence != uint64(hdr.Num) {
		return nil, fmt.Errorf("proposed number %d isn't equal to computed number %x", hdr.Num, md.LatestSequence)
	}

	c.stateLock.Lock()
	computedState, attestations, configRequests := c.Arma.SimulateStateTransition(c.State, arma_types.ConfigSequence(c.VerificationSequence()), requests)
	if configRequests != nil {
		var err error
		if computedState, err = c.ConfigApplier.ApplyConfigToState(computedState, configRequests[0]); err != nil {
			return nil, fmt.Errorf("failed applying config to state, err: %s", err)
		}
	}
	lastConfigBlockNum := c.lastConfigBlockNum
	decisionNumOfLastConfigBlock := c.decisionNumOfLastConfigBlock
	c.stateLock.Unlock()

	numOfAvailableBlocks := len(attestations)

	if len(configRequests) > 0 {
		decisionNumOfLastConfigBlock = arma_types.DecisionNum(md.LatestSequence)
		numOfAvailableBlocks++
	}

	verificationSeq := c.VerificationSequence()
	if verificationSeq != uint64(proposal.VerificationSequence) {
		return nil, errors.Errorf("expected verification sequence %d, but proposal has %d", verificationSeq, proposal.VerificationSequence)
	}

	if hdr.DecisionNumOfLastConfigBlock != decisionNumOfLastConfigBlock {
		return nil, fmt.Errorf("proposed decision num of last config block %d isn't equal to computed %d", hdr.DecisionNumOfLastConfigBlock, decisionNumOfLastConfigBlock)
	}

	computedCommonBlocksHeaders := make([]*common.BlockHeader, numOfAvailableBlocks)

	lastCommonBlockHeader := &common.BlockHeader{}
	if err := proto.Unmarshal(computedState.AppContext, lastCommonBlockHeader); err != nil {
		c.Logger.Panicf("Failed unmarshaling app context to block header from state: %v", err)
	}

	lastBlockNumber := lastCommonBlockHeader.Number
	prevHash := protoutil.BlockHeaderHash(lastCommonBlockHeader)

	for i, ba := range attestations {
		lastBlockNumber++

		if err := VerifyDataCommonBlock(hdr.AvailableCommonBlocks[i], lastBlockNumber, prevHash, ba[0], arma_types.DecisionNum(md.LatestSequence), numOfAvailableBlocks, i, lastConfigBlockNum); err != nil {
			return nil, errors.Wrapf(err, "failed verifying proposed block num %d in index %d", lastBlockNumber, i)
		}

		computedCommonBlocksHeaders[i] = &common.BlockHeader{Number: lastBlockNumber, PreviousHash: prevHash, DataHash: ba[0].Digest()}
		prevHash = protoutil.BlockHeaderHash(computedCommonBlocksHeaders[i])
	}

	if len(configRequests) > 0 {
		computedConfigBlockHeader := &common.BlockHeader{Number: lastBlockNumber + 1, PreviousHash: prevHash}
		configReq, err := protoutil.Marshal(configRequests[0].Envelope) // TODO handle when there are multiple requests
		if err != nil {
			c.Logger.Panicf("Failed marshaling config request")
		}
		batchedConfigReq := arma_types.BatchedRequests([][]byte{configReq})
		computedConfigBlockHeader.DataHash = batchedConfigReq.Digest()
		computedCommonBlocksHeaders[numOfAvailableBlocks-1] = computedConfigBlockHeader

		if err := VerifyConfigCommonBlock(hdr.AvailableCommonBlocks[numOfAvailableBlocks-1], lastBlockNumber+1, prevHash, computedConfigBlockHeader.DataHash, arma_types.DecisionNum(md.LatestSequence), numOfAvailableBlocks, numOfAvailableBlocks-1); err != nil {
			return nil, errors.Wrapf(err, "failed verifying proposed config block num %d", lastBlockNumber+1)
		}
	}

	if len(attestations) > 0 || len(configRequests) > 0 {
		computedState.AppContext = protoutil.MarshalOrPanic(computedCommonBlocksHeaders[numOfAvailableBlocks-1])
	}

	if !bytes.Equal(hdr.State.Serialize(), computedState.Serialize()) {
		return nil, fmt.Errorf("proposed state %x isn't equal to computed state %x", hdr.State, computedState)
	}

	reqInfos := make([]smartbft_types.RequestInfo, 0, len(requests))
	for _, rawReq := range requests {
		configSeq, err := c.getReqConfigSeq(rawReq)
		if err != nil {
			return nil, fmt.Errorf("invalid request %s: %v", rawReq, err)
		}
		if configSeq != c.VerificationSequence() {
			continue // ignore (no need to verify) request with mismatch config sequence
		}
		reqID, err := c.VerifyRequest(rawReq)
		if err != nil {
			return nil, fmt.Errorf("invalid request %s: %v", rawReq, err)
		}

		reqInfos = append(reqInfos, reqID)
	}

	return reqInfos, nil
}

// VerifyRequest verifies the given request and returns its info
// (from SmartBFT API)
func (c *Consensus) VerifyRequest(req []byte) (smartbft_types.RequestInfo, error) {
	reqID, _, err := c.verifyCE(req)
	return reqID, err
}

// VerifyConsenterSig verifies the signature for the given proposal
// It returns the auxiliary data in the signature
// (from SmartBFT API)
func (c *Consensus) VerifyConsenterSig(signature smartbft_types.Signature, prop smartbft_types.Proposal) ([]byte, error) {
	var values [][]byte
	if _, err := asn1.Unmarshal(signature.Value, &values); err != nil {
		return nil, err
	}

	if err := c.VerifySignature(smartbft_types.Signature{
		Value: values[0],
		Msg:   []byte(prop.Digest()),
		ID:    signature.ID,
	}); err != nil {
		return nil, errors.Wrap(err, "failed verifying signature over proposal")
	}

	var hdr state.Header
	if err := hdr.Deserialize(prop.Header); err != nil {
		return nil, errors.Wrap(err, "failed deserializing proposal header")
	}

	for i, bh := range hdr.AvailableCommonBlocks {
		if err := c.VerifySignature(smartbft_types.Signature{
			Value: values[i+1],
			Msg:   protoutil.BlockHeaderBytes(bh.Header),
			ID:    signature.ID,
		}); err != nil {
			return nil, errors.Wrap(err, "failed verifying signature over block header")
		}
	}

	return nil, nil
}

// VerifySignature verifies the signature
// (from SmartBFT API)
func (c *Consensus) VerifySignature(signature smartbft_types.Signature) error {
	return c.SigVerifier.VerifySignature(arma_types.PartyID(signature.ID), arma_types.ShardIDConsensus, signature.Msg, signature.Value)
}

// VerificationSequence returns the current verification sequence
// (from SmartBFT API)
func (c *Consensus) VerificationSequence() uint64 {
	return c.Config.Bundle.ConfigtxValidator().Sequence()
}

// RequestsFromProposal returns from the given proposal the included requests' info
// (from SmartBFT API)
func (c *Consensus) RequestsFromProposal(proposal smartbft_types.Proposal) []smartbft_types.RequestInfo {
	var batch arma_types.BatchedRequests
	if err := batch.Deserialize(proposal.Payload); err != nil {
		panic("failed deserializing proposal payload")
	}
	reqInfos := make([]smartbft_types.RequestInfo, 0, len(batch))
	for _, rawReq := range batch {
		reqID, err := c.VerifyRequest(rawReq)
		if err != nil {
			panic(fmt.Errorf("invalid request %s: %v", rawReq, err))
		}

		reqInfos = append(reqInfos, reqID)
	}

	return reqInfos
}

// AuxiliaryData extracts the auxiliary data from a signature's message
// (from SmartBFT API)
func (c *Consensus) AuxiliaryData(i []byte) []byte {
	return nil
}

// Sign signs on the given data and returns the signature
// (from SmartBFT API)
func (c *Consensus) Sign(msg []byte) []byte {
	sig, err := c.Signer.Sign(msg)
	if err != nil {
		panic(err)
	}

	return sig
}

// RequestID returns info about the given request
// (from SmartBFT API)
func (c *Consensus) RequestID(req []byte) smartbft_types.RequestInfo {
	var ce state.ControlEvent
	bafd := &state.BAFDeserialize{}
	if err := ce.FromBytes(req, bafd.Deserialize); err != nil {
		return smartbft_types.RequestInfo{}
	}

	if ce.Complaint == nil && ce.BAF == nil && ce.ConfigRequest == nil {
		c.Logger.Warnf("Empty control event")
		return smartbft_types.RequestInfo{}
	}

	return smartbft_types.RequestInfo{
		ID:       ce.ID(),
		ClientID: ce.SignerID(),
	}
}

// SignProposal signs on the given proposal and returns a composite Signature
// (from SmartBFT API)
func (c *Consensus) SignProposal(proposal smartbft_types.Proposal, _ []byte) *smartbft_types.Signature {
	var requests arma_types.BatchedRequests
	if err := requests.Deserialize(proposal.Payload); err != nil {
		c.Logger.Panicf("Failed deserializing proposal payload: %v", err)
	}

	var hdr state.Header
	if err := hdr.Deserialize(proposal.Header); err != nil {
		c.Logger.Panicf("Failed deserializing proposal header: %v", err)
	}

	sigs := make([][]byte, 0)

	proposalSig, err := c.Signer.Sign([]byte(proposal.Digest()))
	if err != nil {
		c.Logger.Panicf("Failed signing proposal digest: %v", err)
	}

	sigs = append(sigs, proposalSig)

	for _, bh := range hdr.AvailableCommonBlocks {
		msg := protoutil.BlockHeaderBytes(bh.Header)
		sig, err := c.Signer.Sign(msg) // TODO sign like we do in Fabric
		if err != nil {
			c.Logger.Panicf("Failed signing block header: %v", err)
		}

		sigs = append(sigs, sig)
	}

	sigsRaw, err := asn1.Marshal(sigs)
	if err != nil {
		c.Logger.Panicf("Failed marshaling signatures: %v", err)
	}

	return &smartbft_types.Signature{
		// the Msg is defined by VerifyConsenterSig
		Value: sigsRaw,
		ID:    c.BFTConfig.SelfID,
	}
}

// AssembleProposal creates a proposal which includes the given requests (when permitting) and metadata
// (from SmartBFT API)
func (c *Consensus) AssembleProposal(metadata []byte, requests [][]byte) smartbft_types.Proposal {
	c.stateLock.Lock()
	newState, attestations, configRequests := c.Arma.SimulateStateTransition(c.State, arma_types.ConfigSequence(c.VerificationSequence()), requests)
	if configRequests != nil {
		var err error
		if newState, err = c.ConfigApplier.ApplyConfigToState(newState, configRequests[0]); err != nil {
			c.Logger.Panicf("failed applying config to state, err: %s", err)
		}
	}
	lastConfigBlockNum := c.lastConfigBlockNum
	decisionNumOfLastConfigBlock := c.decisionNumOfLastConfigBlock
	c.stateLock.Unlock()

	lastCommonBlockHeader := &common.BlockHeader{}
	if err := proto.Unmarshal(newState.AppContext, lastCommonBlockHeader); err != nil {
		c.Logger.Panicf("Failed unmarshaling app context to block header from state: %v", err)
	}

	lastBlockNumber := lastCommonBlockHeader.Number
	prevHash := protoutil.BlockHeaderHash(lastCommonBlockHeader)

	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(metadata, md); err != nil {
		panic(err)
	}

	numOfAvailableBlocks := len(attestations)
	c.Logger.Infof("Creating proposal with %d attestations", numOfAvailableBlocks)
	if len(configRequests) > 0 {
		numOfAvailableBlocks++
	}
	availableCommonBlocks := make([]*common.Block, numOfAvailableBlocks)

	for i, ba := range attestations {
		lastBlockNumber++

		block, err := CreateDataCommonBlock(lastBlockNumber, prevHash, ba[0], arma_types.DecisionNum(md.LatestSequence), numOfAvailableBlocks, i, lastConfigBlockNum)
		if err != nil {
			c.Logger.Panicf("Failed to create data block: %s", err.Error())
		}

		availableCommonBlocks[i] = block

		prevHash = protoutil.BlockHeaderHash(block.Header)
	}

	if len(configRequests) > 0 {
		c.Logger.Infof("There are %d config requests, creating a config block from the first request", len(configRequests))
		// TODO something when there are a few config request
		configReq, err := protoutil.Marshal(configRequests[0].Envelope)
		if err != nil {
			c.Logger.Panicf("Failed marshaling config request")
		}

		configBlock, err := CreateConfigCommonBlock(lastBlockNumber+1, prevHash, arma_types.DecisionNum(md.LatestSequence), numOfAvailableBlocks, numOfAvailableBlocks-1, configReq)
		if err != nil {
			c.Logger.Panicf("Failed to create config block: %s", err.Error())
		}

		availableCommonBlocks[numOfAvailableBlocks-1] = configBlock

		decisionNumOfLastConfigBlock = arma_types.DecisionNum(md.LatestSequence)
	}

	if len(attestations) > 0 || len(configRequests) > 0 {
		newState.AppContext = protoutil.MarshalOrPanic(availableCommonBlocks[numOfAvailableBlocks-1].Header)
	}

	reqs := arma_types.BatchedRequests(requests)

	return smartbft_types.Proposal{
		Header: (&state.Header{
			AvailableCommonBlocks:        availableCommonBlocks,
			State:                        newState,
			Num:                          arma_types.DecisionNum(md.LatestSequence),
			DecisionNumOfLastConfigBlock: decisionNumOfLastConfigBlock,
		}).Serialize(),
		Metadata:             metadata,
		Payload:              reqs.Serialize(),
		VerificationSequence: int64(c.Config.Bundle.ConfigtxValidator().Sequence()),
	}
}

// Deliver delivers the given proposal and signatures.
// After the call returns we assume that this proposal is stored in persistent memory.
// It returns whether this proposal was a reconfiguration and the current config.
// (from SmartBFT API)
func (c *Consensus) Deliver(proposal smartbft_types.Proposal, signatures []smartbft_types.Signature) smartbft_types.Reconfig {
	rawDecision := state.DecisionToBytes(proposal, signatures)

	hdr := &state.Header{}
	if err := hdr.Deserialize(proposal.Header); err != nil {
		c.Logger.Panicf("Failed deserializing header: %v", err)
	}

	digests := make([][]byte, 0, len(hdr.AvailableCommonBlocks))
	for _, ab := range hdr.AvailableCommonBlocks {
		if !protoutil.IsConfigBlock(ab) {
			digests = append(digests, ab.GetHeader().GetDataHash())
		}
	}

	// Why do we first give Arma the batchAttestations (digests) and then append the decision to storage?
	// Upon commit, Arma indexes the batch attestations (digests) which passed the threshold in its index,
	// to avoid signing them again in the (near) future.
	// If we crash after this, we will replicate the block and will overwrite the index again.
	// However, if we first append the decision and then index afterwards and crash during or right before
	// we index, next time we spawn, we will not recognize we did not index and as a result we will may sign
	// a batch attestation twice.
	// This is true because a Index(digests) with the same digests is idempotent.

	c.Arma.Index(digests)
	c.Storage.Append(rawDecision)

	// update metrics
	c.Metrics.decisionsCount.Add(1)
	c.Metrics.blocksCount.Add(float64(len(hdr.AvailableCommonBlocks)))

	c.stateLock.Lock()
	c.State = hdr.State
	currentNodes := c.CurrentNodes
	currentBFTConfig := c.BFTConfig
	inLatestDecision := false
	if hdr.Num == hdr.DecisionNumOfLastConfigBlock {
		configBlock := hdr.AvailableCommonBlocks[len(hdr.AvailableCommonBlocks)-1]
		lastBlockNum := configBlock.Header.Number
		var err error
		currentNodes, currentBFTConfig, err = c.ConfigApplier.ExtractSmartBFTConfigFromBlock(configBlock, c.Config.PartyId)
		if err != nil {
			c.Logger.Panicf("Failed extracting config from config block: %v", err)
		}
		inLatestDecision = true
		c.Logger.Infof("Delivering config block number %d", lastBlockNum)
		c.decisionNumOfLastConfigBlock = hdr.Num
		if c.lastConfigBlockNum < lastBlockNum {
			c.lastConfigBlockNum = lastBlockNum
			c.Logger.Warnf("Soft stop: pending restart")
			go c.SoftStop()
			// TODO apply reconfig after deliver
		}
	}
	c.stateLock.Unlock()

	return smartbft_types.Reconfig{
		CurrentNodes:     currentNodes,
		CurrentConfig:    currentBFTConfig,
		InLatestDecision: inLatestDecision,
	}
}

func (c *Consensus) pickEndpoint() string {
	var r int
	for {
		r = rand.Intn(len(c.Config.Consenters)) // pick a node randomly
		if c.Config.PartyId != c.Config.Consenters[r].PartyID {
			break // make sure not to pick myself
		}
	}
	c.Logger.Debugf("Returning random node (ID=%d) endpoint : %s", c.Config.Consenters[r].PartyID, c.Config.Consenters[r].Endpoint)
	return c.Config.Consenters[r].Endpoint
}

func (c *Consensus) getReqConfigSeq(req []byte) (uint64, error) {
	ce := &state.ControlEvent{}
	bafd := &state.BAFDeserialize{}
	if err := ce.FromBytes(req, bafd.Deserialize); err != nil {
		return 0, err
	}

	switch {
	case ce.Complaint != nil:
		return uint64(ce.Complaint.ConfigSeq), nil
	case ce.BAF != nil:
		return uint64(ce.BAF.ConfigSequence()), nil
	case ce.ConfigRequest != nil:
		return c.VerificationSequence(), nil
	default:
		return 0, errors.New("empty control event")

	}
}

func (c *Consensus) verifyCE(req []byte) (smartbft_types.RequestInfo, *state.ControlEvent, error) {
	ce := &state.ControlEvent{}
	bafd := &state.BAFDeserialize{}
	if err := ce.FromBytes(req, bafd.Deserialize); err != nil {
		return smartbft_types.RequestInfo{}, nil, err
	}

	reqID := c.RequestID(req)

	configSeq := arma_types.ConfigSequence(c.VerificationSequence())

	if ce.Complaint != nil {
		if ce.Complaint.ConfigSeq != configSeq {
			return reqID, ce, errors.Errorf("mismatch config sequence; the complaint's config seq is %d while the config seq should be %d", ce.Complaint.ConfigSeq, configSeq)
		}
		return reqID, ce, c.SigVerifier.VerifySignature(ce.Complaint.Signer, ce.Complaint.Shard, ce.Complaint.ToBeSigned(), ce.Complaint.Signature)
	} else if ce.BAF != nil {
		if ce.BAF.ConfigSequence() != configSeq {
			return reqID, ce, errors.Errorf("mismatch config sequence; the BAF's config seq is %d while the config seq should be %d", ce.BAF.ConfigSequence(), configSeq)
		}
		return reqID, ce, c.SigVerifier.VerifySignature(ce.BAF.Signer(), ce.BAF.Shard(), toBeSignedBAF(ce.BAF), ce.BAF.Signature())
	} else if ce.ConfigRequest != nil {
		err := c.ConfigRequestValidator.ValidateConfigRequest(ce.ConfigRequest.Envelope)
		if err != nil {
			return reqID, ce, errors.Wrapf(err, "failed to verify and classify request")
		}
		if err := c.ConfigRulesVerifier.ValidateNewConfig(ce.ConfigRequest.Envelope); err != nil {
			return reqID, ce, errors.Wrap(err, "failed to validate rules in new config")
		}
		if err := c.ConfigRulesVerifier.ValidateTransition(c.Config.Bundle, ce.ConfigRequest.Envelope); err != nil {
			return reqID, ce, errors.Wrap(err, "failed to validate config transition rules")
		}
		// TODO: revisit this return
		return reqID, ce, nil
	} else {
		return smartbft_types.RequestInfo{}, ce, fmt.Errorf("empty control event")
	}
}

func (c *Consensus) validateRouterFromContext(ctx context.Context) error {
	// extract the client certificate from the context
	cert := utils.ExtractCertificateFromContext(ctx)
	if cert == nil {
		return errors.New("error: access denied; could not extract certificate from context")
	}

	// extract the router certificate from the ConsenterNodeConfig
	rawRouterCert := c.Config.Router.TLSCert
	pemBlock, _ := pem.Decode(rawRouterCert)
	if pemBlock == nil || pemBlock.Bytes == nil {
		return errors.New("error decoding router TLS certificate")
	}

	// compare the two certificates
	if !bytes.Equal(pemBlock.Bytes, cert.Raw) {
		c.Logger.Errorf("error: access denied. The client certificate does not match the router's certificate. \n client's certificate: \n %s \n %x \n ", utils.CertificateToString(cert), cert.Raw)
		return errors.New("error: access denied. The client certificate does not match the router's certificate")
	}
	return nil
}

func (c *Consensus) verifyAndClassifyRequest(request *protos.Request) (*protos.Request, error) {
	var reqType common.HeaderType

	if err := c.RequestVerifier.Verify(request); err != nil {
		c.Logger.Debugf("request is invalid: %s", err)
		return nil, fmt.Errorf("request verification error: %s", err)
	}

	reqType, err := c.RequestVerifier.VerifyStructureAndClassify(request)
	if err != nil {
		c.Logger.Debugf("request structure is invalid: %s", err)
		return nil, fmt.Errorf("request structure verification error: %s", err)
	}

	// if the request comes from the Router then we expect reqType = HeaderType_CONFIG_UPDATE
	// if the request comes from the Consensus leader then we expect reqType = HeaderType_CONFIG
	if reqType != common.HeaderType_CONFIG_UPDATE && reqType != common.HeaderType_CONFIG {
		c.Logger.Debugf("request has unsupported type: %s", reqType)
		return nil, fmt.Errorf("request structure verification error: request has unsupported type %s", reqType)
	}

	configRequest, err := c.ConfigUpdateProposer.ProposeConfigUpdate(request, c.Config.Bundle, c.Signer, c.RequestVerifier)
	if err != nil {
		return nil, fmt.Errorf("propose config update error: %s", err)
	}

	if configRequest == nil {
		return nil, errors.Errorf("unexpected config request was verified and proposed")
	}

	return configRequest, nil
}
