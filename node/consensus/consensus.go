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
	"encoding/hex"
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
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/badb"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/delivery"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type Storage interface {
	Append([]byte)
	Close()
}

type Net interface {
	Stop()
}

type Signer interface {
	Sign(message []byte) ([]byte, error)
}

type SigVerifier interface {
	VerifySignature(id arma_types.PartyID, shardID arma_types.ShardID, msg, sig []byte) error
}

type Arma interface {
	SimulateStateTransition(prevState *state.State, events [][]byte) (*state.State, [][]arma_types.BatchAttestationFragment, []*state.ConfigRequest)
	Commit(events [][]byte)
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
	Net          Net
	Config       *config.ConsenterNodeConfig
	SigVerifier  SigVerifier
	Signer       Signer
	CurrentNodes []uint64
	BFTConfig    smartbft_types.Configuration
	BFT          *consensus.Consensus
	Storage      Storage
	BADB         *badb.BatchAttestationDB
	Arma         Arma
	stateLock    sync.Mutex
	State        *state.State
	Logger       arma_types.Logger
	Synchronizer *synchronizer
	Metrics      *ConsensusMetrics
}

func (c *Consensus) Start() error {
	c.Metrics.Start()
	return c.BFT.Start()
}

func (c *Consensus) Stop() {
	c.BFT.Stop()
	c.Synchronizer.stop()
	c.BADB.Close()
	c.Storage.Close()
	c.Net.Stop()
	c.Metrics.Stop()
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
	if err := c.validateRouterFromContext(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to validate router from context")
	}

	c.Logger.Infof("Received config request from router %s", c.Config.Router.Endpoint)

	return &protos.SubmitResponse{Error: "SubmitConfig is not implemented in consenter", TraceId: request.TraceId}, nil
}

func (c *Consensus) SubmitRequest(req []byte) error {
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
	var batch arma_types.BatchedRequests
	if err := batch.Deserialize(proposal.Payload); err != nil {
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

	if hdr.DecisionNumOfLastConfigBlock != arma_types.DecisionNum(0) {
		return nil, fmt.Errorf("proposed decision num of last config block %d isn't equal to 0", hdr.DecisionNumOfLastConfigBlock) // TODO change when reconfig
	}

	c.stateLock.Lock()
	computedState, attestations, _ := c.Arma.SimulateStateTransition(c.State, batch)
	c.stateLock.Unlock()

	availableCommonBlocks := make([]*common.Block, len(attestations))

	for i, ba := range attestations {
		if !bytes.Equal(hdr.AvailableCommonBlocks[i].Header.DataHash, ba[0].Digest()) {
			return nil, fmt.Errorf("proposed available common block %s data hash in index %d isn't equal to computed digest %s", arma_types.CommonBlockToString(hdr.AvailableCommonBlocks[i]), i, arma_types.BatchIDToString(ba[0]))
		}
	}

	lastCommonBlockHeader := &common.BlockHeader{}
	if err := proto.Unmarshal(computedState.AppContext, lastCommonBlockHeader); err != nil {
		c.Logger.Panicf("Failed unmarshaling app context to block header from state: %v", err)
	}

	lastBlockNumber := lastCommonBlockHeader.Number
	prevHash := protoutil.BlockHeaderHash(lastCommonBlockHeader)

	for i, ba := range attestations {
		var bh state.BlockHeader
		bh.Digest = ba[0].Digest()
		lastBlockNumber++

		if hex.EncodeToString(hdr.AvailableCommonBlocks[i].Header.PreviousHash) != hex.EncodeToString(prevHash) {
			return nil, fmt.Errorf("proposed common block header prev hash %s in index %d isn't equal to computed prev hash %s, last block number is %d", hex.EncodeToString(hdr.AvailableCommonBlocks[i].Header.PreviousHash), i, hex.EncodeToString(prevHash), lastBlockNumber)
		}

		availableCommonBlocks[i] = protoutil.NewBlock(lastBlockNumber, prevHash)
		availableCommonBlocks[i].Header.DataHash = ba[0].Digest()
		bh.Number = lastBlockNumber
		bh.PrevHash = prevHash
		prevHash = protoutil.BlockHeaderHash(availableCommonBlocks[i].Header)

		if hdr.AvailableCommonBlocks[i].Header.Number != lastBlockNumber {
			return nil, fmt.Errorf("proposed common block header number %d in index %d isn't equal to computed number %d", hdr.AvailableCommonBlocks[i].Header.Number, i, lastBlockNumber)
		}

		blockMetadata, err := ledger.AssemblerBlockMetadataToBytes(ba[0], &state.OrderingInformation{DecisionNum: arma_types.DecisionNum(md.LatestSequence), BatchCount: len(attestations), BatchIndex: i}, 0)
		if err != nil {
			c.Logger.Panicf("Failed to invoke AssemblerBlockMetadataToBytes: %s", err)
		}

		if hdr.AvailableCommonBlocks[i].Metadata == nil || hdr.AvailableCommonBlocks[i].Metadata.Metadata == nil {
			return nil, fmt.Errorf("proposed common block metadata in index %d is nil", i)
		}

		if !bytes.Equal(blockMetadata, hdr.AvailableCommonBlocks[i].Metadata.Metadata[common.BlockMetadataIndex_ORDERER]) {
			return nil, fmt.Errorf("proposed common block metadata in index %d isn't equal to computed metadata", i)
		}

	}

	if len(attestations) > 0 {
		computedState.AppContext = protoutil.MarshalOrPanic(availableCommonBlocks[len(attestations)-1].Header)
	}

	if !bytes.Equal(hdr.State.Serialize(), computedState.Serialize()) {
		return nil, fmt.Errorf("proposed state %x isn't equal to computed state %x", hdr.State, computedState)
	}

	reqInfos := make([]smartbft_types.RequestInfo, 0, len(batch))
	for _, rawReq := range batch {
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
	return 0 // TODO save current verification sequence and return it here
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

	c.stateLock.Lock()
	_, bafs, _ := c.Arma.SimulateStateTransition(c.State, requests)
	c.stateLock.Unlock()

	sigs := make([][]byte, 0, len(bafs)+1)

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
	// TODO: config request not handled yet
	newState, attestations, _ := c.Arma.SimulateStateTransition(c.State, requests)
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

	c.Logger.Infof("Creating proposal with %d attestations", len(attestations))

	availableCommonBlocks := make([]*common.Block, len(attestations))

	for i, ba := range attestations {
		var hdr state.BlockHeader
		hdr.Digest = ba[0].Digest()
		lastBlockNumber++
		availableCommonBlocks[i] = protoutil.NewBlock(lastBlockNumber, prevHash)
		availableCommonBlocks[i].Header.DataHash = ba[0].Digest()
		blockMetadata, err := ledger.AssemblerBlockMetadataToBytes(ba[0], &state.OrderingInformation{DecisionNum: arma_types.DecisionNum(md.LatestSequence), BatchCount: len(attestations), BatchIndex: i}, 0)
		if err != nil {
			c.Logger.Panicf("Failed to invoke AssemblerBlockMetadataToBytes: %s", err)
		}
		protoutil.InitBlockMetadata(availableCommonBlocks[i])
		availableCommonBlocks[i].Metadata.Metadata[common.BlockMetadataIndex_ORDERER] = blockMetadata
		hdr.Number = lastBlockNumber
		hdr.PrevHash = prevHash
		prevHash = protoutil.BlockHeaderHash(availableCommonBlocks[i].Header)
	}

	if len(attestations) > 0 {
		newState.AppContext = protoutil.MarshalOrPanic(availableCommonBlocks[len(attestations)-1].Header)
	}

	reqs := arma_types.BatchedRequests(requests)

	return smartbft_types.Proposal{
		Header: (&state.Header{
			AvailableCommonBlocks:        availableCommonBlocks,
			State:                        newState,
			Num:                          arma_types.DecisionNum(md.LatestSequence),
			DecisionNumOfLastConfigBlock: arma_types.DecisionNum(0), // TODO update when reconfig
		}).Serialize(),
		Metadata: metadata,
		Payload:  reqs.Serialize(),
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

	var controlEvents arma_types.BatchedRequests
	if err := controlEvents.Deserialize(proposal.Payload); err != nil {
		c.Logger.Panicf("Failed deserializing proposal payload: %v", err)
	}

	// Why do we first give Arma the events and then append the decision to storage?
	// Upon commit, Arma indexes the batch attestations which passed the threshold in its index,
	// to avoid signing them again in the (near) future.
	// If we crash after this, we will replicate the block and will overwrite the index again.
	// However, if we first commit the decision and then index afterwards and crash during or right before
	// we index, next time we spawn, we will not recognize we did not index and as a result we will may sign
	// a batch attestation twice.
	// This is true because a Commit(controlEvents) with the same controlEvents is idempotent.
	c.Arma.Commit(controlEvents)
	c.Storage.Append(rawDecision)

	// update metrics
	c.Metrics.decisionsCount.Add(1)
	c.Metrics.blocksCount.Add(uint64(len(hdr.AvailableCommonBlocks)))

	c.stateLock.Lock()
	c.State = hdr.State
	c.stateLock.Unlock()

	return smartbft_types.Reconfig{
		CurrentNodes:  c.CurrentNodes,
		CurrentConfig: c.BFTConfig,
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

func (c *Consensus) verifyCE(req []byte) (smartbft_types.RequestInfo, *state.ControlEvent, error) {
	ce := &state.ControlEvent{}
	bafd := &state.BAFDeserialize{}
	if err := ce.FromBytes(req, bafd.Deserialize); err != nil {
		return smartbft_types.RequestInfo{}, nil, err
	}

	reqID := c.RequestID(req)

	if ce.Complaint != nil {
		return reqID, ce, c.SigVerifier.VerifySignature(ce.Complaint.Signer, ce.Complaint.Shard, ce.Complaint.ToBeSigned(), ce.Complaint.Signature)
	} else if ce.BAF != nil {
		return reqID, ce, c.SigVerifier.VerifySignature(ce.BAF.Signer(), ce.BAF.Shard(), toBeSignedBAF(ce.BAF), ce.BAF.Signature())
	} else {
		return smartbft_types.RequestInfo{}, ce, fmt.Errorf("empty control event")
	}
}

func (c *Consensus) validateRouterFromContext(ctx context.Context) error {
	// extract the client certificate from the context
	cert := node.ExtractCertificateFromContext(ctx)
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
		c.Logger.Errorf("error: access denied. The client certificate does not match the router's certificate. \n client's certificate: \n %s \n %x \n ", node.CertificateToString(cert), cert.Raw)
		return errors.New("error: access denied. The client certificate does not match the router's certificate")
	}
	return nil
}
