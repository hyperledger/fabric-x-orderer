package consensus

import (
	"bytes"
	"encoding/asn1"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"sync"

	arma_types "arma/common/types"
	"arma/core"
	"arma/node/comm"
	"arma/node/config"
	"arma/node/consensus/state"
	"arma/node/delivery"
	protos "arma/node/protos/comm"

	"github.com/hyperledger-labs/SmartBFT/pkg/consensus"
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger-labs/SmartBFT/smartbftprotos"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type Storage interface {
	Append([]byte)
}

type Signer interface {
	Sign(message []byte) ([]byte, error)
}

type SigVerifier interface {
	VerifySignature(id arma_types.PartyID, shardID arma_types.ShardID, msg, sig []byte) error
}

type Arma interface {
	SimulateStateTransition(prevState *core.State, events [][]byte) (*core.State, [][]core.BatchAttestationFragment)
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
	Config       config.ConsenterNodeConfig
	SigVerifier  SigVerifier
	Signer       Signer
	CurrentNodes []uint64
	BFTConfig    types.Configuration
	BFT          *consensus.Consensus
	Storage      Storage
	Arma         Arma
	stateLock    sync.Mutex
	State        *core.State
	Logger       arma_types.Logger
}

func (c *Consensus) Start() error {
	return c.BFT.Start()
}

func (c *Consensus) Stop() {
	c.BFT.Stop()
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
	var ce core.ControlEvent
	bafd := &state.BAFDeserializer{}
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

		c.Logger.Infof("Received event %x", event.Payload)

		if err := c.SubmitRequest(event.GetPayload()); err != nil {
			c.Logger.Warnf("Failed submitting request: %v", err)
		}
	}
}

func (c *Consensus) SubmitRequest(req []byte) error {
	if _, err := c.VerifyRequest(req); err != nil {
		c.Logger.Warnf("Received bad request: %v", err)
		return err
	}
	return c.BFT.SubmitRequest(req)
}

// VerifyProposal verifies the given proposal and returns the included requests' info
// (from SmartBFT API)
func (c *Consensus) VerifyProposal(proposal types.Proposal) ([]types.RequestInfo, error) {
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

	c.stateLock.Lock()
	computedState, attestations := c.Arma.SimulateStateTransition(c.State, batch)
	c.stateLock.Unlock()

	availableBlocks := make([]state.AvailableBlock, len(attestations))
	for i, ba := range attestations {
		availableBlocks[i].Batch = state.NewAvailableBatch(ba[0].Primary(), ba[0].Shard(), ba[0].Seq(), ba[0].Digest())
	}

	for i, ab := range hdr.AvailableBlocks {
		if !ab.Batch.Equal(availableBlocks[i].Batch) {
			return nil, fmt.Errorf("proposed available batch %v in index %d isn't equal to computed available batch %v", availableBlocks[i].Batch, i, ab)
		}
	}

	var lastBlockHeader state.BlockHeader
	if err := lastBlockHeader.FromBytes(computedState.AppContext); err != nil {
		c.Logger.Panicf("Failed deserializing app context to BlockHeader from state: %v", err)
	}

	lastBlockNumber := lastBlockHeader.Number
	prevHash := lastBlockHeader.Hash()
	if lastBlockNumber == math.MaxUint64 { // This is a signal that we bootstrap
		prevHash = nil
	}

	for i, ba := range attestations {
		var hdr state.BlockHeader
		hdr.Digest = ba[0].Digest()
		lastBlockNumber++
		hdr.Number = lastBlockNumber
		hdr.PrevHash = prevHash
		prevHash = hdr.Hash()
		availableBlocks[i].Header = &hdr
	}

	for i, availableBlock := range hdr.AvailableBlocks {
		if !availableBlock.Header.Equal(availableBlocks[i].Header) {
			return nil, fmt.Errorf("proposed block header %+v in index %d isn't equal to computed block header %+v",
				availableBlock.Header, i, availableBlocks[i].Header)
		}
	}

	if len(attestations) > 0 {
		computedState.AppContext = availableBlocks[len(attestations)-1].Header.Bytes()
	}

	if !bytes.Equal(hdr.State.Serialize(), computedState.Serialize()) {
		return nil, fmt.Errorf("proposed state %x isn't equal to computed state %x", hdr.State, computedState)
	}

	reqInfos := make([]types.RequestInfo, 0, len(batch))
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
func (c *Consensus) VerifyRequest(req []byte) (types.RequestInfo, error) {
	var ce core.ControlEvent
	bafd := &state.BAFDeserializer{}
	if err := ce.FromBytes(req, bafd.Deserialize); err != nil {
		return types.RequestInfo{}, err
	}

	reqID := c.RequestID(req)

	if ce.Complaint != nil {
		return reqID, c.SigVerifier.VerifySignature(ce.Complaint.Signer, ce.Complaint.Shard, toBeSignedComplaint(ce.Complaint), ce.Complaint.Signature)
	} else if ce.BAF != nil {
		return reqID, c.SigVerifier.VerifySignature(ce.BAF.Signer(), ce.BAF.Shard(), toBeSignedBAF(ce.BAF), ce.BAF.Signature())
	} else {
		return types.RequestInfo{}, fmt.Errorf("empty control event")
	}
}

// VerifyConsenterSig verifies the signature for the given proposal
// It returns the auxiliary data in the signature
// (from SmartBFT API)
func (c *Consensus) VerifyConsenterSig(signature types.Signature, prop types.Proposal) ([]byte, error) {
	var values [][]byte
	if _, err := asn1.Unmarshal(signature.Value, &values); err != nil {
		return nil, err
	}

	if err := c.VerifySignature(types.Signature{
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

	for i, bh := range hdr.AvailableBlocks {
		if err := c.VerifySignature(types.Signature{
			Value: values[i+1],
			Msg:   bh.Header.Bytes(),
			ID:    signature.ID,
		}); err != nil {
			return nil, errors.Wrap(err, "failed verifying signature over block header")
		}
	}

	return nil, nil
}

// VerifySignature verifies the signature
// (from SmartBFT API)
func (c *Consensus) VerifySignature(signature types.Signature) error {
	return c.SigVerifier.VerifySignature(arma_types.PartyID(signature.ID), arma_types.ShardID(math.MaxUint16), signature.Msg, signature.Value)
}

// VerificationSequence returns the current verification sequence
// (from SmartBFT API)
func (c *Consensus) VerificationSequence() uint64 {
	return 0
}

// RequestsFromProposal returns from the given proposal the included requests' info
// (from SmartBFT API)
func (c *Consensus) RequestsFromProposal(proposal types.Proposal) []types.RequestInfo {
	var batch arma_types.BatchedRequests
	if err := batch.Deserialize(proposal.Payload); err != nil {
		panic("failed deserializing proposal payload")
	}
	reqInfos := make([]types.RequestInfo, 0, len(batch))
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
func (c *Consensus) RequestID(req []byte) types.RequestInfo {
	var ce core.ControlEvent
	bafd := &state.BAFDeserializer{}
	if err := ce.FromBytes(req, bafd.Deserialize); err != nil {
		return types.RequestInfo{}
	}

	if ce.Complaint == nil && ce.BAF == nil {
		c.Logger.Warnf("Empty control event")
		return types.RequestInfo{}
	}

	return types.RequestInfo{
		ID:       ce.ID(),
		ClientID: ce.SignerID(),
	}
}

// SignProposal signs on the given proposal and returns a composite Signature
// (from SmartBFT API)
func (c *Consensus) SignProposal(proposal types.Proposal, _ []byte) *types.Signature {
	var requests arma_types.BatchedRequests
	if err := requests.Deserialize(proposal.Payload); err != nil {
		c.Logger.Panicf("Failed deserializing proposal payload: %v", err)
	}

	var hdr state.Header
	if err := hdr.Deserialize(proposal.Header); err != nil {
		c.Logger.Panicf("Failed deserializing proposal header: %v", err)
	}

	c.stateLock.Lock()
	_, bafs := c.Arma.SimulateStateTransition(c.State, requests)
	c.stateLock.Unlock()

	sigs := make([][]byte, 0, len(bafs)+1)

	proposalSig, err := c.Signer.Sign([]byte(proposal.Digest()))
	if err != nil {
		c.Logger.Panicf("Failed signing proposal digest: %v", err)
	}

	sigs = append(sigs, proposalSig)

	for _, bh := range hdr.AvailableBlocks {
		msg := bh.Header.Bytes()
		sig, err := c.Signer.Sign(msg)
		if err != nil {
			c.Logger.Panicf("Failed signing block header: %v", err)
		}

		sigs = append(sigs, sig)
	}

	sigsRaw, err := asn1.Marshal(sigs)
	if err != nil {
		c.Logger.Panicf("Failed marshaling signatures: %v", err)
	}

	return &types.Signature{
		// the Msg is defined by VerifyConsenterSig
		Value: sigsRaw,
		ID:    c.BFTConfig.SelfID,
	}
}

// AssembleProposal creates a proposal which includes the given requests (when permitting) and metadata
// (from SmartBFT API)
func (c *Consensus) AssembleProposal(metadata []byte, requests [][]byte) types.Proposal {
	c.stateLock.Lock()
	newState, attestations := c.Arma.SimulateStateTransition(c.State, requests)
	c.stateLock.Unlock()

	var lastBlockHeader state.BlockHeader
	if err := lastBlockHeader.FromBytes(newState.AppContext); err != nil {
		c.Logger.Panicf("Failed deserializing app context to BlockHeader from state: %v", err)
	}

	lastBlockNumber := lastBlockHeader.Number
	prevHash := lastBlockHeader.Hash()

	c.Logger.Infof("Creating proposal with %d attestations", len(attestations))

	availableBlocks := make([]state.AvailableBlock, len(attestations))

	for i, ba := range attestations {
		availableBlocks[i].Batch = state.NewAvailableBatch(ba[0].Primary(), ba[0].Shard(), ba[0].Seq(), ba[0].Digest())
	}

	for i, ba := range attestations {
		var hdr state.BlockHeader
		hdr.Digest = ba[0].Digest()
		lastBlockNumber++
		hdr.Number = lastBlockNumber
		hdr.PrevHash = prevHash
		prevHash = hdr.Hash()
		availableBlocks[i].Header = &hdr
	}

	if len(attestations) > 0 {
		newState.AppContext = availableBlocks[len(attestations)-1].Header.Bytes()
	}

	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(metadata, md); err != nil {
		panic(err)
	}

	reqs := arma_types.BatchedRequests(requests)

	return types.Proposal{
		Header: (&state.Header{
			AvailableBlocks: availableBlocks,
			State:           newState,
			Num:             arma_types.DecisionNum(md.LatestSequence),
		}).Serialize(),
		Metadata: metadata,
		Payload:  reqs.Serialize(),
	}
}

// Deliver delivers the given proposal and signatures.
// After the call returns we assume that this proposal is stored in persistent memory.
// It returns whether this proposal was a reconfiguration and the current config.
// (from SmartBFT API)
func (c *Consensus) Deliver(proposal types.Proposal, signatures []types.Signature) types.Reconfig {
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

	c.stateLock.Lock()
	c.State = hdr.State
	c.stateLock.Unlock()

	return types.Reconfig{
		CurrentNodes:  c.CurrentNodes,
		CurrentConfig: c.BFTConfig,
	}
}
