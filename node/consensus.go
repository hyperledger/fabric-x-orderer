package node

import (
	arma "arma/pkg"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/SmartBFT-Go/consensus/v2/pkg/types"
	"github.com/SmartBFT-Go/consensus/v2/smartbftprotos"
	"github.com/golang/protobuf/proto"
	protos "github.ibm.com/Yacov-Manevich/ARMA/node/protos/comm"
	"sync"
)

type Storage interface {
	Append([]byte)
}

type Signer interface {
	Sign(message []byte) ([]byte, error)
}

type SigVerifier interface {
	VerifySignature(id arma.PartyID, msg, sig []byte) error
}

type Synchronizer interface {
	Sync() []byte
}

type Arma interface {
	SimulateStateTransition(prevState []byte, events [][]byte) ([]byte, [][]arma.BatchAttestationFragment)
	Commit(events [][]byte)
}

type BFT interface {
	SubmitRequest(req []byte) error
	Start() error
	HandleMessage(targetID uint64, m *smartbftprotos.Message)
	HandleRequest(targetID uint64, request []byte)
}

type Consensus struct {
	PrevHash      []byte
	LastSeq       uint64
	SigVerifier   SigVerifier
	Signer        Signer
	CurrentNodes  []uint64
	CurrentConfig types.Configuration
	BFT           BFT
	Storage       Storage
	Arma          Arma
	lock          sync.RWMutex
	State         []byte
	Synchronizer  Synchronizer
	Logger        arma.Logger
}

func (c *Consensus) NotifyEvent(ctx context.Context, event *protos.Event) (*protos.EventResponse, error) {
	var ce arma.ControlEvent
	if err := ce.FromBytes(event.GetPayload(), BatchAttestationFromBytes); err != nil {
		return &protos.EventResponse{Error: fmt.Sprintf("malformed control event: %v", err)}, nil
	}

	c.Logger.Infof("Received event %x", event.Payload)

	if err := c.BFT.SubmitRequest(ce.Bytes()); err != nil {
		c.Logger.Errorf("Failed submitting request: %v", err)
		return &protos.EventResponse{Error: fmt.Sprintf("failed submitting request: %v", err)}, nil
	}

	return &protos.EventResponse{}, nil
}

func (c *Consensus) SubmitRequest(req []byte) error {
	if _, err := c.VerifyRequest(req); err != nil {
		c.Logger.Warnf("Received bad request: %v", err)
		return err
	}

	return c.BFT.SubmitRequest(req)
}

func (c *Consensus) VerifyProposal(proposal types.Proposal) ([]types.RequestInfo, error) {
	batch := arma.BatchFromRaw(proposal.Payload)
	var hdr Header
	if err := hdr.FromBytes(proposal.Header); err != nil {
		return nil, err
	}

	computedState, _ := c.Arma.SimulateStateTransition(c.State, batch)
	if !bytes.Equal(hdr.State, computedState) {
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

func (c *Consensus) VerifyRequest(req []byte) (types.RequestInfo, error) {
	var ce arma.ControlEvent
	if err := ce.FromBytes(req, BatchAttestationFromBytes); err != nil {
		return types.RequestInfo{}, err
	}

	reqID := c.RequestID(req)

	if ce.Complaint != nil {
		ce.Complaint.Bytes()
		err := c.SigVerifier.VerifySignature(ce.Complaint.Signer, ToBeSignedComplaint(ce.Complaint), ce.Complaint.Signature)
		return reqID, err
	} else if ce.BAF != nil {
		err := c.SigVerifier.VerifySignature(ce.BAF.Signer(), ToBeSignedBAF(ce.BAF), ce.BAF.(*arma.SimpleBatchAttestationFragment).Sig)
		return reqID, err
	} else {
		return types.RequestInfo{}, fmt.Errorf("empty Control Event")
	}
}

func ToBeSignedComplaint(c *arma.Complaint) []byte {
	buff := make([]byte, 12)
	var pos int
	binary.BigEndian.PutUint16(buff, uint16(c.Shard))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], c.Term)
	pos += 8
	binary.BigEndian.PutUint16(buff[pos:], uint16(c.Signer))

	return buff
}

func ToBeSignedBAF(baf arma.BatchAttestationFragment) []byte {
	buff := make([]byte, 18)
	var pos int
	binary.BigEndian.PutUint16(buff, uint16(baf.Shard()))
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], baf.Seq())
	pos += 8
	binary.BigEndian.PutUint16(buff[pos:], uint16(baf.Signer()))
	pos += 2
	copy(buff[pos:], baf.Digest())

	return buff
}

func (c *Consensus) VerifyConsenterSig(signature types.Signature, prop types.Proposal) ([]byte, error) {
	var msgs Bytes
	if _, err := asn1.Unmarshal(signature.Msg, &msgs); err != nil {
		return nil, err
	}

	var values Bytes
	if _, err := asn1.Unmarshal(signature.Value, &values); err != nil {
		return nil, err
	}

	if err := c.VerifySignature(types.Signature{
		Value: values[len(values)-1],
		Msg:   []byte(prop.Digest()),
		ID:    signature.ID,
	}); err != nil {
		return nil, err
	}

	for i, msg := range msgs {
		if err := c.VerifySignature(types.Signature{
			Value: values[i],
			Msg:   msg,
			ID:    signature.ID,
		}); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (c *Consensus) VerifySignature(signature types.Signature) error {
	return c.SigVerifier.VerifySignature(arma.PartyID(signature.ID), signature.Msg, signature.Value)
}

func (c *Consensus) VerificationSequence() uint64 {
	return 0
}

func (c *Consensus) RequestsFromProposal(proposal types.Proposal) []types.RequestInfo {
	batch := arma.BatchFromRaw(proposal.Payload)
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

func (c *Consensus) AuxiliaryData(i []byte) []byte {
	return nil
}

type toBeSignedBlockHeader struct {
	Sequence int64
	Digest   []byte
	PrevHash []byte
}

func (tbsbh toBeSignedBlockHeader) Bytes() []byte {
	buff := make([]byte, 8+64)
	binary.BigEndian.PutUint64(buff, uint64(tbsbh.Sequence))
	copy(buff[8:], tbsbh.Digest)
	copy(buff[40:], tbsbh.PrevHash)
	return buff
}

type AvailableBatch struct {
	primary uint16
	shard   uint16
	seq     uint64
	digest  []byte
}

func (ab *AvailableBatch) Fragments() []arma.BatchAttestationFragment {
	panic("should not be called")
}

func (ab *AvailableBatch) Digest() []byte {
	return ab.digest
}

func (ab *AvailableBatch) Seq() uint64 {
	return ab.seq
}

func (ab *AvailableBatch) Primary() arma.PartyID {
	return arma.PartyID(ab.primary)
}

func (ab *AvailableBatch) Shard() arma.ShardID {
	return arma.ShardID(ab.shard)
}

func (ab *AvailableBatch) Serialize() []byte {
	buff := make([]byte, 32+2*2+8)
	var pos int
	binary.BigEndian.PutUint16(buff[pos:], ab.primary)
	pos += 2
	binary.BigEndian.PutUint16(buff[pos:], ab.shard)
	pos += 2
	binary.BigEndian.PutUint64(buff[pos:], ab.seq)
	pos += 8
	copy(buff[pos:], ab.digest)

	return buff
}

func (ab *AvailableBatch) Deserialize(bytes []byte) error {
	ab.primary = binary.BigEndian.Uint16(bytes[0:2])
	ab.shard = binary.BigEndian.Uint16(bytes[2:4])
	ab.seq = binary.BigEndian.Uint64(bytes[4:12])
	ab.digest = bytes[12:]

	return nil
}

type Header struct {
	Num              uint64
	AvailableBatches []AvailableBatch
	State            []byte
}

func (h *Header) FromBytes(rawHeader []byte) error {
	h.Num = binary.BigEndian.Uint64(rawHeader[0:8])
	availableBatchCount := int(binary.BigEndian.Uint16(rawHeader[8:10]))

	pos := 10

	h.AvailableBatches = nil
	for i := 0; i < availableBatchCount; i++ {
		abSize := 2 + 2 + 8 + 32
		var ab AvailableBatch
		ab.Deserialize(rawHeader[pos : pos+abSize])
		pos += abSize
		h.AvailableBatches = append(h.AvailableBatches, ab)
	}

	h.State = rawHeader[pos:]

	return nil
}

func (h *Header) Bytes() []byte {

	prefix := make([]byte, 8+2)
	binary.BigEndian.PutUint64(prefix, h.Num)
	binary.BigEndian.PutUint16(prefix[8:10], uint16(len(h.AvailableBatches)))

	availableBatchesBytes := availableBatchesToBytes(h.AvailableBatches)

	buff := make([]byte, len(availableBatchesBytes)+len(prefix)+len(h.State))
	copy(buff, prefix)
	copy(buff[len(prefix):], availableBatchesBytes)
	copy(buff[len(prefix)+len(availableBatchesBytes):], h.State)

	return buff
}

func availableBatchesToBytes(availableBatches []AvailableBatch) []byte {
	sequencesBuff := make([]byte, len(availableBatches)*(32+2*2+8))

	var pos int
	for _, ab := range availableBatches {
		bytes := ab.Serialize()
		copy(sequencesBuff[pos:], bytes)
		pos += len(bytes)
	}
	return sequencesBuff
}

type Bytes [][]byte

func (c *Consensus) Sign(msg []byte) []byte {
	sig, err := c.Signer.Sign(msg)
	if err != nil {
		panic(err)
	}

	return sig
}

func (c *Consensus) RequestID(req []byte) types.RequestInfo {
	var ce arma.ControlEvent
	if err := ce.FromBytes(req, BatchAttestationFromBytes); err != nil {
		return types.RequestInfo{}
	}

	var clientID string
	var payloadToHash []byte
	if ce.Complaint != nil {
		ce.Complaint.Signature = nil
		payloadToHash = ce.Complaint.Bytes()
		clientID = fmt.Sprintf("%d", ce.Complaint.Signer)
	} else if ce.BAF != nil {
		clientID = fmt.Sprintf("%d", ce.BAF.Signer())
		payloadToHash = make([]byte, 26)
		binary.BigEndian.PutUint64(payloadToHash, ce.BAF.Seq())
		binary.BigEndian.PutUint64(payloadToHash[8:], ce.BAF.Epoch())
		binary.BigEndian.PutUint16(payloadToHash[16:], uint16(ce.BAF.Signer()))
		binary.BigEndian.PutUint16(payloadToHash[18:], uint16(ce.BAF.Primary()))
		binary.BigEndian.PutUint16(payloadToHash[20:], uint16(ce.BAF.Shard()))
		copy(payloadToHash[22:], ce.BAF.Digest())
	} else {
		c.Logger.Warnf("Empty ControlEvent")
		return types.RequestInfo{}
	}

	dig := sha256.Sum256(payloadToHash)
	return types.RequestInfo{
		ID:       hex.EncodeToString(dig[:]),
		ClientID: clientID,
	}
}

func (c *Consensus) SignProposal(proposal types.Proposal, _ []byte) *types.Signature {
	requests := arma.BatchFromRaw(proposal.Payload)

	hdr := &Header{}
	if err := hdr.FromBytes(proposal.Header); err != nil {
		c.Logger.Panicf("Failed deserializing header: %v", err)
		return nil
	}

	c.lock.RLock()
	_, bafs := c.Arma.SimulateStateTransition(c.State, requests)
	c.lock.RUnlock()

	sigs := make(Bytes, 0, len(bafs)+1)
	msgs := make(Bytes, 0, len(bafs)+1)

	for _, ba := range bafs {
		var hdr toBeSignedBlockHeader
		hdr.Digest = ba[0].Digest()
		hdr.Sequence = int64(ba[0].Seq())
		hdr.PrevHash = c.PrevHash
		msg := hdr.Bytes()
		dig := sha256.Sum256(msg)
		c.PrevHash = dig[:]
		sig, err := c.Signer.Sign(msg)
		if err != nil {
			panic(err)
		}

		sigs = append(sigs, sig)
		msgs = append(msgs, msg)
	}

	proposalSig, err := c.Signer.Sign([]byte(proposal.Digest()))
	if err != nil {
		panic(err)
	}

	sigs = append(sigs, proposalSig)

	msgsRaw, err := asn1.Marshal(msgs)
	if err != nil {
		panic(err)
	}

	sigsRaw, err := asn1.Marshal(sigs)
	if err != nil {
		panic(err)
	}

	return &types.Signature{
		Msg:   msgsRaw,
		Value: sigsRaw,
		ID:    c.CurrentConfig.SelfID,
	}

}

func (c *Consensus) AssembleProposal(metadata []byte, requests [][]byte) types.Proposal {
	c.lock.RLock()
	newRawState, attestations := c.Arma.SimulateStateTransition(c.State, requests)
	c.lock.RUnlock()

	c.Logger.Infof("Created proposal with %d attestations", len(attestations))

	seq := c.LastSeq
	availableBatches := make([]AvailableBatch, 0, len(attestations))
	for _, ba := range attestations {
		availableBatches = append(availableBatches, AvailableBatch{
			digest:  ba[0].Digest(),
			shard:   uint16(ba[0].Shard()),
			seq:     ba[0].Seq(),
			primary: uint16(ba[0].Primary()),
		})

		seq++
	}

	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(metadata, md); err != nil {
		panic(err)
	}

	return types.Proposal{
		Header: (&Header{
			AvailableBatches: availableBatches,
			State:            newRawState,
			Num:              md.LatestSequence,
		}).Bytes(),
		Metadata: metadata,
		Payload:  arma.BatchedRequests(requests).ToBytes(),
	}
}

func BatchAttestationFromBytes(in []byte) (arma.BatchAttestationFragment, error) {
	var baf arma.SimpleBatchAttestationFragment
	if err := baf.Deserialize(in); err != nil {
		return nil, err
	}

	return &baf, nil
}

func (c *Consensus) Deliver(proposal types.Proposal, signatures []types.Signature) types.Reconfig {
	rawDecision := decisionToBytes(proposal, signatures)

	hdr := &Header{}
	if err := hdr.FromBytes(proposal.Header); err != nil {
		c.Logger.Panicf("Failed deserializing header: %v", err)
		return types.Reconfig{}
	}

	c.LastSeq += uint64(len(hdr.AvailableBatches))

	controlEvents := arma.BatchFromRaw(proposal.Payload)
	// Why do we first give Arma the events and then append the decision to storage?
	// Upon commit, Arma indexes the batch attestations which passed the threshold in its index,
	// to avoid signing them again in the (near) future.
	// If we crash after this, we will replicate the block and will overwrite the index again.
	// However, if we first commit the decision and then index afterwards and crash during or right before
	// we index, next time we spawn, we will not recognize we did not index and as a result we will may sign
	// a batch attestation twice.
	c.Arma.Commit(controlEvents)
	c.Storage.Append(rawDecision)

	c.lock.Lock()
	c.State = hdr.State
	c.lock.Unlock()

	return types.Reconfig{
		CurrentNodes:  c.CurrentNodes,
		CurrentConfig: c.CurrentConfig,
	}
}

func (c *Consensus) Sync() types.SyncResponse {

	resp := types.SyncResponse{
		Reconfig: types.ReconfigSync{
			CurrentConfig: c.CurrentConfig,
			CurrentNodes:  c.CurrentNodes,
		},
		Latest: types.Decision{},
	}

	latestRawDecision := c.Synchronizer.Sync()

	proposal, signatures, err := bytesToDecision(latestRawDecision)
	if err != nil {
		return resp
	}

	resp.Latest.Proposal = proposal
	resp.Latest.Signatures = signatures

	return resp
}

type Signature struct {
	ID    int64
	Value []byte
	Msg   []byte
}

func decisionToBytes(proposal types.Proposal, signatures []types.Signature) []byte {
	sigBuff := bytes.Buffer{}

	for _, sig := range signatures {
		rawSig, err := asn1.Marshal(Signature{Msg: sig.Msg, Value: sig.Value, ID: int64(sig.ID)})
		if err != nil {
			panic(err)
		}
		rawSigSize := make([]byte, 2)
		binary.BigEndian.PutUint16(rawSigSize, uint16(len(rawSig)))
		sigBuff.Write(rawSigSize)
		sigBuff.Write(rawSig)
	}

	buff := make([]byte, 4*3+len(proposal.Header)+len(proposal.Payload)+len(proposal.Metadata)+sigBuff.Len())
	binary.BigEndian.PutUint32(buff, uint32(len(proposal.Header)))
	binary.BigEndian.PutUint32(buff[4:], uint32(len(proposal.Payload)))
	binary.BigEndian.PutUint32(buff[8:], uint32(len(proposal.Metadata)))
	copy(buff[12:], proposal.Header)
	copy(buff[12+len(proposal.Header):], proposal.Payload)
	copy(buff[12+len(proposal.Header)+len(proposal.Payload):], proposal.Metadata)
	copy(buff[12+len(proposal.Header)+len(proposal.Payload)+len(proposal.Metadata):], sigBuff.Bytes())

	return buff
}

func bytesToDecision(rawBytes []byte) (types.Proposal, []types.Signature, error) {
	buff := bytes.NewBuffer(rawBytes)
	headerSize := make([]byte, 4)
	if _, err := buff.Read(headerSize); err != nil {
		return types.Proposal{}, nil, err
	}

	payloadSize := make([]byte, 4)
	if _, err := buff.Read(payloadSize); err != nil {
		return types.Proposal{}, nil, err
	}

	metadataSize := make([]byte, 4)
	if _, err := buff.Read(metadataSize); err != nil {
		return types.Proposal{}, nil, err
	}

	header := make([]byte, binary.BigEndian.Uint32(headerSize))
	if _, err := buff.Read(header); err != nil {
		return types.Proposal{}, nil, err
	}

	payload := make([]byte, binary.BigEndian.Uint32(payloadSize))
	if _, err := buff.Read(payload); err != nil {
		return types.Proposal{}, nil, err
	}

	metadata := make([]byte, binary.BigEndian.Uint32(metadataSize))
	if _, err := buff.Read(metadata); err != nil {
		return types.Proposal{}, nil, err
	}

	proposalSize := 4*3 + len(header) + len(payload) + len(metadata)

	signatureBuff := make([]byte, len(rawBytes)-proposalSize)

	if _, err := buff.Read(signatureBuff); err != nil {
		return types.Proposal{}, nil, err
	}

	var sigs []types.Signature

	var pos int
	for pos < len(signatureBuff) {
		sigSize := int(binary.BigEndian.Uint16([]byte{signatureBuff[pos], signatureBuff[pos+1]}))
		pos += 2
		sig := Signature{}
		if _, err := asn1.Unmarshal(signatureBuff[pos:pos+sigSize], &sig); err != nil {
			return types.Proposal{}, nil, err
		}
		pos += sigSize
		sigs = append(sigs, types.Signature{
			Msg:   sig.Msg,
			Value: sig.Value,
			ID:    uint64(sig.ID),
		})
	}

	return types.Proposal{
		Header:   header,
		Payload:  payload,
		Metadata: metadata,
	}, sigs, nil
}
