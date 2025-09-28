/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

//go:generate counterfeiter -o mocks/state_replicator.go . StateReplicator
type StateReplicator interface {
	ReplicateState() <-chan *state.State
	Stop()
}

// Signer signs messages
type Signer interface {
	Sign([]byte) ([]byte, error)
}

type Net interface {
	Stop()
}

type Batcher struct {
	requestsInspectorVerifier *RequestsInspectorVerifier
	batcherDeliverService     *BatcherDeliverService
	stateReplicator           StateReplicator
	logger                    types.Logger
	batcher                   *BatcherRole
	batcherCerts2IDs          map[string]types.PartyID
	controlEventSenders       []ConsenterControlEventSender
	controlEventBroadcaster   *ControlEventBroadcaster
	primaryAckConnector       *PrimaryAckConnector
	primaryReqConnector       *PrimaryReqConnector
	Net                       Net
	Ledger                    *node_ledger.BatchLedgerArray
	config                    *node_config.BatcherNodeConfig
	batchers                  []node_config.BatcherInfo
	signer                    Signer

	stateChan chan *state.State

	running  sync.WaitGroup
	stopOnce sync.Once
	stopChan chan struct{}

	primaryLock sync.RWMutex
	term        uint64
	primaryID   types.PartyID
}

func (b *Batcher) Run() {
	b.stopChan = make(chan struct{})

	b.stateChan = make(chan *state.State, 1)

	go b.replicateState()

	b.logger.Infof("Starting batcher")
	b.batcher.Start()
}

func (b *Batcher) Stop() {
	b.logger.Infof("Stopping batcher node")
	b.stopOnce.Do(func() { close(b.stopChan) })
	b.controlEventBroadcaster.Stop()
	b.batcher.Stop()
	for len(b.stateChan) > 0 {
		<-b.stateChan // drain state channel
	}
	b.Net.Stop()
	b.primaryAckConnector.Stop()
	b.primaryReqConnector.Stop()
	b.Ledger.Close()
	b.running.Wait()
}

// replicateState runs by a separate go routine
func (b *Batcher) replicateState() {
	b.logger.Infof("Started replicating state")
	b.running.Add(1)
	defer func() {
		b.stateReplicator.Stop()
		b.running.Done()
		b.logger.Infof("Stopped replicating state")
	}()
	stateChan := b.stateReplicator.ReplicateState()
	for {
		select {
		case state := <-stateChan:
			b.stateChan <- state
			primaryID, term := b.getPrimaryIDAndTerm(state)
			changed := false
			b.primaryLock.Lock()
			if b.primaryID != primaryID {
				b.logger.Infof("Primary changed from %d to %d", b.primaryID, primaryID)
				b.primaryID = primaryID
				b.term = term
				changed = true
			}
			b.primaryLock.Unlock()
			if changed {
				b.primaryAckConnector.ConnectToNewPrimary(primaryID)
				b.primaryReqConnector.ConnectToNewPrimary(primaryID)
			}
		case <-b.stopChan:
			return
		}
	}
}

func (b *Batcher) GetLatestStateChan() <-chan *state.State {
	return b.stateChan
}

func (b *Batcher) Broadcast(_ orderer.AtomicBroadcast_BroadcastServer) error {
	return fmt.Errorf("not implemented")
}

func (b *Batcher) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	return b.batcherDeliverService.Deliver(stream)
}

func (b *Batcher) Submit(ctx context.Context, req *protos.Request) (*protos.SubmitResponse, error) {
	traceId := req.TraceId
	req.TraceId = nil

	rawReq, err := proto.Marshal(req)
	if err != nil {
		b.logger.Panicf("Failed marshaling request: %v", err)
	}

	b.logger.Infof("Received request %x", req.Payload)

	var resp protos.SubmitResponse
	resp.TraceId = traceId
	if err := b.requestsInspectorVerifier.VerifyRequest(rawReq); err != nil {
		// TODO make sure the router verifies the request
		b.logger.Panicf("Failed verifying request before submitting from router; err: %v", err)
	}
	if err := b.batcher.Submit(rawReq); err != nil {
		resp.Error = err.Error()
	}

	return &resp, nil
}

func (b *Batcher) SubmitStream(stream protos.RequestTransmit_SubmitStreamServer) error {
	stop := make(chan struct{})
	defer close(stop)

	defer func() {
		b.logger.Infof("Client disconnected")
	}()

	responses := make(chan *protos.SubmitResponse, 1000)

	go b.sendResponses(stream, responses, stop)

	return b.dispatchRequests(stream, responses)
}

func (b *Batcher) dispatchRequests(stream protos.RequestTransmit_SubmitStreamServer, responses chan *protos.SubmitResponse) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		traceId := req.TraceId
		req.TraceId = nil

		rawReq, err := proto.Marshal(req)
		if err != nil {
			b.logger.Panicf("Failed marshaling request: %v", err)
		}

		var resp protos.SubmitResponse
		resp.TraceId = traceId

		if err := b.requestsInspectorVerifier.VerifyRequest(rawReq); err != nil {
			// TODO make sure the router verifies the request
			b.logger.Panicf("Failed verifying request before submitting from router; err: %v", err)
		}
		if err := b.batcher.Submit(rawReq); err != nil {
			resp.Error = err.Error()
		}

		if len(traceId) > 0 {
			responses <- &resp
		}

		b.logger.Debugf("Submitted request %x", traceId)

	}
}

func (b *Batcher) sendResponses(stream protos.RequestTransmit_SubmitStreamServer, responses chan *protos.SubmitResponse, stop chan struct{}) {
	for {
		select {
		case resp := <-responses:
			b.logger.Debugf("Sending response %x", resp.TraceId)
			stream.Send(resp)
			b.logger.Debugf("Sent response %x", resp.TraceId)
		case <-stop:
			b.logger.Debugf("Stopped sending responses")
			return
		}
	}
}

func (b *Batcher) extractBatcherFromContext(c context.Context) (types.PartyID, error) {
	cert := node.ExtractCertificateFromContext(c)
	if cert == nil {
		return 0, errors.New("access denied; could not extract certificate from context")
	}

	from, exists := b.batcherCerts2IDs[string(cert.Raw)]
	if !exists {
		return 0, errors.Errorf("access denied; unknown certificate; %s", node.CertificateToString(cert))
	}

	return from, nil
}

func (b *Batcher) FwdRequestStream(stream protos.BatcherControlService_FwdRequestStreamServer) error {
	from, err := b.extractBatcherFromContext(stream.Context())
	if err != nil {
		return errors.Errorf("Could not extract batcher from context; err %v", err)
	}
	b.logger.Infof("Starting to handle fwd requests from batcher %d", from)

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		b.logger.Debugf("Calling submit request from batcher %d", from)
		if err := b.requestsInspectorVerifier.VerifyRequest(msg.Request); err != nil {
			b.logger.Infof("Failed verifying request before submitting from batcher %d; err: %v", from, err)
		}
		if err := b.batcher.Submit(msg.Request); err != nil {
			if strings.Contains(err.Error(), "already inserted") {
				b.logger.Debugf("Failed to submit request from batcher %d; err: %v", from, err)
				continue
			}
			b.logger.Infof("Failed to submit request from batcher %d; err: %v", from, err)
		}
	}
}

func (b *Batcher) NotifyAck(stream protos.BatcherControlService_NotifyAckServer) error {
	from, err := b.extractBatcherFromContext(stream.Context())
	if err != nil {
		return errors.Errorf("Could not extract batcher from context; err %v", err)
	}

	b.logger.Infof("Starting to handle acks from batcher %d", from)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		b.logger.Debugf("Calling handle ack with seq %d from batcher %d", msg.Seq, from)
		b.batcher.HandleAck(types.BatchSequence(msg.Seq), from)
	}
}

func (b *Batcher) OnFirstStrikeTimeout(req []byte) {
	b.logger.Debugf("First strike timeout occurred on request %s", b.requestsInspectorVerifier.RequestID(req))
	b.sendReq(req)
}

func (b *Batcher) OnSecondStrikeTimeout() {
	b.logger.Warnf("Second strike timeout occurred; sending a complaint")
	b.Complain(fmt.Sprintf("batcher %d (shard %d) complaining; second strike timeout occurred", b.config.PartyId, b.config.ShardId))
}

func (b *Batcher) CreateBAF(seq types.BatchSequence, primary types.PartyID, shard types.ShardID, digest []byte) types.BatchAttestationFragment {
	baf, err := CreateBAF(b.signer, b.config.PartyId, shard, digest, primary, seq)
	if err != nil {
		b.logger.Panicf("Failed creating batch attestation fragment: %v", err)
	}

	return baf
}

func (b *Batcher) GetTerm() uint64 {
	b.primaryLock.RLock()
	defer b.primaryLock.RUnlock()
	return b.term
}

func (b *Batcher) GetPrimaryID() types.PartyID {
	b.primaryLock.RLock()
	defer b.primaryLock.RUnlock()
	return b.primaryID
}

func (b *Batcher) getPrimaryIDAndTerm(state *state.State) (types.PartyID, uint64) {
	term := uint64(math.MaxUint64)
	for _, shard := range state.Shards {
		if shard.Shard == b.config.ShardId {
			term = shard.Term
		}
	}

	if term == math.MaxUint64 {
		b.logger.Panicf("Could not find our shard (%d) within the shards: %v", b.config.ShardId, state.Shards)
	}

	primaryIndex := types.PartyID((uint64(b.config.ShardId) + term) % uint64(state.N))

	primaryID := b.batchers[primaryIndex].PartyID

	return primaryID, term
}

func (b *Batcher) createComplaint(reason string) *state.Complaint {
	term := b.GetTerm()
	c, err := CreateComplaint(b.signer, b.config.PartyId, b.config.ShardId, term, reason)
	if err != nil {
		b.logger.Panicf("Failed creating complaint: %v", err)
	}
	b.logger.Infof("Created complaint with term %d and reason %s", term, reason)
	return c
}

func (b *Batcher) sendReq(req []byte) {
	t1 := time.Now()

	defer func() {
		b.logger.Debugf("Sending req took %v", time.Since(t1))
	}()

	b.primaryReqConnector.SendReq(req)
}

func (b *Batcher) Ack(seq types.BatchSequence, to types.PartyID) {
	t1 := time.Now()

	defer func() {
		b.logger.Debugf("Sending ack took %v", time.Since(t1))
	}()

	primaryID := b.GetPrimaryID()
	if to != primaryID {
		b.logger.Warnf("Trying to send ack to %d while primary is %d", to, primaryID)
		return
	}

	b.primaryAckConnector.SendAck(seq)
}

func (b *Batcher) Complain(reason string) {
	if err := b.controlEventBroadcaster.BroadcastControlEvent(state.ControlEvent{Complaint: b.createComplaint(reason)}); err != nil {
		b.logger.Errorf("Failed to broadcast complaint; err: %v", err)
	}
}

func (b *Batcher) SendBAF(baf types.BatchAttestationFragment) {
	b.logger.Infof("Sending batch attestation fragment for seq %d with digest %x", baf.Seq(), baf.Digest())
	if err := b.controlEventBroadcaster.BroadcastControlEvent(state.ControlEvent{BAF: baf}); err != nil {
		b.logger.Errorf("Failed to broadcast batch attestation fragment; err: %v", err)
	}
}

func CreateComplaint(signer Signer, id types.PartyID, shard types.ShardID, term uint64, reason string) (*state.Complaint, error) {
	c := &state.Complaint{
		ShardTerm: state.ShardTerm{Shard: shard, Term: term},
		Signer:    id,
		Signature: nil,
		Reason:    reason,
	}
	sig, err := signer.Sign(c.ToBeSigned())
	if err != nil {
		return nil, err
	}
	c.Signature = sig

	return c, nil
}

func CreateBAF(signer Signer, id types.PartyID, shard types.ShardID, digest []byte, primary types.PartyID, seq types.BatchSequence) (types.BatchAttestationFragment, error) {
	baf := types.NewSimpleBatchAttestationFragment(shard, primary, seq, digest, id, 0)
	sig, err := signer.Sign(baf.ToBeSigned())
	if err != nil {
		return nil, err
	}
	baf.SetSignature(sig)

	return baf, nil
}
