/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"context"
	"encoding/asn1"
	"strconv"
	"sync"

	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Signer interface {
	Sign(message []byte) ([]byte, error)
}

// AuthCommMgr implements the Communicator
// It manages the client side connections and streams established with
// the Cluster GRPC server and new Cluster service
type AuthCommMgr struct {
	Logger Logger

	Lock           sync.RWMutex
	shutdown       bool
	shutdownSignal chan struct{}

	Mapping     MemberMapping
	Connections *ConnectionsMgr

	SendBufferSize int
	NodeIdentity   []byte
	Signer         Signer
}

func (ac *AuthCommMgr) Remote(id uint64) (*RemoteContext, error) {
	ac.Lock.RLock()
	defer ac.Lock.RUnlock()

	if ac.shutdown {
		return nil, errors.New("communication has been shut down")
	}

	mapping := ac.Mapping
	stub := mapping.ByID(id)
	if stub == nil {
		return nil, errors.Errorf("node %d doesn't exist in channel ", id)
	}

	if stub.Active() {
		return stub.RemoteContext, nil
	}

	err := stub.Activate(ac.createRemoteContext(stub))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return stub.RemoteContext, nil
}

func (ac *AuthCommMgr) Configure(members []RemoteNode) {
	ac.Logger.Infof("Configuring communication module with nodes:%v", members)

	ac.Lock.Lock()
	defer ac.Lock.Unlock()

	if ac.shutdown {
		return
	}

	if ac.shutdownSignal == nil {
		ac.shutdownSignal = make(chan struct{})
	}

	mapping := ac.getOrCreateMapping()
	newNodeIDs := make(map[uint64]struct{})

	for _, node := range members {
		newNodeIDs[node.ID] = struct{}{}
		ac.updateStubInMapping(mapping, node)
	}

	// Remove all stubs without a corresponding node
	// in the new nodes
	mapping.Foreach(func(id uint64, stub *Stub) {
		if _, exists := newNodeIDs[id]; exists {
			ac.Logger.Infof("Node with ID %v exists in new membership", id)
			return
		}
		ac.Logger.Infof("Deactivated node %v who's endpoint is %v", id, stub.Endpoint)
		mapping.Remove(id)
		stub.Deactivate()
		ac.Connections.Disconnect(stub.Endpoint)
	})
}

func (ac *AuthCommMgr) Shutdown() {
	ac.Lock.Lock()
	defer ac.Lock.Unlock()

	if !ac.shutdown && ac.shutdownSignal != nil {
		close(ac.shutdownSignal)
	}

	ac.shutdown = true
	ac.Mapping.Foreach(func(id uint64, stub *Stub) {
		ac.Connections.Disconnect(stub.endpoint)
	})
}

// getOrCreateMapping creates a MemberMapping for the given channel
// or returns the existing one.
func (ac *AuthCommMgr) getOrCreateMapping() MemberMapping {
	// Lazily create a mapping if it doesn't already exist
	if ac.Mapping.id2stub == nil {
		ac.Mapping = MemberMapping{
			id2stub: make(map[uint64]*Stub),
		}
	}

	return ac.Mapping
}

// updateStubInMapping updates the given RemoteNode and adds it to the MemberMapping
func (ac *AuthCommMgr) updateStubInMapping(mapping MemberMapping, node RemoteNode) {
	stub := mapping.ByID(node.ID)
	if stub == nil {
		ac.Logger.Infof("Allocating a new stub for node %v with endpoint %v", node.ID, node.Endpoint)
		stub = &Stub{}
	}

	// Overwrite the stub Node data with the new data
	stub.RemoteNode = node

	// Put the stub into the mapping
	mapping.Put(stub)

	// Check if the stub needs activation.
	if stub.Active() {
		return
	}

	// Activate the stub
	stub.Activate(ac.createRemoteContext(stub))
}

func (ac *AuthCommMgr) createRemoteContext(stub *Stub) func() (*RemoteContext, error) {
	return func() (*RemoteContext, error) {
		ac.Logger.Debugf("Connecting to node: %v", stub.RemoteNode.NodeAddress)

		conn, err := ac.Connections.Connect(stub.Endpoint, stub.RemoteNode.ServerRootCA)
		if err != nil {
			ac.Logger.Warnf("Unable to obtain connection to %d(%s): %v", stub.ID, stub.Endpoint, err)
			return nil, err
		}

		probeConnection := func(conn *grpc.ClientConn) error {
			connState := conn.GetState()
			if connState == connectivity.Connecting {
				return errors.Errorf("connection to %d(%s) is in state %s", stub.ID, stub.Endpoint, connState)
			}
			return nil
		}

		clusterClient := orderer.NewClusterNodeServiceClient(conn)
		getStepClientStream := func(ctx context.Context) (StepClientStream, error) {
			stream, err := clusterClient.Step(ctx)
			if err != nil {
				return nil, err
			}

			membersMapping := ac.Mapping

			nodeStub := membersMapping.LookupByIdentity(ac.NodeIdentity)

			if nodeStub == nil {
				return nil, errors.Errorf("node identity %v is missing in channel", ac.NodeIdentity)
			}

			stepClientStream := &NodeClientStream{
				Identity:          ac.NodeIdentity,
				Version:           0,
				StepClient:        stream,
				SourceNodeID:      nodeStub.ID,
				DestinationNodeID: stub.ID,
				Signer:            ac.Signer,
			}
			return stepClientStream, nil
		}

		rc := &RemoteContext{
			SendBuffSize:   ac.SendBufferSize,
			endpoint:       stub.Endpoint,
			Logger:         ac.Logger,
			ProbeConn:      probeConnection,
			conn:           conn,
			GetStreamFunc:  getStepClientStream,
			shutdownSignal: ac.shutdownSignal,
		}
		return rc, nil
	}
}

type NodeClientStream struct {
	Identity          []byte
	StepClient        orderer.ClusterNodeService_StepClient
	Version           uint32
	SourceNodeID      uint64
	DestinationNodeID uint64
	Signer            Signer
}

func (cs *NodeClientStream) Send(request *orderer.StepRequest) error {
	stepRequest, cerr := BuildStepRequest(request)
	if cerr != nil {
		return cerr
	}
	return cs.StepClient.Send(stepRequest)
}

func (cs *NodeClientStream) Recv() (*orderer.StepResponse, error) {
	nodeResponse, err := cs.StepClient.Recv()
	if err != nil {
		return nil, err
	}
	return BuildStepRespone(nodeResponse)
}

func (cs *NodeClientStream) Auth() error {
	if cs.Signer == nil {
		return errors.New("signer is nil")
	}

	payload := &orderer.NodeAuthRequest{
		Version:   cs.Version,
		Timestamp: timestamppb.Now(),
		FromId:    cs.SourceNodeID,
		ToId:      cs.DestinationNodeID,
	}

	bindingFieldsHash := GetSessionBindingHash(payload)

	tlsBinding, err := GetTLSSessionBinding(cs.StepClient.Context(), bindingFieldsHash)
	if err != nil {
		return errors.Wrap(err, "TLSBinding failed")
	}
	payload.SessionBinding = tlsBinding

	asnSignFields, err := asn1.Marshal(AuthRequestSignature{
		Version:        int64(payload.Version),
		Timestamp:      EncodeTimestamp(payload.Timestamp),
		FromId:         strconv.FormatUint(payload.FromId, 10),
		ToId:           strconv.FormatUint(payload.ToId, 10),
		SessionBinding: payload.SessionBinding,
	})
	if err != nil {
		panic(err)
	}

	sig, err := cs.Signer.Sign(asnSignFields)
	if err != nil {
		return errors.Wrap(err, "signing failed")
	}

	payload.Signature = sig
	stepRequest := &orderer.ClusterNodeServiceStepRequest{
		Payload: &orderer.ClusterNodeServiceStepRequest_NodeAuthrequest{
			NodeAuthrequest: payload,
		},
	}

	return cs.StepClient.Send(stepRequest)
}

func (cs *NodeClientStream) Context() context.Context {
	return cs.StepClient.Context()
}

func BuildStepRequest(request *orderer.StepRequest) (*orderer.ClusterNodeServiceStepRequest, error) {
	if request == nil {
		return nil, errors.New("request is nil")
	}
	var stepRequest *orderer.ClusterNodeServiceStepRequest
	if consReq := request.GetConsensusRequest(); consReq != nil {
		stepRequest = &orderer.ClusterNodeServiceStepRequest{
			Payload: &orderer.ClusterNodeServiceStepRequest_NodeConrequest{
				NodeConrequest: &orderer.NodeConsensusRequest{
					Payload:  consReq.Payload,
					Metadata: consReq.Metadata,
				},
			},
		}
		return stepRequest, nil
	} else if subReq := request.GetSubmitRequest(); subReq != nil {
		stepRequest = &orderer.ClusterNodeServiceStepRequest{
			Payload: &orderer.ClusterNodeServiceStepRequest_NodeTranrequest{
				NodeTranrequest: &orderer.NodeTransactionOrderRequest{
					Payload:           subReq.Payload,
					LastValidationSeq: subReq.LastValidationSeq,
				},
			},
		}
		return stepRequest, nil
	}
	return nil, errors.New("service message type not valid")
}

func BuildStepRespone(stepResponse *orderer.ClusterNodeServiceStepResponse) (*orderer.StepResponse, error) {
	if stepResponse == nil {
		return nil, errors.New("input response object is nil")
	}
	if respPayload := stepResponse.GetTranorderRes(); respPayload != nil {
		stepResponse := &orderer.StepResponse{
			Payload: &orderer.StepResponse_SubmitRes{
				SubmitRes: &orderer.SubmitResponse{
					Channel: respPayload.Channel,
					Status:  respPayload.Status,
				},
			},
		}
		return stepResponse, nil
	}
	return nil, errors.New("service stream returned with invalid response type")
}
