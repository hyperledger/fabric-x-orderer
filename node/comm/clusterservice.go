/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package comm

import (
	"bytes"
	"encoding/asn1"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric/common/util"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:generate mockery --dir . --name ClusterStepStream --case underscore --output ./mocks/

// ClusterStepStream defines the gRPC stream for sending
// transactions, and receiving corresponding responses
type ClusterStepStream interface {
	Send(response *orderer.ClusterNodeServiceStepResponse) error
	Recv() (*orderer.ClusterNodeServiceStepRequest, error)
	grpc.ServerStream
}

type MembersConfig struct {
	MemberMapping     map[uint64][]byte
	AuthorizedStreams sync.Map // Stream ID --> node identifier
	nextStreamID      uint64
}

// ClusterService implements the server API for ClusterNodeService service
type ClusterService struct {
	RequestHandler                   Handler
	Logger                           *flogging.FabricLogger
	StepLogger                       *flogging.FabricLogger
	MinimumExpirationWarningInterval time.Duration
	CertExpWarningThreshold          time.Duration
	Membership                       *MembersConfig
	Lock                             sync.RWMutex
	NodeIdentity                     []byte
}

type AuthRequestSignature struct {
	Version        int64
	Timestamp      []byte
	FromId         string
	ToId           string
	SessionBinding []byte
}

// Step passes an implementation-specific message to another cluster member.
func (s *ClusterService) Step(stream orderer.ClusterNodeService_StepServer) error {
	addr := util.ExtractRemoteAddress(stream.Context())
	commonName := commonNameFromContext(stream.Context())
	exp := s.initializeExpirationCheck(stream, addr, commonName)
	s.Logger.Debugf("Connection from %s(%s)", commonName, addr)

	// On a new stream, auth request is the first msg
	request, err := stream.Recv()
	if err == io.EOF {
		s.Logger.Debugf("%s(%s) disconnected well before establishing the stream", commonName, addr)
		return nil
	}
	if err != nil {
		s.Logger.Warnf("Stream read from %s failed: %v", addr, err)
		return err
	}

	s.Lock.RLock()
	authReq, err := s.VerifyAuthRequest(stream, request)
	if err != nil {
		s.Lock.RUnlock()
		s.Logger.Warnf("service authentication of %s failed with error: %v", addr, err)
		return status.Errorf(codes.Unauthenticated, "access denied")
	}

	streamID := atomic.AddUint64(&s.Membership.nextStreamID, 1)
	s.Membership.AuthorizedStreams.Store(streamID, authReq.FromId)
	s.Lock.RUnlock()

	defer s.Logger.Debugf("Closing connection from %s(%s)", commonName, addr)
	defer func() {
		s.Lock.RLock()
		s.Membership.AuthorizedStreams.Delete(streamID)
		s.Lock.RUnlock()
	}()

	for {
		err := s.handleMessage(stream, addr, exp, authReq.Channel, authReq.FromId, streamID)
		if err == io.EOF {
			s.Logger.Debugf("%s(%s) disconnected", commonName, addr)
			return nil
		}
		if err != nil {
			return err
		}
		// Else, no error occurred, so we continue to the next iteration
	}
}

func (s *ClusterService) VerifyAuthRequest(stream orderer.ClusterNodeService_StepServer, request *orderer.ClusterNodeServiceStepRequest) (*orderer.NodeAuthRequest, error) {
	authReq := request.GetNodeAuthrequest()
	if authReq == nil {
		return nil, errors.New("invalid request object")
	}

	bindingFieldsHash := GetSessionBindingHash(authReq)

	tlsBinding, err := GetTLSSessionBinding(stream.Context(), bindingFieldsHash)
	if err != nil {
		return nil, errors.Wrap(err, "session binding read failed")
	}

	if !bytes.Equal(tlsBinding, authReq.SessionBinding) {
		return nil, errors.New("session binding mismatch")
	}

	msg, err := asn1.Marshal(AuthRequestSignature{
		Version:        int64(authReq.Version),
		Timestamp:      EncodeTimestamp(authReq.Timestamp),
		FromId:         strconv.FormatUint(authReq.FromId, 10),
		ToId:           strconv.FormatUint(authReq.ToId, 10),
		SessionBinding: tlsBinding,
	})
	if err != nil {
		return nil, errors.Wrap(err, "ASN encoding failed")
	}

	membership := s.Membership
	if membership == nil {
		return nil, errors.Errorf("channel %s not found in config", authReq.Channel)
	}

	fromIdentity := membership.MemberMapping[authReq.FromId]
	if fromIdentity == nil {
		return nil, errors.Errorf("node %d is not member of channel %s", authReq.FromId, authReq.Channel)
	}

	toIdentity := membership.MemberMapping[authReq.ToId]
	if toIdentity == nil {
		return nil, errors.Errorf("node %d is not member of channel %s", authReq.ToId, authReq.Channel)
	}

	if !bytes.Equal(toIdentity, s.NodeIdentity) {
		return nil, errors.Errorf("node id mismatch")
	}

	err = VerifySignature(fromIdentity, SHA256Digest(msg), authReq.Signature)
	if err != nil {
		return nil, errors.Wrap(err, "signature mismatch")
	}

	return authReq, nil
}

func (s *ClusterService) handleMessage(stream ClusterStepStream, addr string, exp *certificateExpirationCheck, channel string, sender uint64, streamID uint64) error {
	request, err := stream.Recv()
	if err == io.EOF {
		return err
	}
	if err != nil {
		s.Logger.Warnf("Stream read from %s failed: %v", addr, err)
		return err
	}
	if request == nil {
		return errors.Errorf("request message is nil")
	}

	s.Lock.RLock()
	_, authorized := s.Membership.AuthorizedStreams.Load(streamID)
	s.Lock.RUnlock()

	if !authorized {
		return errors.Errorf("stream %d is stale", streamID)
	}

	exp.checkExpiration(time.Now(), channel)

	if tranReq := request.GetNodeTranrequest(); tranReq != nil {
		submitReq := &orderer.SubmitRequest{
			Channel:           channel,
			LastValidationSeq: tranReq.LastValidationSeq,
			Payload:           tranReq.Payload,
		}
		return s.RequestHandler.OnSubmit(channel, sender, submitReq)
	} else if clusterConReq := request.GetNodeConrequest(); clusterConReq != nil {
		conReq := &orderer.ConsensusRequest{
			Channel:  channel,
			Payload:  clusterConReq.Payload,
			Metadata: clusterConReq.Metadata,
		}
		return s.RequestHandler.OnConsensus(channel, sender, conReq)
	}
	return errors.Errorf("Message is neither a Submit nor Consensus request")
}

func (s *ClusterService) initializeExpirationCheck(stream orderer.ClusterNodeService_StepServer, endpoint, nodeName string) *certificateExpirationCheck {
	expiresAt := time.Time{}
	cert := util.ExtractCertificateFromContext(stream.Context())
	if cert != nil {
		expiresAt = cert.NotAfter
	}

	return &certificateExpirationCheck{
		minimumExpirationWarningInterval: s.MinimumExpirationWarningInterval,
		expirationWarningThreshold:       s.CertExpWarningThreshold,
		expiresAt:                        expiresAt,
		endpoint:                         endpoint,
		nodeName:                         nodeName,
		alert: func(template string, args ...interface{}) {
			s.Logger.Warnf(template, args...)
		},
	}
}

func (c *ClusterService) ConfigureNodeCerts(newNodes []*common.Consenter) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if c.Membership == nil {
		c.Membership = &MembersConfig{}
	}

	c.Logger.Infof("Updating nodes identity, nodes: %v", newNodes)

	c.Membership.MemberMapping = make(map[uint64][]byte)

	for _, nodeIdentity := range newNodes {
		c.Membership.MemberMapping[uint64(nodeIdentity.Id)] = nodeIdentity.Identity
	}

	// Iterate over existing streams and prune those that should not be there anymore
	c.Membership.AuthorizedStreams.Range(func(streamID, nodeID interface{}) bool {
		if _, exists := c.Membership.MemberMapping[nodeID.(uint64)]; !exists {
			c.Membership.AuthorizedStreams.Delete(streamID.(uint64))
		}
		return true
	})

	return nil
}
