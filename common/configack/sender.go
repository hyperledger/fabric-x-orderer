/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configack

import (
	"context"
	"fmt"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"google.golang.org/grpc"
)

type Sender interface {
	Stop()
	SubmitConfigAck(configSeq uint32) error
}

type sender struct {
	consensusEndpoint string
	consensusRootCAs  [][]byte
	tlsCert           []byte
	tlsKey            []byte
	logger            *flogging.FabricLogger
	partyID           types.PartyID
	nodeType          protos.NodeType
	shard             types.ShardID

	ctx    context.Context
	cancel context.CancelFunc

	minRetryInterval time.Duration
	maxRetryInterval time.Duration
	DialTimeout      time.Duration
}

type ConnectionInfo struct {
	TLSCert           []byte
	TLSKey            []byte
	ConsensusEndpoint string
	ConsensusRootCAs  [][]byte
	PartyID           types.PartyID
	NodeType          protos.NodeType
	Shard             types.ShardID
}

func NewSender(connInfo *ConnectionInfo, logger *flogging.FabricLogger) *sender {
	ctx, cancel := context.WithCancel(context.Background())

	s := &sender{
		consensusEndpoint: connInfo.ConsensusEndpoint,
		consensusRootCAs:  connInfo.ConsensusRootCAs,
		tlsCert:           connInfo.TLSCert,
		tlsKey:            connInfo.TLSKey,
		logger:            logger,
		partyID:           connInfo.PartyID,
		nodeType:          connInfo.NodeType,
		shard:             connInfo.Shard,
		ctx:               ctx,
		cancel:            cancel,
		minRetryInterval:  50 * time.Millisecond, // TODO: take from config
		maxRetryInterval:  10 * time.Second,      // TODO: take from config
		DialTimeout:       5 * time.Second,       // TODO: take from config
	}
	return s
}

// SubmitConfigAck attempts to deliver a ConfigAck for the given configSeq to the consenter.
//
// The whole delivery process is bounded by a single timeout context derived from ca.ctx.
// This timeout covers all retry attempts together. Therefore, once the timeout expires,
// SubmitConfigAck stops retrying and returns an error.
//
// Each retry attempt consists of:
//   - connect to the consenter
//   - send the ConfigAck
//
// If either the connection or the send fails, the function waits before retrying the
// entire flow again. The wait interval uses exponential backoff, starting from
// minRetryInterval and doubling after each failed attempt, up to maxRetryInterval.
//
// The function returns nil once the ConfigAck is successfully sent. It returns an error
// if the total timeout expires or if ca.ctx is cancelled, for example when the
// sender is stopped.
func (s *sender) SubmitConfigAck(configSeq uint32) error {
	ctx, cancel := context.WithTimeout(s.ctx, 60*time.Second)
	defer cancel()

	err := s.submitWithRetry(ctx, configSeq)
	if err == nil {
		return nil
	}

	interval := s.minRetryInterval
	numOfRetries := 1
	s.logger.Warningf("Sending config ack to consensus on sequence %d failed: %v, trying again in: %s", configSeq, err, interval)

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("sending config ack to consensus aborted: %w", ctx.Err())
		case <-time.After(interval):
			s.logger.Debugf("Retry attempt #%d", numOfRetries)
			numOfRetries++

			err := s.submitWithRetry(ctx, configSeq)
			if err != nil {
				interval = min(interval*2, s.maxRetryInterval)
				s.logger.Warningf("Sending config ack to consensus on sequence %d failed: %v, trying again in: %s", configSeq, err, interval)
				continue
			}

			s.logger.Infof("config ack was successfully sent to consensus on config sequence %d", configSeq)
			return nil
		}
	}
}

func (s *sender) Stop() {
	s.logger.Infof("config acker is stopping")
	s.cancel()
}

func (s *sender) submitWithRetry(ctx context.Context, configSeq uint32) error {
	conn, err := s.connectToConsenter()
	if err != nil {
		s.logger.Errorf("failed to connect to consensus, err: %v\n", err)
		return err
	}
	defer conn.Close()

	err = s.sendConfigAckToConsensus(ctx, conn, configSeq)
	if err != nil {
		s.logger.Errorf("failed to send config ack to consensus on config sequence %d, err: %v\n", configSeq, err)
		return err
	}

	return nil
}

func (s *sender) connectToConsenter() (*grpc.ClientConn, error) {
	cc := comm.ClientConfig{
		AsyncConnect: false,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: 30 * time.Second,
			ClientTimeout:  30 * time.Second,
		},
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			ServerRootCAs:     s.consensusRootCAs,
			Key:               s.tlsKey,
			Certificate:       s.tlsCert,
			RequireClientCert: true,
		},
		DialTimeout: s.DialTimeout,
	}

	return cc.Dial(s.consensusEndpoint)
}

// sendConfigAckToConsensus sends a single ConfigAck RPC attempt over the given connection.
// The RPC is bounded by a timeout derived from ca.ctx. If ca.ctx is cancelled, the RPC returns with an error.
// If the consenter returns an error in the ConfigAckResponse, it is logged for
// visibility. The response error is not returned to the caller because the client
// does not take any recovery action based on this response; only the RPC error is
// used to decide whether the ConfigAck should be retried.
func (s *sender) sendConfigAckToConsensus(ctx context.Context, conn *grpc.ClientConn, configSeq uint32) error {
	client := protos.NewConsensusClient(conn)

	configAckReq := &protos.ConfigAck{
		ConfigSeq: configSeq,
		NodeType:  s.nodeType,
		Shard:     uint32(s.shard),
	}

	s.logger.Infof("Sending ConfigAck for config sequence %d to consenter", configSeq)
	resp, err := client.AckConfig(ctx, configAckReq)
	if resp != nil && resp.GetError() != "" {
		s.logger.Warnf("Received bad response from consensus on config ack on config sequence %d: %s", configSeq, resp.GetError())
	}
	return err
}
