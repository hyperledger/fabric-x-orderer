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

// TODO: take minRetryInterval + maxRetryInterval + DialTimeout from config
const (
	minRetryInterval = 50 * time.Millisecond
	maxRetryInterval = 10 * time.Second
)

type ConfigAcker interface {
	Stop()
	SubmitConfigAck(configSeq uint32) error
}

type configAcker struct {
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

func NewConfigAcker(connInfo *ConnectionInfo, logger *flogging.FabricLogger) *configAcker {
	ctx, cancel := context.WithCancel(context.Background())

	ca := &configAcker{
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
	}
	return ca
}

func (ca *configAcker) SubmitConfigAck(configSeq uint32) error {
	return ca.handleConfigAck(configSeq)
}

func (ca *configAcker) Stop() {
	ca.logger.Infof("config acker is stopping")
	ca.cancel()
}

// handleConfigAck attempts to deliver a ConfigAck for the given configSeq to the consenter.
//
// The whole delivery process is bounded by a single timeout context derived from ca.ctx.
// This timeout covers all retry attempts together. Therefore, once the timeout expires,
// handleConfigAck stops retrying and returns an error.
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
// configAcker is stopped.
func (ca *configAcker) handleConfigAck(configSeq uint32) error {
	ctx, cancel := context.WithTimeout(ca.ctx, 60*time.Second)
	defer cancel()

	err := ca.tryToHandleConfigAck(ctx, configSeq)
	if err == nil {
		return nil
	}

	interval := minRetryInterval
	numOfRetries := 1

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("sending config ack to consensus aborted, because context is done and configAcker stopped")
		case <-time.After(interval):
			ca.logger.Debugf("Retry attempt #%d", numOfRetries)
			numOfRetries++

			err := ca.tryToHandleConfigAck(ctx, configSeq)
			if err != nil {
				interval = min(interval*2, maxRetryInterval)
				ca.logger.Errorf("Sending config ack to consensus failed: %v, trying again in: %s", err, interval)
				continue
			}

			ca.logger.Infof("config ack was successfully sent to consensus")
			return nil
		}
	}
}

func (ca *configAcker) tryToHandleConfigAck(ctx context.Context, configSeq uint32) error {
	conn, err := ca.connectToConsenter()
	if err != nil {
		ca.logger.Errorf("failed to connect to consensus, err: %v\n", err)
		return err
	}
	defer conn.Close()

	err = ca.sendConfigAckToConsensus(ctx, conn, configSeq)
	if err != nil {
		ca.logger.Errorf("failed to send config ack to consensus, err: %v\n", err)
		return err
	}

	return nil
}

func (ca *configAcker) connectToConsenter() (*grpc.ClientConn, error) {
	cc := comm.ClientConfig{
		AsyncConnect: false,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: 30 * time.Second,
			ClientTimeout:  30 * time.Second,
		},
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			ServerRootCAs:     ca.consensusRootCAs,
			Key:               ca.tlsKey,
			Certificate:       ca.tlsCert,
			RequireClientCert: true,
		},
		DialTimeout: time.Second * 5,
	}

	return cc.Dial(ca.consensusEndpoint)
}

// sendConfigAckToConsensus sends a single ConfigAck RPC attempt over the given connection.
// The RPC is bounded by a timeout derived from ca.ctx. If ca.ctx is cancelled, the RPC returns with an error.
// If the consenter returns an error in the ConfigAckResponse, it is logged for
// visibility. The response error is not returned to the caller because the client
// does not take any recovery action based on this response; only the RPC error is
// used to decide whether the ConfigAck should be retried.
func (ca *configAcker) sendConfigAckToConsensus(ctx context.Context, conn *grpc.ClientConn, configSeq uint32) error {
	client := protos.NewConsensusClient(conn)

	configAckReq := &protos.ConfigAck{
		ConfigSeq: configSeq,
		NodeType:  ca.nodeType,
		Shard:     uint32(ca.shard),
	}

	ca.logger.Infof("Sending ConfigAck for config sequence %d to consenter", configSeq)
	resp, err := client.AckConfig(ctx, configAckReq)
	if resp != nil && resp.GetError() != "" {
		ca.logger.Warnf("Received bad response from consensus on config ack: %v\n", resp.Error)
	}
	return err
}
