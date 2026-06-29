/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configack

import (
	"context"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"google.golang.org/grpc"
)

const (
	minRetryInterval = 50 * time.Millisecond
	maxRetryInterval = 10 * time.Second
)

type ConfigAcker interface {
	Stop()
	SubmitConfigAck(configSeq uint64) error
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

func (ca *configAcker) SubmitConfigAck(configSeq uint64) error {
	return ca.handleConfigAck(configSeq)
}

func (ca *configAcker) Stop() {
	ca.logger.Infof("config acker is stopping")
	ca.cancel()
}

// handleConfigAck keeps trying to deliver a ConfigAck for configSeq until either the ack is successfully sent to the consenter,
// or the config acker context is cancelled.
func (ca *configAcker) handleConfigAck(configSeq uint64) error {
	for {
		conn, err := ca.connectToConsenterWithRetry()
		if err != nil {
			return err // context cancelled
		}

		err = ca.sendConfigAckToConsensus(conn, configSeq)
		_ = conn.Close()

		if err == nil {
			return nil
		}

		ca.logger.Infof("failed send config ack to consensus, err: %v\n", err)
		// if send fails, then exit check if the context is done
		if ca.ctx.Err() != nil {
			return ca.ctx.Err()
		}
	}
}

// connectToConsenterWithRetry tries to establish a gRPC connection to the consenter, using exponential backoff between attempts.
// It returns only when a connection is successfully established, or the config acker context is cancelled.
func (ca *configAcker) connectToConsenterWithRetry() (*grpc.ClientConn, error) {
	interval := minRetryInterval

	for {
		conn, err := ca.tryToConnect()
		if err == nil {
			ca.logger.Infof("Connected to consenter %s", ca.consensusEndpoint)
			return conn, nil
		}

		ca.logger.Warnf("Failed connecting to consenter: %v. Retrying in %s", err, interval)

		select {
		case <-ca.ctx.Done():
			return nil, ca.ctx.Err()
		case <-time.After(interval):
		}

		interval = min(interval*2, maxRetryInterval)
	}
}

func (ca *configAcker) tryToConnect() (*grpc.ClientConn, error) {
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
		DialTimeout: time.Second * 20,
	}

	return cc.Dial(ca.consensusEndpoint)
}

// sendConfigAckToConsensus sends a single ConfigAck RPC attempt over the given connection.
// The RPC is bounded by a timeout derived from ca.ctx. If ca.ctx is cancelled, the RPC returns with an error.
// The ConfigAckResponse is intentionally ignored because the response carries no
// useful semantics for the router, batcher and assembler; only the RPC error is used.
func (ca *configAcker) sendConfigAckToConsensus(conn *grpc.ClientConn, configSeq uint64) error {
	ctx, cancel := context.WithTimeout(ca.ctx, 30*time.Second)
	defer cancel()

	client := protos.NewConsensusClient(conn)

	configAckReq := &protos.ConfigAck{
		ConfigSeq: configSeq,
		NodeType:  ca.nodeType,
		Shard:     uint32(ca.shard),
	}

	ca.logger.Infof("Sending ConfigAck for config sequence %d to consenter", configSeq)
	_, err := client.AckConfig(ctx, configAckReq)
	return err
}
