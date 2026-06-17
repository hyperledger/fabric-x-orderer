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

const (
	minRetryInterval = 50 * time.Millisecond
	maxRetryInterval = 10 * time.Second
	maxRetries       = 10
)

type ConfigAcker interface {
	SendConfigAckToConsensus(configSeq uint64) (*protos.ConfigAckResponse, error)
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
	ca := &configAcker{
		consensusEndpoint: connInfo.ConsensusEndpoint,
		consensusRootCAs:  connInfo.ConsensusRootCAs,
		tlsCert:           connInfo.TLSCert,
		tlsKey:            connInfo.TLSKey,
		logger:            logger,
		partyID:           connInfo.PartyID,
		nodeType:          connInfo.NodeType,
		shard:             connInfo.Shard,
	}
	return ca
}

// TODO: revisit retry + context
func (ca *configAcker) connectToConsenter() (*grpc.ClientConn, error) {
	conn, err := ca.tryToConnect()
	if err == nil {
		return conn, err
	}

	// repeatedly try to connect, with backoff
	interval := minRetryInterval
	for retry := 1; retry <= maxRetries; retry++ {
		if retry > 1 {
			<-time.After(interval)
			interval = min(interval*2, maxRetryInterval)
		}

		ca.logger.Debugf("Retry attempt #%d", retry)

		conn, err := ca.tryToConnect()
		if err == nil {
			return conn, nil
		}

		ca.logger.Errorf("Reconnection failed: %v", err)
	}

	return nil, fmt.Errorf("connection to consenter failed after %d retries", maxRetries)
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

	conn, err := cc.Dial(ca.consensusEndpoint)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (ca *configAcker) SendConfigAckToConsensus(configSeq uint64) (*protos.ConfigAckResponse, error) {
	conn, err := ca.connectToConsenter()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to consenter: %w", err)
	}
	defer conn.Close()

	client := protos.NewConsensusClient(conn)

	configAckReq := &protos.ConfigAck{
		ConfigSeq: configSeq,
		NodeType:  ca.nodeType,
		Shard:     uint32(ca.shard),
	}

	ca.logger.Infof("Sending ConfigAck for config sequence %d to consenter", configSeq)
	resp, err := client.AckConfig(context.Background(), configAckReq)

	return resp, err
}
