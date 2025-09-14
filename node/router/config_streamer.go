/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"google.golang.org/grpc"
)

type configStreamer struct {
	consensusEndpoint     string
	consensusRootCAs      [][]byte
	tlsCert               []byte
	tlsKey                []byte
	logger                types.Logger
	configRequestsChannel chan *TrackedRequest
	lock                  sync.RWMutex
	doneChannel           chan bool
	doneOnce              sync.Once
	startOnce             sync.Once
}

func (cs *configStreamer) Start() {
	cs.startOnce.Do(func() {
		cs.logger.Debugf("config streamer was started")
		go cs.ReadConfigRequests()
	})
}

func (cs *configStreamer) Stop() {
	cs.doneOnce.Do(func() {
		cs.logger.Debugf("config streamer was stopped")
		close(cs.doneChannel)
	})
}

func (cs *configStreamer) Update() {
	cs.lock.Lock()
	_ = true
	// TODO update consensus params
	cs.lock.Unlock()
}

func (cs *configStreamer) ReadConfigRequests() {
	cs.logger.Infof("config streamer start listening for requests")
	for {
		select {
		case <-cs.doneChannel:
			cs.logger.Debugf("done channel is closed, stop listening for requests")
			return
		case tr, ok := <-cs.configRequestsChannel:
			if !ok {
				cs.logger.Debugf("config requests channel was closed, stop listening for requests")
				return
			}
			cs.Forward(tr)
		}
	}
}

func (cs *configStreamer) Forward(tr *TrackedRequest) error {
	req := tr.request
	resp, err := cs.submitConfigRequestToConsensus(req)
	if err != nil {
		return err
	}
	if tr.trace != nil {
		_ = resp
		tr.responses <- Response{}
	}
	return nil
}

func (cs *configStreamer) submitConfigRequestToConsensus(req *protos.Request) (*protos.SubmitResponse, error) {
	conn, err := cs.connectToConsenter()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cl := protos.NewConsensusClient(conn)
	// just forward the request for now
	resp, err := cl.SubmitConfig(context.Background(), req)

	return resp, err
}

// func (cs *configStreamer) broadcastConfigRequestToConsensus(env *common.Envelope) (*ab.BroadcastResponse, error) {

// 	conn, err := cs.connectToConsenter()
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer conn.Close()

// 	cl := ab.NewAtomicBroadcastClient(conn)
// 	stream, err := cl.Broadcast(context.Background())
// 	if err != nil {
// 		return nil, err
// 	}

// 	err = stream.Send(env)
// 	if err != nil {
// 		return nil, err
// 	}

// 	resp, err := stream.Recv()
// 	return resp, err
// }

func (cs *configStreamer) connectToConsenter() (*grpc.ClientConn, error) {
	conn, err := cs.tryToConnect()
	if err != nil {
		return conn, err
	}

	// repeatedly try to connect, with backoff
	interval := minRetryInterval
	numOfRetries := 1
	for {
		select {
		case <-cs.doneChannel:
			return nil, fmt.Errorf("reconnection to consensus %s aborted, because done channel is closed", cs.consensusEndpoint)
		case <-time.After(interval):
			cs.logger.Debugf("Retry attempt #%d", numOfRetries)
			numOfRetries++
			conn, err := cs.tryToConnect()
			if err != nil {
				interval = min(interval*2, maxRetryInterval)
				cs.logger.Errorf("Reconnection to consensus failed: %v, trying again in: %s", err, interval)
				continue
			} else {
				cs.logger.Debugf("Reconnection to consensus %s succeeded", cs.consensusEndpoint)
				return conn, nil
			}
		}
	}
}

func (cs *configStreamer) tryToConnect() (*grpc.ClientConn, error) {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	cc := comm.ClientConfig{
		AsyncConnect: false,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: 30 * time.Second,
			ClientTimeout:  30 * time.Second,
		},
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			ServerRootCAs:     cs.consensusRootCAs,
			Key:               cs.tlsKey,
			Certificate:       cs.tlsCert,
			RequireClientCert: true,
		},
		DialTimeout: time.Second * 20,
	}

	conn, err := cc.Dial(cs.consensusEndpoint)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
