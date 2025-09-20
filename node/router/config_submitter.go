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

//go:generate counterfeiter -o ./mocks/submitter.go . Submitter
type Submitter interface {
	Start()
	Stop()
	Update()
	Forward(tr *TrackedRequest) error
}

type configSubmitter struct {
	consensusEndpoint     string
	consensusRootCAs      [][]byte
	tlsCert               []byte
	tlsKey                []byte
	logger                types.Logger
	configRequestsChannel chan *TrackedRequest
	lock                  sync.RWMutex
	doneChannel           chan struct{}
	doneOnce              sync.Once
	startOnce             sync.Once
}

func (cs *configSubmitter) Start() {
	cs.startOnce.Do(func() {
		cs.logger.Debugf("config submitter is starting")
		go cs.ReadConfigRequests()
	})
}

func (cs *configSubmitter) Stop() {
	cs.doneOnce.Do(func() {
		cs.logger.Debugf("config submitter is stopping")
		close(cs.doneChannel)
	})
}

func (cs *configSubmitter) Update() {
	cs.lock.Lock()
	_ = true
	// TODO update consensus params
	cs.lock.Unlock()
}

func (cs *configSubmitter) ReadConfigRequests() {
	cs.logger.Infof("config submitter start listening for requests")
	for {
		select {
		case <-cs.doneChannel:
			cs.logger.Debugf("done channel is closed, stop listening on channel for config requests")
			return
		case tr, ok := <-cs.configRequestsChannel:
			if !ok {
				cs.logger.Debugf("config requests channel was closed, stop listening for requests")
				return
			}
			err := cs.forwardRequest(tr)
			if err != nil {
				cs.logger.Errorf("error forwarding config request to consenter: %v", err)
			}
		}
	}
}

func (cs *configSubmitter) Forward(tr *TrackedRequest) {
	cs.configRequestsChannel <- tr
}

func (cs *configSubmitter) forwardRequest(tr *TrackedRequest) error {
	if tr == nil {
		return fmt.Errorf("received nil tracked request")
	}
	resp, err := cs.submitConfigRequestToConsensus(tr.request)
	if err != nil {
		return err
	}
	tr.responses <- Response{SubmitResponse: resp}
	return nil
}

func (cs *configSubmitter) submitConfigRequestToConsensus(req *protos.Request) (*protos.SubmitResponse, error) {
	conn, err := cs.connectToConsenter()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cl := protos.NewConsensusClient(conn)

	resp, err := cl.SubmitConfig(context.Background(), req)

	return resp, err
}

func (cs *configSubmitter) connectToConsenter() (*grpc.ClientConn, error) {
	conn, err := cs.tryToConnect()
	if err == nil {
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

func (cs *configSubmitter) tryToConnect() (*grpc.ClientConn, error) {
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
