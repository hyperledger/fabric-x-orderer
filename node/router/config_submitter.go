/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/policy"
	"github.com/hyperledger/fabric-x-orderer/common/requestfilter"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config/verify"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"google.golang.org/grpc"
)

type ConfigurationSubmitter interface {
	Start()
	Stop()
	// Update() // TODO implement a thread-safe update method for config submitter
	Forward(tr *TrackedRequest)
}

type configSubmitter struct {
	consensusEndpoint     string
	consensusRootCAs      [][]byte
	tlsCert               []byte
	tlsKey                []byte
	logger                types.Logger
	configRequestsChannel chan *TrackedRequest
	ctx                   context.Context
	cancelFunc            func()
	bundle                channelconfig.Resources
	verifier              *requestfilter.RulesVerifier
	signer                identity.SignerSerializer
	configUpdateProposer  policy.ConfigUpdateProposer
	configRulesVerifier   verify.OrdererRules
}

func NewConfigSubmitter(consensusEndpoint string, consensusRootCAs [][]byte, tlsCert []byte, tlsKey []byte, logger types.Logger, bundle channelconfig.Resources, verifier *requestfilter.RulesVerifier, signer identity.SignerSerializer, configUpdateProposer policy.ConfigUpdateProposer, configRulesVerifier verify.OrdererRules) *configSubmitter {
	cs := &configSubmitter{
		consensusEndpoint:     consensusEndpoint,
		consensusRootCAs:      consensusRootCAs,
		tlsCert:               tlsCert,
		tlsKey:                tlsKey,
		logger:                logger,
		configRequestsChannel: make(chan *TrackedRequest, 100),
		bundle:                bundle,
		verifier:              verifier,
		signer:                signer,
		configUpdateProposer:  configUpdateProposer,
		configRulesVerifier:   configRulesVerifier,
	}
	return cs
}

func (cs *configSubmitter) Start() {
	cs.logger.Infof("config submitter is starting")
	cs.ctx, cs.cancelFunc = context.WithCancel(context.Background())
	go cs.readConfigRequests()
}

func (cs *configSubmitter) Stop() {
	cs.logger.Infof("config submitter is stopping")
	cs.cancelFunc()
}

func (cs *configSubmitter) readConfigRequests() {
	cs.logger.Infof("config submitter start listening for requests")
	for {
		select {
		case <-cs.ctx.Done():
			cs.logger.Infof("context is done, stop listening on channel for config requests")
			return
		case tr, ok := <-cs.configRequestsChannel:
			if !ok {
				cs.logger.Infof("config requests channel was closed, stop listening for requests")
				return
			}
			err := cs.forwardRequest(tr)
			if err != nil {
				cs.logger.Errorf("error forwarding config request to consenter: %v", err)
			}
		}
	}
}

// Forward forwards the config request from the shard router to the config submitter requests channel
func (cs *configSubmitter) Forward(tr *TrackedRequest) {
	cs.configRequestsChannel <- tr
}

// forwardRequest forwards the config request from the config submitter to the consensus
func (cs *configSubmitter) forwardRequest(tr *TrackedRequest) error {
	if tr == nil {
		return fmt.Errorf("received nil tracked request")
	}

	feedback := Response{}
	var err error

	configRequest, err := cs.configUpdateProposer.ProposeConfigUpdate(tr.request, cs.bundle, cs.signer, cs.verifier)
	if err != nil {
		feedback.err = fmt.Errorf("error in verification and proposing update: %s", err)
		tr.responses <- feedback
		return err
	}

	env := &common.Envelope{Payload: configRequest.Payload, Signature: configRequest.Signature}
	if err = cs.configRulesVerifier.ValidateNewConfig(env); err != nil {
		feedback.err = fmt.Errorf("error in validating config rules: %w", err)
		tr.responses <- feedback
		return err
	}

	if err = cs.configRulesVerifier.ValidateTransition(cs.bundle, env); err != nil {
		feedback.err = fmt.Errorf("error in validating config transition rules: %w", err)
		tr.responses <- feedback
		return err
	}

	var resp *protos.SubmitResponse
	resp, err = cs.submitConfigRequestToConsensus(tr.request)
	feedback.SubmitResponse = resp
	if err != nil {
		feedback.err = fmt.Errorf("error forwarding config request to consenter: %v", err)
	} else {
		feedback.SubmitResponse = resp
		if resp.Error != "" {
			feedback.err = errors.New(resp.Error)
		}
	}

	tr.responses <- feedback
	return err
}

func (cs *configSubmitter) submitConfigRequestToConsensus(req *protos.Request) (*protos.SubmitResponse, error) {
	conn, err := cs.connectToConsenter()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cl := protos.NewConsensusClient(conn)

	resp, err := cl.SubmitConfig(cs.ctx, req)

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
		case <-cs.ctx.Done():
			return nil, fmt.Errorf("reconnection to consensus %s aborted, because context is done and configSubmitter stopped", cs.consensusEndpoint)
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
	// TODO - make it thread-safe, when implementing the update method

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
