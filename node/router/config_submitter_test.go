/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"fmt"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

type configSubmitTestSetup struct {
	stubConsenter   *stubConsenter
	configSubmitter *configSubmitter
}

func (s configSubmitTestSetup) Start() {
	s.stubConsenter.Start()
	s.configSubmitter.Start()
}

func (s configSubmitTestSetup) Stop() {
	s.stubConsenter.Stop()
	s.configSubmitter.Stop()
}

func createconfigSubmitTestSetup(t *testing.T) configSubmitTestSetup {
	logger := testutil.CreateLogger(t, 0)
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	stubConsenter := NewStubConsenter(t, ca, types.PartyID(1), logger)

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	configSubmitter := configSubmitter{
		consensusEndpoint:     stubConsenter.GetConsenterEndpoint(),
		consensusRootCAs:      [][]byte{ca.CertBytes()},
		tlsCert:               ckp.Cert,
		tlsKey:                ckp.Key,
		logger:                logger,
		configRequestsChannel: make(chan *TrackedRequest, 100),
		doneChannel:           make(chan struct{}),
	}
	return configSubmitTestSetup{configSubmitter: &configSubmitter, stubConsenter: &stubConsenter}
}

func TestConfigSubmitterForward(t *testing.T) {
	setup := createconfigSubmitTestSetup(t)
	setup.Start()
	defer setup.Stop()
	configSubmitter := setup.configSubmitter
	stubConsenter := setup.stubConsenter

	req := tx.CreateStructuredRequest([]byte("data"))
	feedbackChan := make(chan Response, 1)
	configSubmitter.Forward(&TrackedRequest{request: req, responses: feedbackChan})

	resp := <-feedbackChan
	require.Equal(t, resp.SubmitResponse.Error, "dummy submit config")

	require.Eventually(t, func() bool {
		return stubConsenter.ReceivedMessageCount() == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)
}

func TestConfigSubmitterMultipleForward(t *testing.T) {
	setup := createconfigSubmitTestSetup(t)
	setup.Start()
	defer setup.Stop()
	configSubmitter := setup.configSubmitter
	stubConsenter := setup.stubConsenter

	numOfRequests := 10
	for i := 0; i < numOfRequests; i++ {
		req := tx.CreateStructuredRequest([]byte("data"))
		feedbackChan := make(chan Response, 1)
		configSubmitter.Forward(&TrackedRequest{request: req, responses: feedbackChan})

		resp := <-feedbackChan
		require.Equal(t, resp.SubmitResponse.Error, "dummy submit config")
	}

	require.Eventually(t, func() bool {
		return stubConsenter.ReceivedMessageCount() == uint32(numOfRequests)
	}, 10*time.Second, 10*time.Millisecond)
}

func TestForwardWithConsensusRestart(t *testing.T) {
	setup := createconfigSubmitTestSetup(t)
	setup.Start()
	defer setup.Stop()

	configSubmitter := setup.configSubmitter
	stubConsenter := setup.stubConsenter

	// wait until consenter is up
	require.Eventually(t, func() bool {
		conn, err := configSubmitter.tryToConnect()
		if err == nil {
			conn.Close()
			return true
		} else {
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)

	stubConsenter.Stop()

	// wait until consenter is down
	require.Eventually(t, func() bool {
		conn, err := configSubmitter.tryToConnect()
		if err == nil {
			conn.Close()
			return false
		} else {
			return true
		}
	}, 10*time.Second, 10*time.Millisecond)

	req := tx.CreateStructuredRequest([]byte("data"))
	feedbackChan := make(chan Response, 1)

	go func() {
		time.Sleep(500 * time.Millisecond)
		stubConsenter.Restart()
	}()

	configSubmitter.Forward(&TrackedRequest{request: req, responses: feedbackChan})

	resp := <-feedbackChan
	require.Equal(t, resp.SubmitResponse.Error, "dummy submit config")

	require.Eventually(t, func() bool {
		return stubConsenter.ReceivedMessageCount() == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)
}

func TestConfigSubmitterReconnectionAbort(t *testing.T) {
	setup := createconfigSubmitTestSetup(t)

	configSubmitter := setup.configSubmitter
	stubConsenter := setup.stubConsenter

	setup.Start()
	// wait until consenter is up
	require.Eventually(t, func() bool {
		conn, err := configSubmitter.tryToConnect()
		if err == nil {
			conn.Close()
			return true
		} else {
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)

	stubConsenter.Stop()

	// wait until consenter is down
	require.Eventually(t, func() bool {
		conn, err := configSubmitter.tryToConnect()
		if err == nil {
			conn.Close()
			return false
		} else {
			return true
		}
	}, 10*time.Second, 10*time.Millisecond)

	req := tx.CreateStructuredRequest([]byte("data"))
	feedbackChan := make(chan Response, 1)

	errChan := make(chan error)
	go func() {
		errChan <- configSubmitter.forwardRequest(&TrackedRequest{request: req, responses: feedbackChan})
	}()
	go func() {
		configSubmitter.Stop()
	}()

	err := <-errChan
	require.EqualError(t, err, fmt.Sprintf("reconnection to consensus %s aborted, because done channel is closed", configSubmitter.consensusEndpoint))
}
