/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"fmt"
	"testing"
	"time"

	policyMocks "github.com/hyperledger/fabric-x-orderer/common/policy/mocks"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/internal/pkg/identity/mocks"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/stub"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

type configSubmitTestSetup struct {
	stubConsenter   *stub.StubConsenter
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

func createConfigSubmitTestSetup(t *testing.T) configSubmitTestSetup {
	logger := testutil.CreateLogger(t, 0)
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	stubConsenter := stub.NewStubConsenter(t, ca, types.PartyID(1))

	ckp, err := ca.NewServerCertKeyPair("127.0.0.1")
	require.NoError(t, err)

	bundle, verifier := createTestBundleAndVerifier()
	fakeSigner := &mocks.SignerSerializer{}

	mockConfigUpdateProposer := &policyMocks.FakeConfigUpdateProposer{}
	mockConfigUpdateProposer.ProposeConfigUpdateReturns(nil, nil)

	configSubmitter := NewConfigSubmitter(stubConsenter.GetConsenterEndpoint(), [][]byte{ca.CertBytes()}, ckp.Cert, ckp.Key, logger, bundle, verifier, fakeSigner, mockConfigUpdateProposer)

	return configSubmitTestSetup{configSubmitter: configSubmitter, stubConsenter: &stubConsenter}
}

func TestConfigSubmitterForward(t *testing.T) {
	setup := createConfigSubmitTestSetup(t)
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
	setup := createConfigSubmitTestSetup(t)
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
	setup := createConfigSubmitTestSetup(t)
	setup.Start()
	defer setup.Stop()

	configSubmitter := setup.configSubmitter
	stubConsenter := setup.stubConsenter

	req := tx.CreateStructuredRequest([]byte("data"))
	feedbackChan := make(chan Response, 1)

	// submit one request, and wait for the response
	configSubmitter.Forward(&TrackedRequest{request: req, responses: feedbackChan})
	resp := <-feedbackChan
	require.Equal(t, resp.SubmitResponse.Error, "dummy submit config")

	// stop the consenter and wait until it is down
	stubConsenter.Stop()
	time.Sleep(250 * time.Millisecond)

	// forward another request
	configSubmitter.Forward(&TrackedRequest{request: req, responses: feedbackChan})

	// wait and restart the consenter
	time.Sleep(250 * time.Millisecond)
	stubConsenter.Restart()

	resp = <-feedbackChan
	require.Equal(t, resp.SubmitResponse.Error, "dummy submit config")

	require.Eventually(t, func() bool {
		return stubConsenter.ReceivedMessageCount() == uint32(2)
	}, 10*time.Second, 10*time.Millisecond)
}

func TestConfigSubmitterReconnectionAbort(t *testing.T) {
	setup := createConfigSubmitTestSetup(t)
	setup.Start()

	configSubmitter := setup.configSubmitter
	stubConsenter := setup.stubConsenter
	req := tx.CreateStructuredRequest([]byte("data"))
	feedbackChan := make(chan Response, 1)

	// submit one request, and wait for the response
	configSubmitter.Forward(&TrackedRequest{request: req, responses: feedbackChan})
	resp := <-feedbackChan
	require.Equal(t, resp.SubmitResponse.Error, "dummy submit config")

	// stop the consenter and wait until it is down
	stubConsenter.Stop()
	time.Sleep(250 * time.Millisecond)

	// forward a request and stop the config submitter
	errChan := make(chan error)
	go func() {
		errChan <- configSubmitter.forwardRequest(&TrackedRequest{request: req, responses: feedbackChan})
	}()
	configSubmitter.Stop()

	err := <-errChan
	require.EqualError(t, err, fmt.Sprintf("reconnection to consensus %s aborted, because context is done and configSubmitter stopped", configSubmitter.consensusEndpoint))
}
