/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configack

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/stub"
	"github.com/stretchr/testify/require"
)

func createTestSetupForConfigAcker(t *testing.T, nodeType protos.NodeType, shard types.ShardID) (stub.StubConsenter, *configAcker) {
	logger := testutil.CreateLogger(t, 0)
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	clientPair, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	sc := stub.NewStubConsenter(t, ca, types.PartyID(1))

	return sc, NewConfigAcker(&ConnectionInfo{
		TLSCert:           clientPair.Cert,
		TLSKey:            clientPair.Key,
		ConsensusEndpoint: sc.GetConsenterEndpoint(),
		ConsensusRootCAs:  [][]byte{ca.CertBytes()},
		PartyID:           types.PartyID(1),
		NodeType:          nodeType,
		Shard:             shard,
	}, logger)
}

// TestSubmitConfigAck_Success verifies that a call to SubmitConfigAck returns nil
// when the consenter accepts the RPC on the first attempt.
func TestSubmitConfigAck_Success(t *testing.T) {
	stubConsenter, configAcker := createTestSetupForConfigAcker(t, protos.NodeType_ROUTER, 0)

	stubConsenter.Start()
	defer stubConsenter.Stop()

	require.NoError(t, configAcker.SubmitConfigAck(1))
	configAcker.Stop()
}

// TestSubmitConfigAck_RetriesOnSendFailure verifies that when the RPC attempt fails because of an error in consensus the acker retries and eventually succeeds.
func TestSubmitConfigAck_RetriesOnSendFailure(t *testing.T) {
	stubConsenter, configAcker := createTestSetupForConfigAcker(t, protos.NodeType_ROUTER, 0)

	var callCount int32
	stubConsenter.AckConfigHandler = func(req *protos.ConfigAck) (*protos.ConfigAckResponse, error) {
		if atomic.AddInt32(&callCount, 1) <= 10 {
			return nil, fmt.Errorf("temporary failure")
		}
		return &protos.ConfigAckResponse{}, nil
	}

	stubConsenter.Start()
	defer stubConsenter.Stop()

	require.NoError(t, configAcker.SubmitConfigAck(1))
	require.EqualValues(t, 11, atomic.LoadInt32(&callCount))
	configAcker.Stop()
}

// TestSubmitConfigAck_RetriesOnConnectFailure verifies that the acker keeps
// retrying when the server is initially down and succeeds once it comes back up.
func TestSubmitConfigAck_RetriesOnConnectFailure(t *testing.T) {
	stubConsenter, configAcker := createTestSetupForConfigAcker(t, protos.NodeType_ROUTER, 0)

	configAcker.DialTimeout = 2 * time.Second
	configAcker.minRetryInterval = 2 * time.Millisecond
	configAcker.maxRetryInterval = 2 * time.Second

	// Start the server after a short delay
	go func() {
		time.AfterFunc(10*time.Second, func() {
			stubConsenter.Start()
		})
	}()

	defer stubConsenter.Stop()

	require.NoError(t, configAcker.SubmitConfigAck(1))
	configAcker.Stop()
}

// TestSubmitConfigAck_StopCancelsBlockedCall verifies that Stop() unblocks a
// SubmitConfigAck that is stuck retrying against an unavailable consenter.
func TestSubmitConfigAck_StopCancelsRetries(t *testing.T) {
	_, configAcker := createTestSetupForConfigAcker(t, protos.NodeType_ROUTER, 0)

	done := make(chan error, 1)
	go func() {
		done <- configAcker.SubmitConfigAck(5)
	}()

	// Give the acker time to attempt at least one connection.
	time.Sleep(10 * time.Second)
	configAcker.Stop()

	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorContains(t, err, "sending config ack to consensus aborted")
	case <-time.After(5 * time.Second):
		t.Fatal("SubmitConfigAck did not return after Stop was called")
	}
}
