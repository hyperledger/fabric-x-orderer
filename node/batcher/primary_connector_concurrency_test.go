/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

const concurrentSenders = 64

// Scenario:
//  1. Start a primary that reads the stream of forwarded requests.
//  2. Forward requests from many goroutines through a connector that has no stream yet.
//  3. Expect all the requests to reach the primary over a single stream: senders that find no stream
//     together must not each reset the connection the previous one just made.
func TestPrimaryReqConnectorConnectsOnceWhenManySendersFindNoStream(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	primary := newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, primary)
	connector := newReqConnector(t, nodes, batchers)

	var sends sync.WaitGroup
	for range concurrentSenders {
		sends.Go(func() { connector.SendReq([]byte{1}) })
	}
	requireDone(t, &sends, "the senders did not return")

	require.Eventually(t, func() bool { return primary.requests.Load() == concurrentSenders }, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return primary.streams.Load() == 1
	}, 10*time.Second, 10*time.Millisecond, "the senders opened more than one stream")
}

// Scenario:
//  1. Start a primary that reads the stream of forwarded requests, and connect to it.
//  2. Forward requests from many goroutines at once, as first-strike resends do.
//  3. Expect all the requests to reach the primary. This is a smoke test for serialized sends;
//     the race detector does not report concurrent SendMsg calls on a gRPC stream.
func TestPrimaryReqConnectorSendsFromManyGoroutines(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	primary := newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, primary)
	connector := newReqConnector(t, nodes, batchers)
	connector.ConnectToPrimary()
	<-primary.accepted

	var sends sync.WaitGroup
	for range concurrentSenders {
		sends.Go(func() { connector.SendReq(make([]byte, 1024)) })
	}
	requireDone(t, &sends, "the senders did not return")

	require.Eventually(t, func() bool { return primary.requests.Load() == concurrentSenders }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), primary.streams.Load())
}

// Scenario:
//  1. Start primary 1, which accepts the stream of forwarded requests but never reads from it, and
//     primary 2, which reads it.
//  2. Forward requests to primary 1 from many goroutines, until the sends block because the stream is full.
//  3. Switch the connector to primary 2, which makes all the blocked senders reconnect at once.
//  4. Expect the senders to return, requests to reach primary 2, and the senders to share one stream to it.
func TestPrimaryReqConnectorOpensOneStreamWhenSwitchingWithBlockedSenders(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	stalled, reading := newFakePrimary(false), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, stalled, reading)
	connector := newReqConnector(t, nodes, batchers)
	connector.ConnectToPrimary()
	<-stalled.accepted

	request := make([]byte, 64*1024)
	var sends sync.WaitGroup
	var returned atomic.Int32
	for range concurrentSenders {
		sends.Go(func() {
			connector.SendReq(request)
			returned.Add(1)
		})
	}
	waitUntilSendsBlock(t, &returned, concurrentSenders)

	connector.ConnectToNewPrimary(types.PartyID(2))
	requireDone(t, &sends, "the senders blocked on the old primary did not return")

	require.Eventually(t, func() bool { return reading.requests.Load() > 0 }, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), reading.streams.Load(), "the senders opened more than one stream to the new primary")
}

// Scenario:
//  1. Start a primary that reads the stream of acks.
//  2. Send acks from many goroutines through a connector that has no stream yet.
//  3. Expect the acks to share a single stream.
func TestPrimaryAckConnectorConnectsOnceWhenManySendersFindNoStream(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	primary := newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, primary)
	conf := &node_config.BatcherNodeConfig{
		PartyId:            2,
		ShardId:            0,
		TLSPrivateKeyFile:  nodes[0].TLSKey,
		TLSCertificateFile: nodes[0].TLSCert,
	}
	connector := batcher.CreatePrimaryAckConnector(
		1, 0, testutil.CreateLogger(t, 2), conf, batchers, context.Background(),
		5*time.Second, 100*time.Millisecond, 500*time.Millisecond,
	)
	t.Cleanup(connector.Stop)

	var sends sync.WaitGroup
	for seq := range concurrentSenders {
		sends.Go(func() { connector.SendAck(types.BatchSequence(seq), types.PartyID(1)) })
	}
	requireDone(t, &sends, "the senders did not return")

	require.Eventually(t, func() bool {
		return primary.streams.Load() == 1
	}, 10*time.Second, 10*time.Millisecond, "the senders opened more than one stream")
}

func TestPrimaryReqConnectorSendTimeoutIncludesConnectionWait(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)
	nodes := createNodes(t, ca, 1)
	newUnresponsiveListener := func() net.Listener {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { listener.Close() })
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				_ = conn
			}
		}()
		return listener
	}
	primaryOne, primaryTwo := newUnresponsiveListener(), newUnresponsiveListener()

	conf := &node_config.BatcherNodeConfig{
		PartyId:            3,
		ShardId:            0,
		TLSPrivateKeyFile:  nodes[0].TLSKey,
		TLSCertificateFile: nodes[0].TLSCert,
	}
	batchers := map[types.PartyID]*batcher.EndpointAndCerts{
		1: {Endpoint: primaryOne.Addr().String(), TLSCACerts: []node_config.RawBytes{ca.CertBytes()}},
		2: {Endpoint: primaryTwo.Addr().String(), TLSCACerts: []node_config.RawBytes{ca.CertBytes()}},
	}
	connector := batcher.CreatePrimaryReqConnector(
		1, testutil.CreateLogger(t, 3), conf, batchers, context.Background(),
		100*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond,
	)
	t.Cleanup(connector.Stop)

	var sends sync.WaitGroup
	for range 4 {
		sends.Go(func() { connector.SendReq([]byte{1}) })
	}
	start := time.Now()
	connector.ConnectToNewPrimary(2)
	require.Less(t, time.Since(start), time.Second)
	requireDone(t, &sends, "senders did not honor the send timeout while connecting")
}

func newReqConnector(t *testing.T, nodes []*node, batchers map[types.PartyID]*batcher.EndpointAndCerts) *batcher.PrimaryReqConnector {
	conf := &node_config.BatcherNodeConfig{
		PartyId:            3,
		ShardId:            0,
		TLSPrivateKeyFile:  nodes[0].TLSKey,
		TLSCertificateFile: nodes[0].TLSCert,
	}
	connector := batcher.CreatePrimaryReqConnector(
		1, testutil.CreateLogger(t, 3), conf, batchers, context.Background(),
		5*time.Second, 100*time.Millisecond, time.Second,
	)
	t.Cleanup(connector.Stop)
	return connector
}

func requireDone(t *testing.T, wg *sync.WaitGroup, msg string) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal(msg)
	}
}
