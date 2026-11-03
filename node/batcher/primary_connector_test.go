/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/pinning"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestPrimaryConnector(t *testing.T) {
	shardID := types.ShardID(0)
	numParties := 4
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	batcherNodes := createNodes(t, ca, numParties)
	batchersInfo := createBatchersInfo(numParties, batcherNodes, ca)

	consenterNodes := createNodes(t, ca, numParties)
	consentersInfo := createConsentersInfo(numParties, consenterNodes, ca)

	stubConsenters, clean := createConsenterStubs(t, consenterNodes, numParties)
	defer clean()

	routerKeyPairs := pinning.CreateRouterKeyPairs(t, ca, numParties)
	batchers, loggers, configs, clean := createBatchers(t, numParties, shardID, batcherNodes, batchersInfo, consentersInfo, routerKeyPairs, stubConsenters)
	defer clean()

	require.Equal(t, types.PartyID(1), batchers[1].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())

	connector := batcher.CreatePrimaryReqConnector(1, loggers[2], configs[2], batcher.GetBatchersEndpointsAndCerts(configs[2].Shards[0].Batchers), context.Background(), 10*time.Second, 100*time.Millisecond, 1*time.Second)
	connector.ConnectToPrimary()

	// send request via normal submit
	batchers[0].Submit(routerContext(t, routerKeyPairs[0]), tx.CreateStructuredRequest([]byte{1}))

	// make sure request was batched
	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(1) && batchers[2].Ledger.Height(1) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	// send request to primary via connector
	req2, _ := proto.Marshal(tx.CreateStructuredRequest([]byte{2}))
	connector.SendReq(req2)

	// make sure request was batched
	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(2) && batchers[2].Ledger.Height(1) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	require.Equal(t, types.PartyID(1), batchers[1].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())

	// stop the primary
	batchers[0].Stop()

	// change term
	termChangeState := &state.State{N: uint16(numParties), Shards: []state.ShardTerm{{Shard: shardID, Term: 1}}}
	for i := 1; i < numParties; i++ {
		stubConsenters[i].UpdateState(termChangeState)
	}

	require.Eventually(t, func() bool {
		return batchers[1].GetPrimaryID() == types.PartyID(2) && batchers[2].GetPrimaryID() == types.PartyID(2)
	}, 30*time.Second, 10*time.Millisecond)

	// update the connector
	connector.ConnectToNewPrimary(2)

	// send request via normal submit
	batchers[1].Submit(routerContext(t, routerKeyPairs[1]), tx.CreateStructuredRequest([]byte{3}))

	// make sure request was batched
	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(2) == uint64(1) && batchers[2].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	// send request via the connector to a new primary
	req4, _ := proto.Marshal(tx.CreateStructuredRequest([]byte{4}))
	connector.SendReq(req4)

	// make sure request was batched
	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(2) == uint64(2) && batchers[2].Ledger.Height(2) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	connector.Stop()
}

// fakePrimary is a batcher control service. Unless it reads, it accepts the streams of forwarded
// requests and acks and never reads from them. It counts the streams it accepts and the requests it reads.
type fakePrimary struct {
	protos.UnimplementedBatcherControlServiceServer
	reads    bool
	accepted chan struct{}
	streams  atomic.Int32
	requests atomic.Int32
}

func newFakePrimary(reads bool) *fakePrimary {
	return &fakePrimary{reads: reads, accepted: make(chan struct{}, 1000)}
}

func (fp *fakePrimary) FwdRequestStream(stream protos.BatcherControlService_FwdRequestStreamServer) error {
	fp.streams.Add(1)
	fp.accepted <- struct{}{}
	if !fp.reads {
		<-stream.Context().Done()
		return nil
	}
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
		fp.requests.Add(1)
	}
}

func (fp *fakePrimary) NotifyAck(stream protos.BatcherControlService_NotifyAckServer) error {
	fp.streams.Add(1)
	fp.accepted <- struct{}{}
	if !fp.reads {
		<-stream.Context().Done()
		return nil
	}
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
	}
}

// serveFakePrimaries serves fake primary i on node i of the shard, and returns the shard's batchers.
func serveFakePrimaries(
	t *testing.T, ca tlsgen.CA, primaries ...*fakePrimary,
) ([]*node, map[types.PartyID]*batcher.EndpointAndCerts) {
	nodes := createNodes(t, ca, len(primaries))
	for i, primary := range primaries {
		protos.RegisterBatcherControlServiceServer(nodes[i].Server(), primary)
		go func() { _ = nodes[i].Start() }()
		t.Cleanup(nodes[i].Stop)
	}
	return nodes, batcher.GetBatchersEndpointsAndCerts(createBatchersInfo(len(primaries), nodes, ca))
}

// waitUntilSendsBlock waits until the count of returned sends stops growing while some are still blocked,
// which shows that the stream to the primary is full.
func waitUntilSendsBlock(t *testing.T, returned *atomic.Int32, senders int32) {
	last := int32(-1)
	require.Eventually(t, func() bool {
		n := returned.Load()
		full := n == last && n < senders
		last = n
		return full
	}, 10*time.Second, 500*time.Millisecond)
}

// Scenario:
//  1. Start primary 1, which accepts the stream of forwarded requests but never reads from it, and
//     primary 2, which reads it.
//  2. Forward requests to primary 1 from many goroutines, until the sends block because the stream is full.
//  3. Switch the connector to primary 2.
//  4. Expect the switch to return, the blocked sends to return, and requests to reach primary 2.
func TestPrimaryReqConnectorSwitchesPrimaryWhileASendIsBlocked(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	stalled, reading := newFakePrimary(false), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, stalled, reading)
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
	connector.ConnectToPrimary()
	<-stalled.accepted

	const senders = 64
	request := make([]byte, 64*1024)
	var sends sync.WaitGroup
	var returned atomic.Int32
	for range senders {
		sends.Go(func() {
			connector.SendReq(request)
			returned.Add(1)
		})
	}
	waitUntilSendsBlock(t, &returned, senders)

	switched := make(chan struct{})
	go func() {
		connector.ConnectToNewPrimary(types.PartyID(2))
		close(switched)
	}()
	select {
	case <-switched:
	case <-time.After(10 * time.Second):
		t.Fatal("the connector did not switch to the new primary while a send was blocked")
	}

	sent := make(chan struct{})
	go func() {
		sends.Wait()
		close(sent)
	}()
	select {
	case <-sent:
	case <-time.After(30 * time.Second):
		t.Fatal("the sends blocked on the old primary did not return")
	}
	require.Positive(t, reading.requests.Load())
}

// Scenario:
//  1. Start primary 1, which accepts the stream of acks but never reads from it, and this batcher, party 2,
//     which reads it.
//  2. Ack batches to primary 1 one after the other, as a secondary does, until an ack blocks because the
//     stream is full.
//  3. Switch the connector to this batcher, which is now the primary.
//  4. Expect the switch to return, the blocked ack to return, and no ack stream opened to this batcher.
func TestPrimaryAckConnectorDropsAcksToTheOldPrimaryWhenBecomingPrimary(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	stalled, self := newFakePrimary(false), newFakePrimary(true)
	nodes, batchers := serveFakePrimaries(t, ca, stalled, self)
	conf := &node_config.BatcherNodeConfig{
		PartyId:            2,
		ShardId:            0,
		TLSPrivateKeyFile:  nodes[1].TLSKey,
		TLSCertificateFile: nodes[1].TLSCert,
	}
	connector := batcher.CreatePrimaryAckConnector(
		1, 0, testutil.CreateLogger(t, 2), conf, batchers, context.Background(),
		5*time.Second, 100*time.Millisecond, 500*time.Millisecond,
	)
	t.Cleanup(connector.Stop)
	connector.ConnectToPrimary()
	<-stalled.accepted

	// One sender acks batch after batch to primary 1, as a secondary does, and counts the acks that
	// returned. It keeps acking after the switch, until stopped.
	var returned atomic.Int32
	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := types.BatchSequence(0); !stop.Load(); seq++ {
			connector.SendAck(seq, types.PartyID(1))
			returned.Add(1)
		}
	}()
	waitUntilSendsBlock(t, &returned, math.MaxInt32)
	blocked := returned.Load()

	switched := make(chan struct{})
	go func() {
		connector.ConnectToNewPrimary(types.PartyID(2))
		close(switched)
	}()
	select {
	case <-switched:
	case <-time.After(10 * time.Second):
		t.Fatal("the connector did not switch to this batcher while an ack was blocked")
	}

	require.Eventually(t, func() bool { return returned.Load() > blocked }, 10*time.Second, 10*time.Millisecond,
		"the ack blocked on the old primary did not return")
	require.Never(t, func() bool { return self.streams.Load() > 0 }, 2*time.Second, 50*time.Millisecond,
		"an ack stream was opened to this batcher")
	stop.Store(true)
	<-done
}
