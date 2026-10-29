/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	node_config "github.com/hyperledger/fabric-x-orderer/node/config"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

// stalledPrimary accepts a stream of forwarded requests and never reads from it.
type stalledPrimary struct {
	protos.UnimplementedBatcherControlServiceServer
	accepted chan struct{}
}

func (sp *stalledPrimary) FwdRequestStream(stream protos.BatcherControlService_FwdRequestStreamServer) error {
	sp.accepted <- struct{}{}
	<-stream.Context().Done()
	return nil
}

// Scenario:
//  1. Start a primary that accepts the stream of forwarded requests but never reads from it.
//  2. Forward requests to it from many goroutines, until the sends block because the stream is full.
//  3. Switch the connector to another primary.
//  4. Expect the switch to return, and the blocked sends to return.
func TestPrimaryReqConnectorSwitchesPrimaryWhileASendIsBlocked(t *testing.T) {
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	nodes := createNodes(t, ca, 2)
	primary := &stalledPrimary{accepted: make(chan struct{}, 10)}
	protos.RegisterBatcherControlServiceServer(nodes[0].Server(), primary)
	go func() { _ = nodes[0].Start() }()
	t.Cleanup(nodes[0].Stop)

	batchersInfo := createBatchersInfo(2, nodes, ca)
	conf := &node_config.BatcherNodeConfig{
		PartyId:            2,
		ShardId:            0,
		TLSPrivateKeyFile:  nodes[1].TLSKey,
		TLSCertificateFile: nodes[1].TLSCert,
	}
	connector := batcher.CreatePrimaryReqConnector(
		1, testutil.CreateLogger(t, 2), conf, batcher.GetBatchersEndpointsAndCerts(batchersInfo),
		context.Background(), 5*time.Second, 100*time.Millisecond, time.Second,
	)
	t.Cleanup(connector.Stop)
	connector.ConnectToPrimary()
	<-primary.accepted

	request := make([]byte, 64*1024)
	var sends sync.WaitGroup
	for range 64 {
		sends.Go(func() { connector.SendReq(request) })
	}
	// Let the sends fill the stream's flow-control window, so the rest block in Send.
	time.Sleep(2 * time.Second)

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
}
