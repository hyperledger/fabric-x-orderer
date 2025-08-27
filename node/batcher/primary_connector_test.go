/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/stretchr/testify/require"
)

func TestPrimaryConnector(t *testing.T) {
	shardID := types.ShardID(0)
	numParties := 4
	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	batcherNodes := createNodes(t, ca, numParties, "127.0.0.1:0")
	batchersInfo := createBatchersInfo(numParties, batcherNodes, ca)

	consenterNodes := createNodes(t, ca, numParties, "127.0.0.1:0")
	consentersInfo := createConsentersInfo(numParties, consenterNodes, ca)

	stubConsenters, clean := createConsenterStubs(t, consenterNodes, numParties)
	defer clean()

	batchers, loggers, configs, clean := createBatchers(t, numParties, shardID, batcherNodes, batchersInfo, consentersInfo, stubConsenters)
	defer clean()

	require.Equal(t, types.PartyID(1), batchers[1].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())

	connector := batcher.CreatePrimaryReqConnector(1, loggers[2], configs[2], batcher.GetBatchersEndpointsAndCerts(configs[2].Shards[0].Batchers), context.Background(), 10*time.Second, 100*time.Millisecond, 1*time.Second)
	connector.ConnectToPrimary()

	// send request via normal submit
	req := make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(1))
	batchers[0].Submit(context.Background(), &protos.Request{Payload: req})

	// make sure request was batched
	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(1) && batchers[2].Ledger.Height(1) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	// send request to primary via connector
	req = make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(2))
	connector.SendReq(req)

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
	req = make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(3))
	batchers[1].Submit(context.Background(), &protos.Request{Payload: req})

	// make sure request was batched
	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(2) == uint64(1) && batchers[2].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	// send request via the connector to a new primary
	req = make([]byte, 8)
	binary.BigEndian.PutUint64(req, uint64(4))
	connector.SendReq(req)

	// make sure request was batched
	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(2) == uint64(2) && batchers[2].Ledger.Height(2) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	connector.Stop()
}
