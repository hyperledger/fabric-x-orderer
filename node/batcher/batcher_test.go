/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/proto"
)

func TestBatcherRun(t *testing.T) {
	grpclog.SetLoggerV2(&testutil.SilentLogger{})

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

	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{1}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(1) && batchers[1].Ledger.Height(1) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return stubConsenters[0].BAFCount() == 1*numParties && stubConsenters[1].BAFCount() == 1*numParties
	}, 30*time.Second, 10*time.Millisecond)

	ce := stubConsenters[0].LastControlEvent()
	require.Equal(t, types.PartyID(1), ce.BAF.Primary())
	require.Equal(t, types.BatchSequence(0), ce.BAF.Seq())

	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{2}))

	require.Eventually(t, func() bool {
		return batchers[2].Ledger.Height(1) == uint64(2) && batchers[3].Ledger.Height(1) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return stubConsenters[2].BAFCount() == 2*numParties && stubConsenters[3].BAFCount() == 2*numParties
	}, 30*time.Second, 10*time.Millisecond)

	ce = stubConsenters[2].LastControlEvent()
	require.Equal(t, types.PartyID(1), ce.BAF.Primary())
	require.Equal(t, types.BatchSequence(1), ce.BAF.Seq())

	require.Equal(t, types.PartyID(1), batchers[0].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())
	stubConsenters[0].UpdateState(&state.State{N: uint16(numParties), Shards: []state.ShardTerm{{Shard: shardID, Term: 0}}})
	require.Equal(t, types.PartyID(1), batchers[0].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())

	termChangeState := &state.State{N: uint16(numParties), Shards: []state.ShardTerm{{Shard: shardID, Term: 1}}}

	for i := 0; i < numParties; i++ {
		stubConsenters[i].UpdateState(termChangeState)
	}
	require.Eventually(t, func() bool {
		return batchers[0].GetPrimaryID() == types.PartyID(2) && batchers[3].GetPrimaryID() == types.PartyID(2)
	}, 30*time.Second, 10*time.Millisecond)

	require.Equal(t, uint64(0), batchers[0].Ledger.Height(2))
	require.Equal(t, uint64(0), batchers[1].Ledger.Height(2))

	// stop and recover secondary
	t.Logf("Stop and recover secondary")
	batchers[3].Stop()
	batchers[3] = recoverBatcher(t, ca, loggers[3], configs[3], batcherNodes[3], stubConsenters[3])
	stubConsenters[3].UpdateState(termChangeState)

	batchers[1].Submit(context.Background(), tx.CreateStructuredRequest([]byte{3}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(2) == uint64(1) && batchers[1].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return batchers[3].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return stubConsenters[2].BAFCount() == 3*numParties && stubConsenters[3].BAFCount() == 3*numParties
	}, 30*time.Second, 10*time.Millisecond)

	for i := 0; i < numParties; i++ {
		require.Equal(t, types.PartyID(2), batchers[i].GetPrimaryID())
	}

	// stop and recover primary
	t.Logf("Stop and recover primary")
	batchers[1].Stop()
	time.Sleep(500 * time.Millisecond)
	batchers[1] = recoverBatcher(t, ca, loggers[1], configs[1], batcherNodes[1], stubConsenters[1])
	stubConsenters[1].UpdateState(termChangeState)

	batchers[1].Submit(context.Background(), tx.CreateStructuredRequest([]byte{4}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(2) == uint64(2) && batchers[1].Ledger.Height(2) == uint64(2) && batchers[3].Ledger.Height(2) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return stubConsenters[2].BAFCount() == 4*numParties && stubConsenters[3].BAFCount() == 4*numParties
	}, 30*time.Second, 10*time.Millisecond)

	for i := 0; i < numParties; i++ {
		require.Equal(t, types.PartyID(2), batchers[i].GetPrimaryID())
	}

	// stop secondary and recover after a batch
	batchers[2].Stop()

	batchers[1].Submit(context.Background(), tx.CreateStructuredRequest([]byte{5}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(2) == uint64(3) && batchers[1].Ledger.Height(2) == uint64(3) && batchers[3].Ledger.Height(2) == uint64(3)
	}, 30*time.Second, 10*time.Millisecond)

	// now recover the secondary
	batchers[2] = recoverBatcher(t, ca, loggers[2], configs[2], batcherNodes[2], stubConsenters[2])
	stubConsenters[2].UpdateState(termChangeState)
	require.Eventually(t, func() bool {
		return batchers[2].Ledger.Height(2) == uint64(3)
	}, 30*time.Second, 10*time.Millisecond)

	for i := 0; i < numParties; i++ {
		require.Equal(t, types.PartyID(2), batchers[i].GetPrimaryID())
	}

	// stop primary, change term, and recover after a batch
	batchers[1].Stop()

	termChangeAgainState := &state.State{N: uint16(numParties), Shards: []state.ShardTerm{{Shard: shardID, Term: 2}}}

	for i := 0; i < numParties; i++ {
		if i != 1 {
			stubConsenters[i].UpdateState(termChangeAgainState)
		}
	}
	require.Eventually(t, func() bool {
		return batchers[0].GetPrimaryID() == types.PartyID(3) && batchers[3].GetPrimaryID() == types.PartyID(3)
	}, 30*time.Second, 10*time.Millisecond)

	require.Equal(t, uint64(0), batchers[0].Ledger.Height(3))
	require.Equal(t, uint64(0), batchers[2].Ledger.Height(3))
	require.Equal(t, uint64(0), batchers[3].Ledger.Height(3))

	batchers[2].Submit(context.Background(), tx.CreateStructuredRequest([]byte{6}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(3) == uint64(1) && batchers[2].Ledger.Height(3) == uint64(1) && batchers[3].Ledger.Height(3) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	// now recover the previous primary
	batchers[1] = recoverBatcher(t, ca, loggers[1], configs[1], batcherNodes[1], stubConsenters[1])
	stubConsenters[1].UpdateState(termChangeAgainState)
	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(3) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)
}

func TestBatcherComplainAndReqFwd(t *testing.T) {
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

	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{1}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(1) && batchers[1].Ledger.Height(1) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return stubConsenters[1].BAFCount() == 1*numParties && stubConsenters[2].BAFCount() == 1*numParties
	}, 30*time.Second, 10*time.Millisecond)

	ce := stubConsenters[1].LastControlEvent()
	require.Equal(t, types.PartyID(1), ce.BAF.Primary())
	require.Equal(t, types.BatchSequence(0), ce.BAF.Seq())

	require.Equal(t, types.PartyID(1), batchers[1].GetPrimaryID())
	require.Equal(t, types.PartyID(1), batchers[2].GetPrimaryID())

	// stop the primary
	batchers[0].Stop()

	// submit request to other batchers
	req2 := tx.CreateStructuredRequest([]byte{2})
	batchers[1].Submit(context.Background(), req2)
	batchers[2].Submit(context.Background(), req2)

	// wait for complaints
	require.Eventually(t, func() bool {
		return stubConsenters[1].ComplaintCount() == 2 && stubConsenters[2].ComplaintCount() == 2
	}, 60*time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(0), stubConsenters[1].LastControlEvent().Complaint.Term)
	require.Equal(t, uint64(0), stubConsenters[2].LastControlEvent().Complaint.Term)

	require.Equal(t, numParties, stubConsenters[1].BAFCount())
	require.Equal(t, numParties, stubConsenters[2].BAFCount())

	// change term
	termChangeState := &state.State{N: uint16(numParties), Shards: []state.ShardTerm{{Shard: shardID, Term: 1}}}
	for i := 1; i < numParties; i++ {
		stubConsenters[i].UpdateState(termChangeState)
	}

	require.Eventually(t, func() bool {
		return batchers[1].GetPrimaryID() == types.PartyID(2) && batchers[2].GetPrimaryID() == types.PartyID(2)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(2) == uint64(1) && batchers[2].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	// make sure req2 did not disappear
	require.Equal(t, 1, len(batchers[1].Ledger.RetrieveBatchByNumber(2, 0).Requests()))
	rawReq, err := proto.Marshal(req2)
	require.NoError(t, err)
	require.Equal(t, rawReq, batchers[1].Ledger.RetrieveBatchByNumber(2, 0).Requests()[0])

	// now recover old primary
	batchers[0] = recoverBatcher(t, ca, loggers[0], configs[0], batcherNodes[0], stubConsenters[0])
	stubConsenters[0].UpdateState(termChangeState)
	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	for i := 0; i < numParties; i++ {
		require.Equal(t, types.PartyID(2), batchers[i].GetPrimaryID())
	}

	// submit another request only to a secondary
	batchers[2].Submit(context.Background(), tx.CreateStructuredRequest([]byte{3}))

	// after a timeout the request is forwarded
	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(2) == uint64(2) && batchers[2].Ledger.Height(2) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	// still same primary
	for i := 0; i < numParties; i++ {
		require.Equal(t, types.PartyID(2), batchers[i].GetPrimaryID())
	}
}

func TestControlEventBroadcasterWaitsForQuorum(t *testing.T) {
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

	batchers, _, _, clean := createBatchers(t, numParties, shardID, batcherNodes, batchersInfo, consentersInfo, stubConsenters)
	defer clean()

	// submit the first request and verify it was received
	// req := make([]byte, 8)
	// binary.BigEndian.PutUint64(req, uint64(1))
	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{1}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(1) && batchers[1].Ledger.Height(1) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return stubConsenters[1].BAFCount() == 1*numParties && stubConsenters[2].BAFCount() == 1*numParties
	}, 30*time.Second, 10*time.Millisecond)

	// stop one consenter – quorum (3/4) is still valid
	stubConsenters[0].StopNet()

	// submit the second request
	// req = make([]byte, 8)
	// binary.BigEndian.PutUint64(req, uint64(2))
	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{2}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(2) && batchers[1].Ledger.Height(1) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return stubConsenters[2].BAFCount() == 2*numParties && stubConsenters[3].BAFCount() == 2*numParties
	}, 30*time.Second, 10*time.Millisecond)

	// now stop another consenter – quorum (2/4) is not enough
	stubConsenters[1].StopNet()

	// submit another request, batch will be created but waiting for quorum
	// req = make([]byte, 8)
	// binary.BigEndian.PutUint64(req, uint64(3))
	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{3}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(3) && batchers[1].Ledger.Height(1) == uint64(3)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return stubConsenters[2].BAFCount() == 3*numParties && stubConsenters[3].BAFCount() == 3*numParties
	}, 30*time.Second, 10*time.Millisecond)

	// submit a fourth request – batcher should wait until the previous batch reaches quorum
	// req = make([]byte, 8)
	// binary.BigEndian.PutUint64(req, uint64(4))
	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{4}))

	time.Sleep(5 * time.Second)

	// verify the batcher did not create a new batch
	require.Equal(t, uint64(3), batchers[0].Ledger.Height(1))

	// recover one consenter – quorum (3/4) is available again
	stubConsenters[0].Restart()

	// verify the batcher created the fourth batch
	require.Eventually(t, func() bool {
		return stubConsenters[2].BAFCount() == 4*numParties && stubConsenters[3].BAFCount() == 4*numParties
	}, 30*time.Second, 10*time.Millisecond)

	for i := 0; i < numParties; i++ {
		require.Equal(t, uint64(4), batchers[i].Ledger.Height(1))
	}
}
