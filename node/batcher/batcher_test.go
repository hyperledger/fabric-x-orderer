/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher_test

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/block"
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

func TestRunBatchersGetMetrics(t *testing.T) {
	shardID := types.ShardID(1)
	numParties := 1
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

	totalTxNumber := 10
	url := batchers[0].MonitoringServiceAddress()

	for range totalTxNumber {
		batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{byte(64)}))
	}

	pattern := fmt.Sprintf(`batcher_router_txs_total\{party_id="%d",shard_id="%d"\} \d+`, types.PartyID(1), types.ShardID(1))
	re := regexp.MustCompile(pattern)

	require.Eventually(t, func() bool {
		return testutil.FetchPrometheusMetricValue(t, re, url) == totalTxNumber
	}, 30*time.Second, 100*time.Millisecond)
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
	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{3}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(3) && batchers[1].Ledger.Height(1) == uint64(3)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return stubConsenters[2].BAFCount() == 3*numParties && stubConsenters[3].BAFCount() == 3*numParties
	}, 30*time.Second, 10*time.Millisecond)

	// submit a fourth request – batcher should wait until the previous batch reaches quorum
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

	for i := range numParties {
		require.Equal(t, uint64(4), batchers[i].Ledger.Height(1))
	}

	// now check if term change can occur while waiting for send baf

	// stop another consenter – quorum (2/4) is not enough
	stubConsenters[3].StopNet()

	// submit another request, batch will be created but waiting for quorum
	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{5}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(5) && batchers[1].Ledger.Height(1) == uint64(5)
	}, 30*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return stubConsenters[2].BAFCount() == 5*numParties
	}, 30*time.Second, 10*time.Millisecond)

	// submit another request – batcher should wait until the previous batch reaches quorum
	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{6}))

	time.Sleep(5 * time.Second)

	// verify the batcher did not create a new batch
	require.Equal(t, uint64(5), batchers[0].Ledger.Height(1))

	for i := range numParties {
		require.Equal(t, types.PartyID(1), batchers[i].GetPrimaryID())
		require.Equal(t, uint64(0), batchers[i].Ledger.Height(2))
	}

	// change term
	termChangeState := &state.State{N: uint16(numParties), Shards: []state.ShardTerm{{Shard: shardID, Term: 1}}}
	stubConsenters[0].UpdateState(termChangeState)
	stubConsenters[2].UpdateState(termChangeState)

	require.Eventually(t, func() bool {
		return batchers[0].GetPrimaryID() == types.PartyID(2) && batchers[2].GetPrimaryID() == types.PartyID(2)
	}, 30*time.Second, 10*time.Millisecond)

	// recover all stub consenters
	stubConsenters[3].Restart()
	stubConsenters[1].Restart()

	stubConsenters[1].UpdateState(termChangeState)
	stubConsenters[3].UpdateState(termChangeState)

	require.Eventually(t, func() bool {
		return batchers[1].GetPrimaryID() == types.PartyID(2) && batchers[3].GetPrimaryID() == types.PartyID(2)
	}, 30*time.Second, 10*time.Millisecond)

	// the baf was not sent and remained in the mem pool, after a first strike the new primary will include it in a new batch
	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(2) == uint64(1) && batchers[1].Ledger.Height(2) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)
}

func TestBatchedRequestsHasEnvelopeBytes(t *testing.T) {
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

	req := tx.CreateStructuredRequest([]byte{1})
	req.TraceId = []byte("123")
	req.Identity = []byte("123")
	req.IdentityId = 123

	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{1}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	rawReq := batchers[0].Ledger.RetrieveBatchByNumber(1, 0).Requests()[0]

	// Unmarshal as Envelope, and check that there are no unknown fields
	env := &common.Envelope{}
	err = proto.Unmarshal(rawReq, env)
	require.NoError(t, err)
	require.Zero(t, len(env.ProtoReflect().GetUnknown()))

	require.True(t, bytes.Equal(req.Payload, env.Payload))
	require.True(t, bytes.Equal(req.Signature, env.Signature))
}

func TestBatcherReceivesConfigBlockFromConsensus(t *testing.T) {
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

	for i := 0; i < numParties; i++ {
		blocks, err := batchers[i].ConfigStore.ListBlockNumbers()
		require.NoError(t, err)
		require.Equal(t, len(blocks), 0)
	}

	// receive config block from consensus
	groups := &common.ConfigGroup{}
	configBlock := block.BlockWithGroups(groups, "arma", 1)
	availableCommonBlocks := []*common.Block{configBlock}

	st := &state.State{N: uint16(numParties), Shards: []state.ShardTerm{{Shard: shardID, Term: 0}}}

	for i := 0; i < numParties; i++ {
		stubConsenters[i].UpdateStateHeaderWithConfigBlock(types.DecisionNum(1), availableCommonBlocks, st)
	}

	for j := 0; j < numParties; j++ {
		require.Eventually(t, func() bool {
			block, err1 := batchers[j].ConfigStore.Last()
			blockNumbers, err2 := batchers[j].ConfigStore.ListBlockNumbers()
			return err1 == nil && err2 == nil && block.Header.Number == uint64(1) && len(blockNumbers) == 1
		}, 60*time.Second, 10*time.Millisecond)
	}
}

func TestPullBatchFromSoftStoppedBatcher(t *testing.T) {
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

	// submit the first request and verify it was received
	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{1}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(1) && batchers[1].Ledger.Height(1) == uint64(1)
	}, 30*time.Second, 10*time.Millisecond)

	// stopped batcher 2
	batchers[1].Stop()

	// submit another request
	batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{2}))

	require.Eventually(t, func() bool {
		return batchers[0].Ledger.Height(1) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	// soft-stop all batchers except batcher 2
	for i, b := range batchers {
		if i != 1 {
			b.SoftStop()
		}
	}

	// recover batcher 2
	batchers[1] = recoverBatcher(t, ca, loggers[1], configs[1], batcherNodes[1], stubConsenters[1])

	// batcher 2 should pull the missing batch
	require.Eventually(t, func() bool {
		return batchers[1].Ledger.Height(1) == uint64(2)
	}, 30*time.Second, 10*time.Millisecond)

	// submit another request during soft-stop, should fail
	_, err = batchers[0].Submit(context.Background(), tx.CreateStructuredRequest([]byte{3}))
	require.Error(t, err)
}
