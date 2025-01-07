package consensus_test

import (
	"testing"
	"time"

	"arma/core"
	"arma/node/batcher"
	"arma/node/comm/tlsgen"
	"arma/node/consensus"
	"arma/node/ledger"
	"arma/testutil"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/grpclog"
)

var (
	digest    = make([]byte, 32-3)
	digest123 = append([]byte{1, 2, 3}, digest...)
	digest124 = append([]byte{1, 2, 4}, digest...)
	digest125 = append([]byte{1, 2, 5}, digest...)
)

func TestCreateOneConsensusNode(t *testing.T) {
	grpclog.SetLoggerV2(&testutil.SilentLogger{})

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	setup := setupConsensusTest(t, ca, 1)

	c := setup.consensusNodes[0]
	listener := setup.listeners[0]

	baf123id1p1s1, err := batcher.CreateBAF(setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	assert.NoError(t, err)
	ce := &core.ControlEvent{BAF: baf123id1p1s1}

	err = c.SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b := <-listener.c
	require.Equal(t, uint64(1), b.Header.Number)

	baf124id1p1s2, err := batcher.CreateBAF(setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf124id1p1s2}

	err = c.SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b = <-listener.c
	require.Equal(t, uint64(2), b.Header.Number)

	c.Stop()

	setup.consenterNodes[0].GRPCServer, err = newGRPCServer(setup.consenterNodes[0].Address(), ca, &tlsgen.CertKeyPair{Key: setup.consenterNodes[0].TLSKey, Cert: setup.consenterNodes[0].TLSCert})
	require.NoError(t, err)

	c = consensus.CreateConsensus(setup.configs[0], setup.loggers[0])
	grpcRegisterAndStart(c, setup.consenterNodes[0])

	c.Storage.(*ledger.ConsensusLedger).RegisterAppendListener(listener)

	err = c.Start()
	require.NoError(t, err)

	baf125id1p1s2, err := batcher.CreateBAF(setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf125id1p1s2}

	err = c.SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b = <-listener.c
	require.Equal(t, uint64(3), b.Header.Number)

	c.Stop()
}

// This test starts a cluster of 4 consensus nodes.
// Sending a couple of requests and waiting for 2 blocks to be written to the ledger.
// Then taking down the leader node (ID=1).
// The rest of the nodes get a new request and we make sure another block is committed (a view change occurs).
// Then we restart the stopped node, send 2 more requests, and we make sure the nodes commit 2 more blocks.
// Finally, we make sure the restarted node is synced with all blocks.
func TestCreateMultipleConsensusNodes(t *testing.T) {
	// grpclog.SetLoggerV2(&testutil.SilentLogger{})
	parties := 4

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	setup := setupConsensusTest(t, ca, parties)

	baf123id1p1s1, err := batcher.CreateBAF(setup.batcherNodes[0].sk, 1, 1, digest123, 1, 1)
	assert.NoError(t, err)
	ce := &core.ControlEvent{BAF: baf123id1p1s1}

	err = setup.consensusNodes[0].SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b := <-setup.listeners[0].c
	require.Equal(t, uint64(1), b.Header.Number)
	b1 := <-setup.listeners[1].c
	require.Equal(t, uint64(1), b1.Header.Number)

	baf124id1p1s2, err := batcher.CreateBAF(setup.batcherNodes[0].sk, 1, 1, digest124, 1, 2)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf124id1p1s2}

	err = setup.consensusNodes[0].SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(2), b.Header.Number)
	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(2), b1.Header.Number)

	setup.consensusNodes[0].Stop()

	time.Sleep(1 * time.Second)

	baf125id1p1s1, err := batcher.CreateBAF(setup.batcherNodes[0].sk, 1, 1, digest125, 1, 3)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf125id1p1s1}

	err = setup.consensusNodes[1].SubmitRequest(ce.Bytes())
	require.NoError(t, err)
	err = setup.consensusNodes[2].SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(3), b1.Header.Number)

	setup.consensusNodes[0] = consensus.CreateConsensus(setup.configs[0], setup.loggers[0])
	newConsenterNode := &node{TLSCert: setup.consenterNodes[0].TLSCert, TLSKey: setup.consenterNodes[0].TLSKey, sk: setup.consenterNodes[0].sk, pk: setup.consenterNodes[0].pk}
	newConsenterNode.GRPCServer, err = newGRPCServer(setup.consenterNodes[0].Address(), ca, &tlsgen.CertKeyPair{Key: setup.consenterNodes[0].TLSKey, Cert: setup.consenterNodes[0].TLSCert})
	require.NoError(t, err)
	grpcRegisterAndStart(setup.consensusNodes[0], newConsenterNode)

	newListener := &storageListener{c: make(chan *common.Block, 100)}
	setup.consensusNodes[0].Storage.(*ledger.ConsensusLedger).RegisterAppendListener(newListener)
	setup.listeners[0] = newListener
	err = setup.consensusNodes[0].Start()
	require.NoError(t, err)

	time.Sleep(10 * time.Second)

	baf125id2p1s1, err := batcher.CreateBAF(setup.batcherNodes[1].sk, 2, 1, digest125, 1, 3)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf125id2p1s1}

	err = setup.consensusNodes[1].SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(4), b1.Header.Number)

	baf123id2p1s1, err := batcher.CreateBAF(setup.batcherNodes[1].sk, 2, 1, digest123, 1, 1)
	assert.NoError(t, err)
	ce = &core.ControlEvent{BAF: baf123id2p1s1}

	err = setup.consensusNodes[1].SubmitRequest(ce.Bytes())
	require.NoError(t, err)

	b1 = <-setup.listeners[1].c
	require.Equal(t, uint64(5), b1.Header.Number)

	b = <-setup.listeners[0].c
	require.Equal(t, uint64(3), b.Header.Number)
	b = <-setup.listeners[0].c
	require.Equal(t, uint64(4), b.Header.Number)
	b = <-setup.listeners[0].c
	require.Equal(t, uint64(5), b.Header.Number)

	for _, c := range setup.consensusNodes {
		c.Stop()
	}
}
