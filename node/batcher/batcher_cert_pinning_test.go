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
	"github.com/hyperledger/fabric-x-orderer/node/batcher"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil/pinning"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

// pinnedPartyID is the party of the batcher under test, and hence of the only router it serves.
const pinnedPartyID = types.PartyID(1)

// pinningTestSetup is a single batcher (the primary of a single party shard) along with the (cert,key)
// pair of the router of its party, which is the only client it should accept requests from.
type pinningTestSetup struct {
	batcher  *batcher.Batcher
	endpoint string
	ca       tlsgen.CA
	routerKP *tlsgen.CertKeyPair
}

func createPinningTestSetup(t *testing.T) *pinningTestSetup {
	shardID := types.ShardID(0)
	numParties := 1

	ca, err := tlsgen.NewCA()
	require.NoError(t, err)

	batcherNodes := createNodes(t, ca, numParties)
	batchersInfo := createBatchersInfo(numParties, batcherNodes, ca)
	consenterNodes := createNodes(t, ca, numParties)
	consentersInfo := createConsentersInfo(numParties, consenterNodes, ca)

	stubConsenters, cleanConsenters := createConsenterStubs(t, consenterNodes, numParties)
	t.Cleanup(cleanConsenters)

	routerKeyPairs := pinning.CreateRouterKeyPairs(t, ca, numParties)

	batchers, _, _, cleanBatchers := createBatchers(t, numParties, shardID, batcherNodes, batchersInfo, consentersInfo, routerKeyPairs, stubConsenters)
	t.Cleanup(cleanBatchers)

	return &pinningTestSetup{
		batcher:  batchers[0],
		endpoint: batcherNodes[0].Address(),
		ca:       ca,
		routerKP: routerKeyPairs[0],
	}
}

// dial opens a mutual TLS connection to the batcher, presenting clientKP as the client certificate.
func (s *pinningTestSetup) dial(t *testing.T, clientKP *tlsgen.CertKeyPair) protos.RequestTransmitClient {
	cc := comm.ClientConfig{
		AsyncConnect: false,
		DialTimeout:  20 * time.Second,
		KaOpts: comm.KeepaliveOptions{
			ClientInterval: 30 * time.Second,
			ClientTimeout:  30 * time.Second,
		},
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			RequireClientCert: true,
			ServerRootCAs:     [][]byte{s.ca.CertBytes()},
			Certificate:       clientKP.Cert,
			Key:               clientKP.Key,
		},
	}

	conn, err := cc.Dial(s.endpoint)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return protos.NewRequestTransmitClient(conn)
}

// RequestsReceived is the number of requests the batcher admitted, i.e. persisted in the batches of
// its own party. It tells apart a request that was merely answered from one that entered the pool.
func (s *pinningTestSetup) RequestsReceived() int {
	var total int
	for seq := range s.batcher.Ledger.Height(pinnedPartyID) {
		batch, err := s.batcher.Ledger.RetrieveBatchByNumber(pinnedPartyID, seq)
		if err != nil {
			// This helper is called from a require.Eventually closure, so it cannot fail the test
			// itself without calling FailNow off the test goroutine.
			panic(err)
		}
		total += len(batch.Requests())
	}
	return total
}

// submitStream opens a submit stream and sends a single request on it, waiting for the response.
// It returns the error of the first stream operation that failed, if any.
func submitStream(t *testing.T, client protos.RequestTransmitClient, req *protos.Request) error {
	stream, err := client.SubmitStream(context.Background())
	require.NoError(t, err) // opening the stream is local, a rejection only shows up on Send/Recv

	// the batcher only responds to requests that are traced
	req.TraceId = []byte("trace")

	if err := stream.Send(req); err != nil {
		return err
	}

	_, err = stream.Recv()
	return err
}

// Scenario:
// 1. start a batcher
// 2. connect to it with the TLS certificate of the router of that party
// 3. submit a request over Submit and a request over SubmitStream
// 4. verify both were accepted and made it into the batcher ledger
func TestBatcherAcceptsSubmitFromRouterOfOwnParty(t *testing.T) {
	setup := createPinningTestSetup(t)
	client := setup.dial(t, setup.routerKP)

	resp, err := client.Submit(context.Background(), tx.CreateStructuredRequest([]byte{1}))
	require.NoError(t, err)
	require.Empty(t, resp.Error)

	require.NoError(t, submitStream(t, client, tx.CreateStructuredRequest([]byte{2})))

	require.Eventually(t, func() bool {
		return setup.RequestsReceived() == 2
	}, 30*time.Second, 10*time.Millisecond)
}

// Scenario:
//  1. start a batcher
//  2. connect to it with a certificate issued by a CA the batcher trusts, that is not the certificate
//     of the router of its party - this is what another party of the shard holds
//  3. submit a request over Submit and a request over SubmitStream
//  4. verify both were rejected and that nothing was batched
func TestBatcherRejectsSubmitFromForeignCertificate(t *testing.T) {
	setup := createPinningTestSetup(t)

	// a certificate signed by a CA in the batcher's pool, so the TLS handshake succeeds
	foreignKP, err := setup.ca.NewClientCertKeyPair()
	require.NoError(t, err)

	client := setup.dial(t, foreignKP)

	_, err = client.Submit(context.Background(), tx.CreateStructuredRequest([]byte{1}))
	require.ErrorContains(t, err, "access denied; only the router of party 1 is served")
	require.ErrorContains(t, err, "does not match the expected certificate")

	err = submitStream(t, client, tx.CreateStructuredRequest([]byte{2}))
	require.ErrorContains(t, err, "access denied; only the router of party 1 is served")
	require.ErrorContains(t, err, "does not match the expected certificate")

	require.Zero(t, setup.RequestsReceived())
}

// Scenario:
//  1. start a batcher
//  2. call the Submit handler with a context that carries no TLS information, which is what a client
//     would produce were the batcher ever served without requiring a client certificate
//  3. verify the request was rejected and that nothing was batched
func TestBatcherRejectsSubmitWithoutClientCertificate(t *testing.T) {
	setup := createPinningTestSetup(t)

	_, err := setup.batcher.Submit(context.Background(), tx.CreateStructuredRequest([]byte{1}))
	require.ErrorContains(t, err, "access denied; only the router of party 1 is served")
	require.ErrorContains(t, err, "could not extract the client certificate from the context")

	require.Zero(t, setup.RequestsReceived())
}
