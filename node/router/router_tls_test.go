package router_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/node/comm"
	"github.ibm.com/decentralized-trust-research/arma/node/comm/tlsgen"
)

// Scenario:
// 1. start a TLS secured router that doesn't require a secured client and 2 stub batchers
// 2. start a no TLS client using the same CA router uses
// 3. submit 2 requests by client to router
// 4. check that the batchers received all the requests
func TestServerTLSClientConnectionToRouter(t *testing.T) {
	numOfShards := 2
	testSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards, true, false)
	defer testSetup.Close()

	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	err = submitBroadcastRequests(testSetup.clientConn, 2)
	require.NoError(t, err)

	recvCond := func() uint32 {
		receivedTxCount := uint32(0)
		for i := 0; i < numOfShards; i++ {
			receivedTxCount += testSetup.batchers[i].ReceivedMessageCount()
		}
		return receivedTxCount
	}

	require.Eventually(t, func() bool {
		return recvCond() == uint32(2)
	}, 60*time.Second, 10*time.Millisecond)
}

// Scenario:
// 1. start a TLS secured router that doesn't require a secured client and 2 stub batchers
// 2. create another CA
// 3. start a no TLS client using the CA from above
// 4. verify an error occurred
func TestServerTLSClientConnectionToRouterWithUnrecognizedCA(t *testing.T) {
	numOfShards := 2
	testSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards, true, false)
	defer testSetup.Close()

	clientCA, err := tlsgen.NewCA()
	require.NoError(t, err)

	err = createServerTLSClientConnection(testSetup, clientCA)
	require.Error(t, err)
	require.Nil(t, testSetup.clientConn)
}

// Scenario:
// 1. start a no TLS router that doesn't require a secured client and 2 stub batchers
// 2. start a TLS client
// 4. verify an error occurred
func TestServerTLSClientConnectionToNoTLSRouter(t *testing.T) {
	numOfShards := 2
	testSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards, false, false)
	defer testSetup.Close()

	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.Error(t, err)
	require.Nil(t, testSetup.clientConn)
}

// Scenario:
// 1. start a TLS secured router that requires a secured client and 2 stub batchers
// 2. start a TLS secured client using the same CA router uses
// 3. submit 2 requests by client to router
// 4. check that the batchers received all the requests
func TestMutualTLSClientConnectionToRouter(t *testing.T) {
	numOfShards := 2
	testSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards, true, true)
	defer testSetup.Close()

	err := createMutualTLSClientConnection(t, testSetup, testSetup.ca)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	err = submitBroadcastRequests(testSetup.clientConn, 2)
	require.NoError(t, err)

	recvCond := func() uint32 {
		receivedTxCount := uint32(0)
		for i := 0; i < numOfShards; i++ {
			receivedTxCount += testSetup.batchers[i].ReceivedMessageCount()
		}
		return receivedTxCount
	}

	require.Eventually(t, func() bool {
		return recvCond() == uint32(2)
	}, 60*time.Second, 10*time.Millisecond)
}

// Scenario:
// 1. start a no TLS router
// 2. start a TLS secured client using the same CA router uses
// 3. verify an error occurred
func TestMutualTLSClientConnectionToNoTLSRouter(t *testing.T) {
	numOfShards := 2
	testSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards, false, true)
	defer testSetup.Close()

	err := createMutualTLSClientConnection(t, testSetup, testSetup.ca)
	require.Error(t, err)
	require.Nil(t, testSetup.clientConn)
}

// Scenario:
// 1. start a TLS secured router that requires a secured client and 2 stub batchers
// 2. create another CA
// 3. start a TLS secured client using the CA from above
// 4. verify an error occurred
func TestMutualTLSClientConnectionToRouterWithUnrecognizedCA(t *testing.T) {
	numOfShards := 2
	testSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards, true, true)
	defer testSetup.Close()

	clientCA, err := tlsgen.NewCA()
	require.NoError(t, err)

	err = createMutualTLSClientConnection(t, testSetup, clientCA)
	require.Error(t, err)
	require.Nil(t, testSetup.clientConn)
}

// Scenario:
// 1. start a TLS secured router that requires a secured client and 2 stub batchers
// 2. start a TLS secured client without client creds
// 4. verify an error occurred
func TestMutualTLSClientConnectionToRouterWithNoClientCreds(t *testing.T) {
	numOfShards := 2
	testSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards, true, true)
	defer testSetup.Close()

	err := createServerTLSClientConnection(testSetup, testSetup.ca)
	require.Error(t, err)
	require.Nil(t, testSetup.clientConn)
}

// Scenario:
// 1. start a TLS secured router that requires a secured client and 2 stub batchers
// 2. start an unsecured client
// 3. verify an error occurred
func TestMutualTLSClientConnectionToRouterWithUnsecuredClient(t *testing.T) {
	numOfShards := 2
	testSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards, true, true)
	defer testSetup.Close()

	err := createNoTLSConnection(testSetup)
	require.Error(t, err)
	require.Nil(t, testSetup.clientConn)
}

// Scenario:
// 1. start a no TLS router and 2 stub batchers
// 2. start a no TLS client
// 3. submit 2 requests by client to router
// 4. check that the batchers received all the requests
func TestNoTLSClientConnectionToRouter(t *testing.T) {
	numOfShards := 2
	testSetup := createRouterTestSetup(t, types.PartyID(1), numOfShards, false, false)
	defer testSetup.Close()

	err := createNoTLSConnection(testSetup)
	require.NoError(t, err)
	require.NotNil(t, testSetup.clientConn)

	err = submitBroadcastRequests(testSetup.clientConn, 2)
	require.NoError(t, err)

	recvCond := func() uint32 {
		receivedTxCount := uint32(0)
		for i := 0; i < numOfShards; i++ {
			receivedTxCount += testSetup.batchers[i].ReceivedMessageCount()
		}
		return receivedTxCount
	}

	require.Eventually(t, func() bool {
		return recvCond() == uint32(2)
	}, 60*time.Second, 10*time.Millisecond)
}

func createNoTLSConnection(testSetup *routerTestSetup) error {
	cc := comm.ClientConfig{
		SecOpts: comm.SecureOptions{
			UseTLS: false,
		},
		DialTimeout:  time.Second,
		AsyncConnect: false,
	}

	var err error
	testSetup.clientConn, err = cc.Dial(testSetup.router.Address())

	return err
}

func createMutualTLSClientConnection(t *testing.T, testSetup *routerTestSetup, ca tlsgen.CA) error {
	ckp, err := ca.NewClientCertKeyPair()
	require.NoError(t, err)

	cc := comm.ClientConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			RequireClientCert: true,
			Certificate:       ckp.Cert,
			Key:               ckp.Key,
			ServerRootCAs:     [][]byte{ca.CertBytes()},
		},
		DialTimeout:  time.Second,
		AsyncConnect: false,
	}

	testSetup.clientConn, err = cc.Dial(testSetup.router.Address())

	return err
}
