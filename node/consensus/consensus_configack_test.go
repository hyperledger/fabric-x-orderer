/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus_test

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	consensus_node "github.com/hyperledger/fabric-x-orderer/node/consensus"
	protos "github.com/hyperledger/fabric-x-orderer/node/protos/comm"
	node_utils "github.com/hyperledger/fabric-x-orderer/node/utils"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/hyperledger/fabric-x-orderer/testutil/configutil"
	"github.com/stretchr/testify/require"
)

// TestConsensusConfigAck verifies that a consenter applies a new config only after it has received
// valid config acks, through its AckConfig handler, from all the members of its own party.
//
// Scenario:
//  1. Start a 4-party, single-shard consensus network.
//  2. Submit a config update that is applied dynamically, and wait until every consenter has decided
//     it and soft stopped, waiting in ApplyConfig for config acks.
//  3. Send each consenter AckConfig requests it must reject: a nil request, a request without a client
//     certificate, an unsupported node type, an ack from another party's router, acks whose certificate
//     does not match the claimed role, and an ack from a batcher of an unknown shard. Then send valid
//     acks from the party's router and batcher, and verify a repeated ack is rejected.
//  4. Verify every consenter stays soft stopped: neither the partial acks nor the rejected ones release the wait.
//  5. Send the assembler's ack and verify every consenter applies the new config on sequence 1.
func TestConsensusConfigAck(t *testing.T) {
	parties := []types.PartyID{1, 2, 3, 4}
	numOfShards := 1

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	netInfo := testutil.CreateNetworkWithPortAllocator(t, configPath, len(parties), numOfShards, "TLS", "none", testutil.SharedTestPortAllocator())
	require.NotNil(t, netInfo)

	armageddon.NewCLI().Run([]string{"generate", "--config", configPath, "--output", dir})

	updateFileStoreAndMonitoringPort(t, dir, netInfo)

	netInfo.CleanUp()
	consensusNodes, servers, _ := createConsensusNodesAndGRPCServers(t, dir, parties)
	startConsensusNodesAndRegisterGRPCServers(t, parties, consensusNodes, servers)

	// submit a config update that the consenters apply dynamically, without an admin action.
	configUpdateBuilder := configutil.NewConfigUpdateBuilder(t, dir, filepath.Join(dir, "bootstrap", "bootstrap.block"))
	configUpdatePbData := configUpdateBuilder.UpdateSmartBFTConfig(t, configutil.NewSmartBFTConfig(configutil.SmartBFTConfigName.SyncOnStart, true))
	env := configutil.CreateConfigTX(t, dir, parties, 1, configUpdatePbData)
	configReq := &protos.Request{
		Payload:   env.Payload,
		Signature: env.Signature,
		ConfigSeq: uint32(0),
	}
	_, err := consensusNodes[0].SubmitConfig(nodeTLSContext(t, dir, parties[0], "router"), configReq)
	require.NoError(t, err)

	// every consenter decides the config block and soft stops, waiting for config acks.
	for _, node := range consensusNodes {
		waitForSoftStoppedState(t, node)
	}

	const configSeq = uint64(1)
	routerAck := &protos.ConfigAck{NodeType: protos.NodeType_ROUTER, ConfigSeq: configSeq}
	batcherAck := &protos.ConfigAck{NodeType: protos.NodeType_BATCHER, Shard: 1, ConfigSeq: configSeq}
	assemblerAck := &protos.ConfigAck{NodeType: protos.NodeType_ASSEMBLER, ConfigSeq: configSeq}
	unknownShardAck := &protos.ConfigAck{NodeType: protos.NodeType_BATCHER, Shard: uint32(numOfShards + 1), ConfigSeq: configSeq}
	unknownTypeAck := &protos.ConfigAck{NodeType: protos.NodeType(3), ConfigSeq: configSeq}

	for i, node := range consensusNodes {
		party := parties[i]
		routerCtx := nodeTLSContext(t, dir, party, "router")
		batcherCtx := nodeTLSContext(t, dir, party, "batcher1")
		assemblerCtx := nodeTLSContext(t, dir, party, "assembler")
		otherPartyRouterCtx := nodeTLSContext(t, dir, parties[(i+1)%len(parties)], "router")

		// the requests are sent in order; an empty expectedErr means the ack is accepted.
		requests := []struct {
			name        string
			ctx         context.Context
			req         *protos.ConfigAck
			expectedErr string
		}{
			{name: "nil request", ctx: routerCtx, req: nil, expectedErr: "nil ConfigAck request"},
			{name: "no client certificate", ctx: context.Background(), req: routerAck, expectedErr: "could not extract the client certificate from the context"},
			{name: "unsupported node type", ctx: routerCtx, req: unknownTypeAck, expectedErr: "unsupported config ack node type"},
			{name: "router of another party", ctx: otherPartyRouterCtx, req: routerAck, expectedErr: "only the router of the consenter's own party is served"},
			{name: "router posing as the assembler", ctx: routerCtx, req: assemblerAck, expectedErr: "only the assembler of the consenter's own party is served"},
			{name: "assembler posing as a batcher", ctx: assemblerCtx, req: batcherAck, expectedErr: "the client certificate does not match the expected certificate"},
			{name: "batcher of an unknown shard", ctx: batcherCtx, req: unknownShardAck, expectedErr: "no batcher configured for shard 2"},
			{name: "router", ctx: routerCtx, req: routerAck},
			{name: "batcher", ctx: batcherCtx, req: batcherAck},
			{name: "repeated router ack", ctx: routerCtx, req: routerAck, expectedErr: "the last acknowledged sequence is 1"},
		}

		for _, r := range requests {
			// AckConfig reports a rejected ack in the response, not as an RPC error.
			resp, err := node.AckConfig(r.ctx, r.req)
			require.NoError(t, err, "party %d: %s", party, r.name)
			require.NotNil(t, resp, "party %d: %s", party, r.name)
			if r.expectedErr == "" {
				require.Empty(t, resp.GetError(), "party %d: %s", party, r.name)
			} else {
				require.Contains(t, resp.GetError(), r.expectedErr, "party %d: %s", party, r.name)
			}
		}
	}

	// every consenter is still waiting for the ack of its assembler.
	require.Never(t, func() bool {
		for _, node := range consensusNodes {
			if node.GetStatus().State != node_utils.StateSoftStopped {
				return true
			}
		}
		return false
	}, 3*time.Second, 100*time.Millisecond, "a consenter applied the new config without an ack from its assembler")

	// the assembler's ack completes the set, and every consenter applies the new config.
	for i, node := range consensusNodes {
		resp, err := node.AckConfig(nodeTLSContext(t, dir, parties[i], "assembler"), assemblerAck)
		require.NoError(t, err)
		require.Empty(t, resp.GetError())
	}
	for i, node := range consensusNodes {
		waitForRunningState(t, node, configSeq)
		waitForConsensusServing(t, dir, parties[i], node)
	}

	for _, node := range consensusNodes {
		node.Stop()
	}
}

// nodeTLSContext returns a context that carries the TLS certificate of the given node of a party
// (e.g. "router", "assembler" or "batcher1"), as a consenter sees a request that node sends over mutual TLS.
func nodeTLSContext(t *testing.T, dir string, partyID types.PartyID, nodeName string) context.Context {
	certPath := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID), "orderers", fmt.Sprintf("party%d", partyID), nodeName, "tls", "server.crt")
	certBytes, err := os.ReadFile(certPath)
	require.NoError(t, err)
	block, _ := pem.Decode(certBytes)
	require.NotNil(t, block)
	require.Equal(t, "CERTIFICATE", block.Type)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	ctx, err := createContextForSubmitConfig(cert)
	require.NoError(t, err)
	return ctx
}

// waitForConsensusServing blocks until the consenter's gRPC server accepts mutual TLS connections.
// After a dynamic reconfiguration StartConsensusService launches srv.Start() in a goroutine and the
// state turns Running right after, so "Running" does not imply the server is serving; stopping the
// server before it starts serving makes that goroutine panic.
func waitForConsensusServing(t *testing.T, dir string, partyID types.PartyID, node *consensus_node.Consensus) {
	orgDir := filepath.Join(dir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", partyID))
	tlsCACert, err := os.ReadFile(filepath.Join(orgDir, "tlsca", fmt.Sprintf("tlsorg%d-CA-cert.pem", partyID)))
	require.NoError(t, err)
	routerTLSDir := filepath.Join(orgDir, "orderers", fmt.Sprintf("party%d", partyID), "router", "tls")
	routerCert, err := os.ReadFile(filepath.Join(routerTLSDir, "server.crt"))
	require.NoError(t, err)
	routerKey, err := os.ReadFile(filepath.Join(routerTLSDir, "server.key"))
	require.NoError(t, err)

	cc := comm.ClientConfig{
		SecOpts: comm.SecureOptions{
			UseTLS:            true,
			ServerRootCAs:     [][]byte{tlsCACert},
			Certificate:       routerCert,
			Key:               routerKey,
			RequireClientCert: true,
		},
		DialTimeout: time.Second,
	}
	require.Eventually(t, func() bool {
		conn, err := cc.Dial(node.Address())
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 10*time.Second, 100*time.Millisecond)
}
