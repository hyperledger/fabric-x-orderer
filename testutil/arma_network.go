/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
)

const (
	TerminationGracePeriod = 10 * time.Second
)

// var NodeSortingTable = map[string]int{Router: 0, Batcher: 1, Consensus: 2, Assembler: 3}

type ArmaNetwork struct {
	armaNodes map[string][][]*ArmaNodeInfo
}

type ArmaNodeRunInfo struct {
	ArmaBinaryPath string
	NodeConfigPath string
	Session        *gexec.Session
}

type ArmaNodeInfo struct {
	RunInfo  *ArmaNodeRunInfo
	NodeType string
	Listener net.Listener
	PartyId  types.PartyID
	ShardId  types.ShardID
}

func (armaNetwork *ArmaNetwork) AddArmaNode(nodeType string, partyIdx int, nodeInfo *ArmaNodeInfo) {
	if nodeType == "batcher" && len(armaNetwork.armaNodes["batcher"]) > partyIdx {
		armaNetwork.armaNodes["batcher"][partyIdx] = append(armaNetwork.armaNodes["batcher"][partyIdx], nodeInfo)
	} else {
		armaNetwork.armaNodes[nodeType] = append(armaNetwork.armaNodes[nodeType], []*ArmaNodeInfo{nodeInfo})
	}
}

func (armaNetwork *ArmaNetwork) Stop() {
	for _, k := range []string{Assembler, Consensus, Batcher, Router} {
		for i := range armaNetwork.armaNodes[k] {
			for j := range armaNetwork.armaNodes[k][i] {
				armaNetwork.armaNodes[k][i][j].StopArmaNode()
			}
		}
	}
}

func (armaNetwork *ArmaNetwork) Kill() {
	for _, k := range []string{Assembler, Consensus, Batcher, Router} {
		for i := range armaNetwork.armaNodes[k] {
			for j := range armaNetwork.armaNodes[k][i] {
				armaNetwork.armaNodes[k][i][j].KillArmaNode()
			}
		}
	}
}

func (armaNetwork *ArmaNetwork) Restart(t *testing.T, readyChan chan string) {
	for _, k := range []string{Assembler, Consensus, Batcher, Router} {
		for i := range armaNetwork.armaNodes[k] {
			for j := range armaNetwork.armaNodes[k][i] {
				armaNetwork.armaNodes[k][i][j].RestartArmaNode(t, readyChan)
			}
		}
	}
}

func (armaNetwork *ArmaNetwork) StopParties(parties []types.PartyID) {
	for _, k := range []string{Assembler, Consensus, Batcher, Router} {
		for _, partyID := range parties {
			for j := range armaNetwork.armaNodes[k][partyID-1] {
				armaNetwork.armaNodes[k][partyID-1][j].StopArmaNode()
			}
		}
	}
}

func (armaNetwork *ArmaNetwork) RestartParties(t *testing.T, parties []types.PartyID, readyChan chan string) {
	for _, k := range []string{Assembler, Consensus, Batcher, Router} {
		for _, partyID := range parties {
			for j := range armaNetwork.armaNodes[k][partyID-1] {
				armaNetwork.armaNodes[k][partyID-1][j].RestartArmaNode(t, readyChan)
			}
		}
	}
}

func (armaNetwork *ArmaNetwork) GetAssembler(t *testing.T, partyID types.PartyID) *ArmaNodeInfo {
	require.True(t, int(partyID) > 0)
	require.True(t, len(armaNetwork.armaNodes[Assembler]) >= int(partyID))
	return armaNetwork.armaNodes[Assembler][partyID-1][0]
}

func (armaNetwork *ArmaNetwork) GetRouter(t *testing.T, partyID types.PartyID) *ArmaNodeInfo {
	require.True(t, int(partyID) > 0)
	require.True(t, len(armaNetwork.armaNodes[Router]) >= int(partyID))
	return armaNetwork.armaNodes[Router][partyID-1][0]
}

func (armaNetwork *ArmaNetwork) GetConsenter(t *testing.T, partyID types.PartyID) *ArmaNodeInfo {
	require.True(t, int(partyID) > 0)
	require.True(t, len(armaNetwork.armaNodes[Consensus]) >= int(partyID))
	return armaNetwork.armaNodes[Consensus][partyID-1][0]
}

func (armaNetwork *ArmaNetwork) GetBatcher(t *testing.T, partyID types.PartyID, shardID types.ShardID) *ArmaNodeInfo {
	require.True(t, int(partyID) > 0)
	require.True(t, int(shardID) > 0)
	require.True(t, len(armaNetwork.armaNodes[Batcher]) >= int(partyID))
	require.True(t, len(armaNetwork.armaNodes[Batcher][partyID-1]) >= int(shardID))
	return armaNetwork.armaNodes[Batcher][partyID-1][shardID-1]
}

func (armaNodeInfo *ArmaNodeInfo) RestartArmaNode(t *testing.T, readyChan chan string) {
	require.FileExists(t, armaNodeInfo.RunInfo.NodeConfigPath)
	nodeConfig := ReadNodeConfigFromYaml(t, armaNodeInfo.RunInfo.NodeConfigPath)
	storagePath := nodeConfig.FileStore.Path
	require.DirExists(t, storagePath)

	armaNodeInfo.RunInfo.Session = runNode(t, armaNodeInfo.NodeType, armaNodeInfo.RunInfo.ArmaBinaryPath,
		armaNodeInfo.RunInfo.NodeConfigPath, readyChan, armaNodeInfo.Listener)
}

func (armaNodeInfo *ArmaNodeInfo) StopArmaNode() {
	select {
	case <-armaNodeInfo.RunInfo.Session.Terminate().Exited:
	case <-time.After(TerminationGracePeriod):
		fmt.Fprintf(os.Stderr, "Graceful shutdown: timeout expired Party%d%s@%s is about to be killed", armaNodeInfo.PartyId, armaNodeInfo.NodeType, armaNodeInfo.Listener.Addr())
		<-armaNodeInfo.RunInfo.Session.Kill().Exited
	}
}

func (armaNodeInfo *ArmaNodeInfo) KillArmaNode() {
	<-armaNodeInfo.RunInfo.Session.Kill().Exited
}
