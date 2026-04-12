/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"fmt"
	"net"
	"os"
	"path"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
)

const (
	TerminationGracePeriod = 10 * time.Second
)

type ArmaNetwork struct {
	armaNodes map[NodeType][][]*ArmaNodeInfo
}

type ArmaNodeRunInfo struct {
	ArmaBinaryPath string
	NodeConfigPath string
	Session        *gexec.Session
}

type ArmaNodesInfoMap map[NodeName]*ArmaNodeInfo

func NewArmaNodesInfoMap() *ArmaNodesInfoMap {
	m := make(ArmaNodesInfoMap)
	return &m
}

func (armaNodesInfoMap *ArmaNodesInfoMap) CleanUp() {
	for _, node := range *armaNodesInfoMap {
		node.Close()
	}
}

type ArmaNodeInfo struct {
	RunInfo            *ArmaNodeRunInfo
	NodeType           NodeType
	Listener           net.Listener
	PartyId            types.PartyID
	ShardId            types.ShardID
	ConfigBlockPath    string
	MonitoringListener net.Listener
}

func (armaNodeInfo *ArmaNodeInfo) Close() {
	if armaNodeInfo.Listener != nil {
		_ = armaNodeInfo.Listener.Close()
	}
	if armaNodeInfo.MonitoringListener != nil {
		_ = armaNodeInfo.MonitoringListener.Close()
	}
}

func (armaNetwork *ArmaNetwork) AddArmaNode(nodeType NodeType, partyIdx int, nodeInfo *ArmaNodeInfo) {
	if nodeType == Batcher && len(armaNetwork.armaNodes[Batcher]) > partyIdx {
		armaNetwork.armaNodes[Batcher][partyIdx] = append(armaNetwork.armaNodes[Batcher][partyIdx], nodeInfo)
	} else {
		armaNetwork.armaNodes[nodeType] = append(armaNetwork.armaNodes[nodeType], []*ArmaNodeInfo{nodeInfo})
	}
}

func (armaNetwork *ArmaNetwork) Stop() {
	for _, k := range []NodeType{Assembler, Consensus, Batcher, Router} {
		for i := range armaNetwork.armaNodes[k] {
			for j := range armaNetwork.armaNodes[k][i] {
				armaNetwork.armaNodes[k][i][j].StopArmaNode()
			}
		}
	}
}

func (armaNetwork *ArmaNetwork) Kill() {
	for _, k := range []NodeType{Assembler, Consensus, Batcher, Router} {
		for i := range armaNetwork.armaNodes[k] {
			for j := range armaNetwork.armaNodes[k][i] {
				armaNetwork.armaNodes[k][i][j].KillArmaNode()
			}
		}
	}
}

func (armaNetwork *ArmaNetwork) AddAndStartParty(t *testing.T, dir string, armaBinaryPath string, readyChan chan string, addedNetInfo map[NodeName]*ArmaNodeInfo) {
	nodes := map[NodeType]string{
		Router:    "local_config_router",
		Batcher:   "local_config_batcher",
		Consensus: "local_config_consenter",
		Assembler: "local_config_assembler",
	}

	nodeInfos := make([]*ArmaNodeInfo, 0, len(addedNetInfo))
	for n := range addedNetInfo {
		nodeInfos = append(nodeInfos, addedNetInfo[n])
	}

	sort.Slice(nodeInfos, sortArmaNodeInfo(nodeInfos))

	for _, netNode := range nodeInfos {
		shardId := ""
		if netNode.ShardId != 0 {
			shardId = strconv.FormatUint(uint64(netNode.ShardId), 10)
		}

		partyId := fmt.Sprintf("party%d", netNode.PartyId)

		partyDir := path.Join(dir, "config", partyId)
		nodeConfigPath := path.Join(partyDir, nodes[netNode.NodeType]+shardId+".yaml")

		storagePath := path.Join(dir, "storage", partyId, fmt.Sprintf("%s%s", netNode.NodeType.String(), shardId))
		err := os.MkdirAll(storagePath, 0o755)
		require.NoError(t, err)

		EditDirectoryInNodeConfigYAML(t, nodeConfigPath, storagePath, netNode.ConfigBlockPath, uint32(netNode.MonitoringListener.Addr().(*net.TCPAddr).Port))
		netNode.RunInfo = &ArmaNodeRunInfo{ArmaBinaryPath: armaBinaryPath, NodeConfigPath: nodeConfigPath}
		netNode.RunInfo.Session = runNode(t, netNode, readyChan)
		require.NotNil(t, netNode.RunInfo.Session, fmt.Sprintf("failed to start Arma node %s", netNode.NodeType.String()))
		armaNetwork.AddArmaNode(netNode.NodeType, int(netNode.PartyId)-1, netNode)
	}
}

func (armaNetwork *ArmaNetwork) Restart(t *testing.T, readyChan chan string) {
	for _, k := range []NodeType{Assembler, Consensus, Batcher, Router} {
		for i := range armaNetwork.armaNodes[k] {
			for j := range armaNetwork.armaNodes[k][i] {
				armaNetwork.armaNodes[k][i][j].RestartArmaNode(t, readyChan)
			}
		}
	}
}

func (armaNetwork *ArmaNetwork) StopParties(parties []types.PartyID) {
	for _, k := range []NodeType{Assembler, Consensus, Batcher, Router} {
		for _, partyID := range parties {
			for j := range armaNetwork.armaNodes[k][partyID-1] {
				armaNetwork.armaNodes[k][partyID-1][j].StopArmaNode()
			}
		}
	}
}

func (armaNetwork *ArmaNetwork) RestartParties(t *testing.T, parties []types.PartyID, readyChan chan string) {
	for _, k := range []NodeType{Assembler, Consensus, Batcher, Router} {
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

	armaNodeInfo.RunInfo.Session = runNode(t, armaNodeInfo, readyChan)
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
