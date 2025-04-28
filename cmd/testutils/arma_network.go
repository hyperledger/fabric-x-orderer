package testutils

import (
	"net"
	"testing"

	"github.com/onsi/gomega/gexec"
	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
)

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
	for k := range armaNetwork.armaNodes {
		for i := range armaNetwork.armaNodes[k] {
			for j := range armaNetwork.armaNodes[k][i] {
				armaNetwork.armaNodes[k][i][j].StopArmaNode()
			}
		}
	}
}

func (armaNetwork *ArmaNetwork) GetAssembler(t *testing.T, partyID types.PartyID) *ArmaNodeInfo {
	require.True(t, int(partyID) > 0)
	require.True(t, len(armaNetwork.armaNodes["assembler"]) >= int(partyID))
	return armaNetwork.armaNodes["assembler"][partyID-1][0]
}

func (armaNetwork *ArmaNetwork) GetRouter(t *testing.T, partyID types.PartyID) *ArmaNodeInfo {
	require.True(t, int(partyID) > 0)
	require.True(t, len(armaNetwork.armaNodes["router"]) >= int(partyID))
	return armaNetwork.armaNodes["router"][partyID-1][0]
}

func (armaNetwork *ArmaNetwork) GetConsenter(t *testing.T, partyID types.PartyID) *ArmaNodeInfo {
	require.True(t, int(partyID) > 0)
	require.True(t, len(armaNetwork.armaNodes["consensus"]) >= int(partyID))
	return armaNetwork.armaNodes["consensus"][partyID-1][0]
}

func (armaNetwork *ArmaNetwork) GeBatcher(t *testing.T, partyID types.PartyID, shardID types.ShardID) *ArmaNodeInfo {
	require.True(t, int(partyID) > 0)
	require.True(t, int(shardID) > 0)
	require.True(t, len(armaNetwork.armaNodes["batcher"]) >= int(partyID))
	require.True(t, len(armaNetwork.armaNodes["batcher"][partyID-1]) >= int(shardID))
	return armaNetwork.armaNodes["batcher"][partyID-1][shardID-1]
}

func (armaNodeInfo *ArmaNodeInfo) RestartArmaNode(t *testing.T, readyChan chan struct{}) {
	require.FileExists(t, armaNodeInfo.RunInfo.NodeConfigPath)
	nodeConfig := readNodeConfigFromYaml(t, armaNodeInfo.RunInfo.NodeConfigPath)
	storagePath := nodeConfig.FileStore.Path
	require.DirExists(t, storagePath)

	armaNodeInfo.RunInfo.Session = runNode(t, armaNodeInfo.NodeType, armaNodeInfo.RunInfo.ArmaBinaryPath,
		armaNodeInfo.RunInfo.NodeConfigPath, readyChan, armaNodeInfo.Listener)
}

func (armaNodeInfo *ArmaNodeInfo) StopArmaNode() {
	<-armaNodeInfo.RunInfo.Session.Kill().Exited
}
