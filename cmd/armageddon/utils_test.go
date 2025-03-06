package armageddon_test

import (
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/common/utils"
	"github.ibm.com/decentralized-trust-research/arma/config"
	genconfig "github.ibm.com/decentralized-trust-research/arma/config/generate"
	nodeconfig "github.ibm.com/decentralized-trust-research/arma/node/config"
	"github.ibm.com/decentralized-trust-research/arma/testutil"
	"gopkg.in/yaml.v3"
)

// EditDirectoryInNodeConfigYAML fill the Directory field in all relevant config structures. This must be done before running Arma nodes
func EditDirectoryInNodeConfigYAML(t *testing.T, name string, path string) {
	dir, err := os.MkdirTemp("", "Directory_"+fmt.Sprint(name))
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	nodeConfig := readNodeConfigFromYaml(t, path)
	nodeConfig.FileStore.Path = dir
	err = nodeconfig.NodeConfigToYAML(nodeConfig, path)
	require.NoError(t, err)
}

func readNodeConfigFromYaml(t *testing.T, path string) *config.NodeLocalConfig {
	configBytes, err := os.ReadFile(path)
	require.NoError(t, err)
	config := config.NodeLocalConfig{}
	err = yaml.Unmarshal(configBytes, &config)
	require.NoError(t, err)
	return &config
}

// CreateNetwork creates a config.yaml file with the network configuration. This file is the input for armageddon generate command.
func CreateNetwork(t *testing.T, path string, numOfParties int, useTLSRouter string, useTLSAssembler string) map[string]net.Listener {
	var parties []genconfig.Party
	listeners := make(map[string]net.Listener)
	for i := 0; i < numOfParties; i++ {
		assemblerPort, lla := testutil.GetAvailablePort(t)
		consenterPort, llc := testutil.GetAvailablePort(t)
		routerPort, llr := testutil.GetAvailablePort(t)
		batcher1Port, llb1 := testutil.GetAvailablePort(t)
		batcher2Port, llb2 := testutil.GetAvailablePort(t)

		party := genconfig.Party{
			ID:                types.PartyID(i + 1),
			AssemblerEndpoint: "127.0.0.1:" + assemblerPort,
			ConsenterEndpoint: "127.0.0.1:" + consenterPort,
			RouterEndpoint:    "127.0.0.1:" + routerPort,
			BatchersEndpoints: []string{"127.0.0.1:" + batcher1Port, "127.0.0.1:" + batcher2Port},
		}

		parties = append(parties, party)
		listeners[fmt.Sprintf("Party%drouter", i+1)] = llr
		listeners[fmt.Sprintf("Party%dbatcher1", i+1)] = llb1
		listeners[fmt.Sprintf("Party%dbatcher2", i+1)] = llb2
		listeners[fmt.Sprintf("Party%dconsensus", i+1)] = llc
		listeners[fmt.Sprintf("Party%dassembler", i+1)] = lla
	}

	network := genconfig.Network{
		Parties:         parties,
		UseTLSRouter:    useTLSRouter,
		UseTLSAssembler: useTLSAssembler,
	}

	err := utils.WriteToYAML(network, path)
	require.NoError(t, err)

	return listeners
}
