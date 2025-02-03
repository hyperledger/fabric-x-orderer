package generate_test

import (
	"net"
	"os"
	"path"
	"path/filepath"
	"testing"

	"arma/common/utils"

	"arma/common/types"
	"arma/config"
	"arma/config/generate"
	"arma/testutil"

	"github.com/stretchr/testify/require"
)

func TestRouterLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "router-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	routerLocalConfig := testutil.CreateTestRouterLocalConfig()

	path := path.Join(dir, "local_router_config.yaml")
	err = utils.WriteToYAML(routerLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = utils.ReadFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *routerLocalConfig)
}

func TestBatcherLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "batcher-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	batcherLocalConfig := testutil.CreateTestBatcherLocalConfig()
	path := path.Join(dir, "local_batcher_config.yaml")
	err = utils.WriteToYAML(batcherLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = utils.ReadFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *batcherLocalConfig)
}

func TestConsensusLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "consensus-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	consensusLocalConfig := testutil.CreateTestConsensusLocalConfig()

	path := path.Join(dir, "local_consensus_config.yaml")
	err = utils.WriteToYAML(consensusLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = utils.ReadFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *consensusLocalConfig)
}

func TestAssemblerLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "assembler-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	assemblerLocalConfig := testutil.CreateTestAssemblerLocalConfig()

	path := path.Join(dir, "local_assembler_config.yaml")
	err = utils.WriteToYAML(assemblerLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = utils.ReadFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *assemblerLocalConfig)
}

func TestARMALocalConfigGeneration(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	networkConfig := generateNetworkConfig(t)

	// 2.
	networkLocalConfig, err := generate.CreateArmaLocalConfig(networkConfig, dir, dir)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)
}

func TestARMALocalConfigLoading(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	networkConfig := generateNetworkConfig(t)

	networkLocalConfig, err := generate.CreateArmaLocalConfig(networkConfig, dir, dir)
	require.NoError(t, err)
	require.NotNil(t, networkLocalConfig)

	configDir := filepath.Join(dir, "/config")
	res, err := generate.LoadArmaLocalConfig(configDir)
	require.NoError(t, err)
	require.NotNil(t, res)
	for i, party := range res.PartiesLocalConfig {
		require.Equal(t, networkLocalConfig.PartiesLocalConfig[i].RouterLocalConfig, party.RouterLocalConfig)
		for j, batcher := range party.BatchersLocalConfig {
			require.Equal(t, batcher, party.BatchersLocalConfig[j])
		}
		require.Equal(t, networkLocalConfig.PartiesLocalConfig[i].ConsenterLocalConfig, party.ConsenterLocalConfig)
		require.Equal(t, networkLocalConfig.PartiesLocalConfig[i].AssemblerLocalConfig, party.AssemblerLocalConfig)
	}
}

// generateNetworkConfig create a network config which collects the enpoints of nodes per party.
// the generated network configuration includes 4 parties and 2 batchers for each party.
func generateNetworkConfig(t *testing.T) generate.Network {
	var parties []generate.Party
	var listeners []net.Listener
	for i := 0; i < 4; i++ {
		assemblerPort, lla := testutil.GetAvailablePort(t)
		consenterPort, llc := testutil.GetAvailablePort(t)
		routerPort, llr := testutil.GetAvailablePort(t)
		batcher1Port, llb1 := testutil.GetAvailablePort(t)
		batcher2Port, llb2 := testutil.GetAvailablePort(t)

		party := generate.Party{
			ID:                types.PartyID(i + 1),
			AssemblerEndpoint: "127.0.0.1:" + assemblerPort,
			ConsenterEndpoint: "127.0.0.1:" + consenterPort,
			RouterEndpoint:    "127.0.0.1:" + routerPort,
			BatchersEndpoints: []string{"127.0.0.1:" + batcher1Port, "127.0.0.1:" + batcher2Port},
		}

		parties = append(parties, party)
		listeners = append(listeners, lla, llc, llr, llb1, llb2)
	}

	network := generate.Network{
		Parties: parties,
	}

	for _, ll := range listeners {
		require.NoError(t, ll.Close())
	}

	return network
}
