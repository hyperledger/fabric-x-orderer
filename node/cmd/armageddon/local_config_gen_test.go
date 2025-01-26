package armageddon

import (
	"net"
	"os"
	"path"
	"testing"

	"arma/common/types"
	"arma/config"

	"github.com/stretchr/testify/require"
)

func TestRouterLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "router-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	nodeLocalConfig := &config.NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &config.GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    5016,
			TLSConfig: config.TLSConfig{
				Enabled:            true,
				PrivateKey:         "path/to/pkey.key",
				Certificate:        "path/to/cert.crt",
				RootCAs:            []string{"path/to/root_cas.crt"},
				ClientAuthRequired: true,
				ClientRootCAs:      []string{"path/to/client_root_cas.crt"},
			},
			MaxRecvMsgSize: 123456789,
			MaxSendMsgSize: 6789,
			Bootstrap: config.Bootstrap{
				Method: "block",
				File:   "path/to/genesis-block",
			},
			Cluster: config.Cluster{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore: &config.FileStore{Path: "path/to/file_store"},
		RouterParams: config.RouterParams{
			NumberOfConnectionsPerBatcher: 10,
			NumberOfStreamsPerConnection:  20,
		},
	}

	path := path.Join(dir, "local_router_config.yaml")
	err = NodeConfigToYAML(nodeLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = config.NodeConfigFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *nodeLocalConfig)
}

func TestBatcherLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "batcher-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	nodeLocalConfig := &config.NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &config.GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    5017,
			TLSConfig: config.TLSConfig{
				Enabled:            true,
				PrivateKey:         "path/to/pkey.key",
				Certificate:        "path/to/cert.crt",
				RootCAs:            []string{"path/to/root_cas.crt"},
				ClientAuthRequired: true,
				ClientRootCAs:      []string{"path/to/client_root_cas.crt"},
			},
			MaxRecvMsgSize: 123456789,
			MaxSendMsgSize: 6789,
			Bootstrap: config.Bootstrap{
				Method: "block",
				File:   "path/to/genesis-block",
			},
			Cluster: config.Cluster{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore:     &config.FileStore{Path: "path/to/file_store"},
		BatcherParams: config.BatcherParams{ShardID: 1},
	}

	path := path.Join(dir, "local_batcher_config.yaml")
	err = NodeConfigToYAML(nodeLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = config.NodeConfigFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *nodeLocalConfig)
}

func TestConsensusLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "consensus-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	nodeLocalConfig := &config.NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &config.GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    5018,
			TLSConfig: config.TLSConfig{
				Enabled:            true,
				PrivateKey:         "path/to/pkey.key",
				Certificate:        "path/to/cert.crt",
				RootCAs:            []string{"path/to/root_cas.crt"},
				ClientAuthRequired: true,
				ClientRootCAs:      []string{"path/to/client_root_cas.crt"},
			},
			MaxRecvMsgSize: 123456789,
			MaxSendMsgSize: 6789,
			Bootstrap: config.Bootstrap{
				Method: "block",
				File:   "path/to/genesis-block",
			},
			Cluster: config.Cluster{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore:       &config.FileStore{Path: "path/to/file_store"},
		ConsensusParams: config.ConsensusParams{WALDir: "path/to/wal"},
	}

	path := path.Join(dir, "local_consensus_config.yaml")
	err = NodeConfigToYAML(nodeLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = config.NodeConfigFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *nodeLocalConfig)
}

func TestAssemblerLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "assembler-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	nodeLocalConfig := &config.NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &config.GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    5019,
			TLSConfig: config.TLSConfig{
				Enabled:            true,
				PrivateKey:         "path/to/pkey.key",
				Certificate:        "path/to/cert.crt",
				RootCAs:            []string{"path/to/root_cas.crt"},
				ClientAuthRequired: true,
				ClientRootCAs:      []string{"path/to/client_root_cas.crt"},
			},
			MaxRecvMsgSize: 123456789,
			MaxSendMsgSize: 6789,
			Bootstrap: config.Bootstrap{
				Method: "block",
				File:   "path/to/genesis-block",
			},
			Cluster: config.Cluster{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore:       &config.FileStore{Path: "path/to/file_store"},
		AssemblerParams: config.AssemblerParams{PrefetchBufferMemoryMB: 10},
	}

	path := path.Join(dir, "local_assembler_config.yaml")
	err = NodeConfigToYAML(nodeLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML config.NodeLocalConfig
	err = config.NodeConfigFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *nodeLocalConfig)
}

func TestLocalConfigGeneration(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// 1.
	networkConfig := generateNetworkConfig(t)

	// 2.
	err = CreateArmaLocalConfig(networkConfig, dir)
	require.NoError(t, err)
}

// generateNetworkConfig create a network config which collects the enpoints of nodes per party.
// the generated network configuration includes 4 parties and 2 batchers for each party.
func generateNetworkConfig(t *testing.T) Network {
	var parties []Party
	var listeners []net.Listener
	for i := 0; i < 4; i++ {
		assemblerPort, lla := getAvailablePort(t)
		consenterPort, llc := getAvailablePort(t)
		routerPort, llr := getAvailablePort(t)
		batcher1Port, llb1 := getAvailablePort(t)
		batcher2Port, llb2 := getAvailablePort(t)

		party := Party{
			ID:                types.PartyID(i + 1),
			AssemblerEndpoint: "127.0.0.1:" + assemblerPort,
			ConsenterEndpoint: "127.0.0.1:" + consenterPort,
			RouterEndpoint:    "127.0.0.1:" + routerPort,
			BatchersEndpoints: []string{"127.0.0.1:" + batcher1Port, "127.0.0.1:" + batcher2Port},
		}

		parties = append(parties, party)
		listeners = append(listeners, lla, llc, llr, llb1, llb2)
	}

	network := Network{
		Parties: parties,
	}

	for _, ll := range listeners {
		require.NoError(t, ll.Close())
	}

	return network
}
