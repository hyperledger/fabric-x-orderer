package config

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouterLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "router-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	nodeLocalConfig := &NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    "5016",
			TLSConfig: TLSConfig{
				Enabled:            true,
				PrivateKey:         "path/to/pkey.key",
				Certificate:        "path/to/cert.crt",
				RootCAs:            []string{"path/to/root_cas.crt"},
				ClientAuthRequired: true,
				ClientRootCAs:      []string{"path/to/client_root_cas.crt"},
			},
			MaxRecvMsgSize: 123456789,
			MaxSendMsgSize: 6789,
			Bootstrap: Bootstrap{
				Method: "block",
				File:   "path/to/genesis-block",
			},
			Cluster: Cluster{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore: &FileStore{Path: "path/to/file_store"},
		RouterParams: RouterParams{
			NumberOfConnectionsPerBatcher: 10,
			NumberOfStreamsPerConnection:  20,
		},
	}

	path := path.Join(dir, "local_router_config.yaml")
	err = NodeConfigToYAML(nodeLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML NodeLocalConfig
	err = NodeConfigFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *nodeLocalConfig)
}

func TestBatcherLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "batcher-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	nodeLocalConfig := &NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    "5017",
			TLSConfig: TLSConfig{
				Enabled:            true,
				PrivateKey:         "path/to/pkey.key",
				Certificate:        "path/to/cert.crt",
				RootCAs:            []string{"path/to/root_cas.crt"},
				ClientAuthRequired: true,
				ClientRootCAs:      []string{"path/to/client_root_cas.crt"},
			},
			MaxRecvMsgSize: 123456789,
			MaxSendMsgSize: 6789,
			Bootstrap: Bootstrap{
				Method: "block",
				File:   "path/to/genesis-block",
			},
			Cluster: Cluster{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore:     &FileStore{Path: "path/to/file_store"},
		BatcherParams: BatcherParams{ShardID: 1},
	}

	path := path.Join(dir, "local_batcher_config.yaml")
	err = NodeConfigToYAML(nodeLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML NodeLocalConfig
	err = NodeConfigFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *nodeLocalConfig)
}

func TestConsensusLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "consensus-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	nodeLocalConfig := &NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    "5018",
			TLSConfig: TLSConfig{
				Enabled:            true,
				PrivateKey:         "path/to/pkey.key",
				Certificate:        "path/to/cert.crt",
				RootCAs:            []string{"path/to/root_cas.crt"},
				ClientAuthRequired: true,
				ClientRootCAs:      []string{"path/to/client_root_cas.crt"},
			},
			MaxRecvMsgSize: 123456789,
			MaxSendMsgSize: 6789,
			Bootstrap: Bootstrap{
				Method: "block",
				File:   "path/to/genesis-block",
			},
			Cluster: Cluster{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore:       &FileStore{Path: "path/to/file_store"},
		ConsensusParams: ConsensusParams{WALDir: "path/to/wal"},
	}

	path := path.Join(dir, "local_consensus_config.yaml")
	err = NodeConfigToYAML(nodeLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML NodeLocalConfig
	err = NodeConfigFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *nodeLocalConfig)
}

func TestAssemblerLocalConfigToYaml(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "assembler-local-config-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	nodeLocalConfig := &NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    "5019",
			TLSConfig: TLSConfig{
				Enabled:            true,
				PrivateKey:         "path/to/pkey.key",
				Certificate:        "path/to/cert.crt",
				RootCAs:            []string{"path/to/root_cas.crt"},
				ClientAuthRequired: true,
				ClientRootCAs:      []string{"path/to/client_root_cas.crt"},
			},
			MaxRecvMsgSize: 123456789,
			MaxSendMsgSize: 6789,
			Bootstrap: Bootstrap{
				Method: "block",
				File:   "path/to/genesis-block",
			},
			Cluster: Cluster{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore:       &FileStore{Path: "path/to/file_store"},
		AssemblerParams: AssemblerParams{PrefetchBufferMemoryMB: 10},
	}

	path := path.Join(dir, "local_assembler_config.yaml")
	err = NodeConfigToYAML(nodeLocalConfig, path)
	require.NoError(t, err)

	var nlcFromYAML NodeLocalConfig
	err = NodeConfigFromYAML(&nlcFromYAML, path)
	require.NoError(t, err)
	require.Equal(t, nlcFromYAML, *nodeLocalConfig)
}
