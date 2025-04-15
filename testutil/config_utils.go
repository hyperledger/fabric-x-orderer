package testutil

import "github.ibm.com/decentralized-trust-research/arma/config"

func CreateTestRouterLocalConfig() *config.NodeLocalConfig {
	routerLocalConfig := &config.NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &config.GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    5016,
			TLSConfig: config.TLSConfigYaml{
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
			Cluster: config.ClusterYaml{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore: &config.FileStore{Path: "path/to/file_store"},
		RouterParams: &config.RouterParams{
			NumberOfConnectionsPerBatcher: 10,
			NumberOfStreamsPerConnection:  20,
		},
	}
	return routerLocalConfig
}

func CreateTestBatcherLocalConfig() *config.NodeLocalConfig {
	batcherLocalConfig := &config.NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &config.GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    5017,
			TLSConfig: config.TLSConfigYaml{
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
			Cluster: config.ClusterYaml{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore:     &config.FileStore{Path: "path/to/file_store"},
		BatcherParams: &config.BatcherParams{ShardID: 1},
	}
	return batcherLocalConfig
}

func CreateTestConsensusLocalConfig() *config.NodeLocalConfig {
	consensusLocalConfig := &config.NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &config.GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    5018,
			TLSConfig: config.TLSConfigYaml{
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
			Cluster: config.ClusterYaml{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore:       &config.FileStore{Path: "path/to/file_store"},
		ConsensusParams: &config.ConsensusParams{WALDir: "path/to/wal"},
	}
	return consensusLocalConfig
}

func CreateTestAssemblerLocalConfig() *config.NodeLocalConfig {
	assemblerLocalConfig := &config.NodeLocalConfig{
		PartyID: 1,
		GeneralConfig: &config.GeneralConfig{
			ListenAddress: "127.0.0.1",
			ListenPort:    5019,
			TLSConfig: config.TLSConfigYaml{
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
			Cluster: config.ClusterYaml{
				SendBufferSize:    0,
				ClientCertificate: "path/to/client_certificate.crt",
				ClientPrivateKey:  "path/to/client_private_key.key",
			},
			LogSpec: "info",
		},
		FileStore:       &config.FileStore{Path: "path/to/file_store"},
		AssemblerParams: &config.AssemblerParams{PrefetchBufferMemoryBytes: 10},
	}
	return assemblerLocalConfig
}
