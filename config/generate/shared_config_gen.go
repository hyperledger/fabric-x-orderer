package generate

import (
	"fmt"
	"os"
	"path/filepath"

	"arma/common/utils"

	"arma/core"

	"github.com/hyperledger-labs/SmartBFT/pkg/types"
)

const (
	DefaultMaxMessageCount   = 1000
	DefaultAbsoluteMaxBytes  = 10 * 1024 * 1024
	DefaultPreferredMaxBytes = 2 * 1024 * 1024
	LocalConfigDirPermission = 0o755
)

func CreateArmaSharedConfig(network Network, networkLocalConfig *NetworkLocalConfig, cryptoBaseDir string, outputBaseDir string) (*SharedConfig, error) {
	sharedConfig := createNetworkSharedConfig(network, networkLocalConfig, cryptoBaseDir)

	outputPath := filepath.Join(outputBaseDir, "bootstrap")
	err := os.MkdirAll(outputPath, LocalConfigDirPermission)
	if err != nil {
		return nil, err
	}

	err = utils.WriteToYAML(&sharedConfig, filepath.Join(outputPath, "shared_config.yaml"))
	if err != nil {
		return nil, err
	}
	return &sharedConfig, nil
}

func createNetworkSharedConfig(network Network, networkLocalConfig *NetworkLocalConfig, cryptoBaseDir string) SharedConfig {
	sharedConfig := SharedConfig{
		PartiesConfig:   createPartiesConfig(network, networkLocalConfig, cryptoBaseDir),
		ConsensusConfig: ConsensusConfig{BFTConfig: createConsensusBFTConfig()},
		BatchingConfig:  createBatchingConfig(),
	}
	return sharedConfig
}

func createConsensusBFTConfig() SmartBFTConfig {
	smartBFTConfig := SmartBFTConfig{
		RequestBatchMaxInterval:   types.DefaultConfig.RequestBatchMaxInterval,
		RequestForwardTimeout:     types.DefaultConfig.RequestForwardTimeout,
		RequestComplainTimeout:    types.DefaultConfig.RequestComplainTimeout,
		RequestAutoRemoveTimeout:  types.DefaultConfig.RequestAutoRemoveTimeout,
		ViewChangeResendInterval:  types.DefaultConfig.ViewChangeResendInterval,
		ViewChangeTimeout:         types.DefaultConfig.ViewChangeTimeout,
		LeaderHeartbeatTimeout:    types.DefaultConfig.LeaderHeartbeatTimeout,
		CollectTimeout:            types.DefaultConfig.CollectTimeout,
		IncomingMessageBufferSize: types.DefaultConfig.IncomingMessageBufferSize,
		RequestPoolSize:           types.DefaultConfig.RequestPoolSize,
		LeaderHeartbeatCount:      types.DefaultConfig.LeaderHeartbeatCount,
	}
	return smartBFTConfig
}

func createBatchingConfig() BatchingConfig {
	return BatchingConfig{
		BatchTimeout: core.DefaultBatchTimeout,
		BatchSize: BatchSize{
			MaxMessageCount:   DefaultMaxMessageCount,
			AbsoluteMaxBytes:  DefaultAbsoluteMaxBytes,
			PreferredMaxBytes: DefaultPreferredMaxBytes,
		},
	}
}

func createPartiesConfig(network Network, networkLocalConfig *NetworkLocalConfig, cryptoBaseDir string) []PartyConfig {
	partiesConfig := make([]PartyConfig, len(network.Parties))

	for i, party := range network.Parties {
		partyPath := filepath.Join(cryptoBaseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "orderers", fmt.Sprintf("party%d", party.ID))

		partyLocalConfig := networkLocalConfig.PartiesLocalConfig[i]

		routerConfig := RouterNodeConfig{
			Host:    partyLocalConfig.RouterLocalConfig.GeneralConfig.ListenAddress,
			Port:    partyLocalConfig.RouterLocalConfig.GeneralConfig.ListenPort,
			TLSCert: partyLocalConfig.RouterLocalConfig.GeneralConfig.TLSConfig.Certificate,
		}

		var batchersConfig []BatcherNodeConfig
		for j := range party.BatchersEndpoints {
			batcherConfig := BatcherNodeConfig{
				ShardID:   partyLocalConfig.BatchersLocalConfig[j].BatcherParams.ShardID,
				Host:      partyLocalConfig.BatchersLocalConfig[j].GeneralConfig.ListenAddress,
				Port:      partyLocalConfig.BatchersLocalConfig[j].GeneralConfig.ListenPort,
				PublicKey: filepath.Join(partyPath, fmt.Sprintf("batcher%d", j+1), "signing-cert.pem"),
				TLSCert:   partyLocalConfig.BatchersLocalConfig[j].GeneralConfig.TLSConfig.Certificate,
			}
			batchersConfig = append(batchersConfig, batcherConfig)
		}

		consenterConfig := ConsenterNodeConfig{
			Host:      partyLocalConfig.ConsenterLocalConfig.GeneralConfig.ListenAddress,
			Port:      partyLocalConfig.ConsenterLocalConfig.GeneralConfig.ListenPort,
			PublicKey: filepath.Join(partyPath, "consenter", "signing-cert.pem"),
			TLSCert:   partyLocalConfig.ConsenterLocalConfig.GeneralConfig.TLSConfig.Certificate,
		}

		assemblerConfig := AssemblerNodeConfig{
			Host:    partyLocalConfig.AssemblerLocalConfig.GeneralConfig.ListenAddress,
			Port:    partyLocalConfig.AssemblerLocalConfig.GeneralConfig.ListenPort,
			TLSCert: partyLocalConfig.AssemblerLocalConfig.GeneralConfig.TLSConfig.Certificate,
		}

		orgDir := filepath.Join(cryptoBaseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID))
		partyConfig := PartyConfig{
			PartyID:         party.ID,
			CACerts:         []string{filepath.Join(orgDir, "ca", "ca-cert.pem")},
			TLSCACerts:      []string{filepath.Join(orgDir, "tlsca", "tlsca-cert.pem")},
			RouterConfig:    routerConfig,
			BatchersConfig:  batchersConfig,
			ConsenterConfig: consenterConfig,
			AssemblerConfig: assemblerConfig,
		}

		partiesConfig[i] = partyConfig
	}

	return partiesConfig
}
