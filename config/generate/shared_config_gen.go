package generate

import (
	"fmt"
	"os"
	"path/filepath"

	"github.ibm.com/decentralized-trust-research/arma/common/utils"
	"github.ibm.com/decentralized-trust-research/arma/config"
)

// CreateArmaSharedConfig creates a bootstrap directory that includes the shared config yaml file.
func CreateArmaSharedConfig(network Network, networkLocalConfig *NetworkLocalConfig, cryptoBaseDir string, outputBaseDir string) (*config.SharedConfigYaml, error) {
	sharedConfig := createNetworkSharedConfig(network, networkLocalConfig, cryptoBaseDir)

	outputPath := filepath.Join(outputBaseDir, "bootstrap")
	err := os.MkdirAll(outputPath, 0o755)
	if err != nil {
		return nil, err
	}

	err = utils.WriteToYAML(&sharedConfig, filepath.Join(outputPath, "shared_config.yaml"))
	if err != nil {
		return nil, err
	}
	return &sharedConfig, nil
}

func createNetworkSharedConfig(network Network, networkLocalConfig *NetworkLocalConfig, cryptoBaseDir string) config.SharedConfigYaml {
	sharedConfig := config.SharedConfigYaml{
		PartiesConfig:   createPartiesConfig(network, networkLocalConfig, cryptoBaseDir),
		ConsensusConfig: config.ConsensusConfig{BFTConfig: config.DefaultArmaBFTConfig()},
		BatchingConfig:  createBatchingConfig(),
	}
	return sharedConfig
}

func createBatchingConfig() config.BatchingConfig {
	return config.DefaultBatchingConfig
}

func createPartiesConfig(network Network, networkLocalConfig *NetworkLocalConfig, cryptoBaseDir string) []config.PartyConfig {
	partiesConfig := make([]config.PartyConfig, len(network.Parties))

	for i, party := range network.Parties {
		partyPath := filepath.Join(cryptoBaseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID), "orderers", fmt.Sprintf("party%d", party.ID))

		partyLocalConfig := networkLocalConfig.PartiesLocalConfig[i]

		routerConfig := config.RouterNodeConfig{
			Host:    partyLocalConfig.RouterLocalConfig.GeneralConfig.ListenAddress,
			Port:    partyLocalConfig.RouterLocalConfig.GeneralConfig.ListenPort,
			TLSCert: partyLocalConfig.RouterLocalConfig.GeneralConfig.TLSConfig.Certificate,
		}

		var batchersConfig []config.BatcherNodeConfig
		for j := range party.BatchersEndpoints {
			batcherConfig := config.BatcherNodeConfig{
				ShardID:   partyLocalConfig.BatchersLocalConfig[j].BatcherParams.ShardID,
				Host:      partyLocalConfig.BatchersLocalConfig[j].GeneralConfig.ListenAddress,
				Port:      partyLocalConfig.BatchersLocalConfig[j].GeneralConfig.ListenPort,
				PublicKey: filepath.Join(partyPath, fmt.Sprintf("batcher%d", j+1), "signing-cert.pem"),
				TLSCert:   partyLocalConfig.BatchersLocalConfig[j].GeneralConfig.TLSConfig.Certificate,
			}
			batchersConfig = append(batchersConfig, batcherConfig)
		}

		consenterConfig := config.ConsenterNodeConfig{
			Host:      partyLocalConfig.ConsenterLocalConfig.GeneralConfig.ListenAddress,
			Port:      partyLocalConfig.ConsenterLocalConfig.GeneralConfig.ListenPort,
			PublicKey: filepath.Join(partyPath, "consenter", "signing-cert.pem"),
			TLSCert:   partyLocalConfig.ConsenterLocalConfig.GeneralConfig.TLSConfig.Certificate,
		}

		assemblerConfig := config.AssemblerNodeConfig{
			Host:    partyLocalConfig.AssemblerLocalConfig.GeneralConfig.ListenAddress,
			Port:    partyLocalConfig.AssemblerLocalConfig.GeneralConfig.ListenPort,
			TLSCert: partyLocalConfig.AssemblerLocalConfig.GeneralConfig.TLSConfig.Certificate,
		}

		orgDir := filepath.Join(cryptoBaseDir, "crypto", "ordererOrganizations", fmt.Sprintf("org%d", party.ID))
		partyConfig := config.PartyConfig{
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
