package generate

import (
	"encoding/pem"
	"fmt"
	"os"

	"arma/common/utils"
	"arma/config"
)

// SharedConfig holds the initial configuration that will be used to bootstrap new nodes.
// This configuration is common to all Arma nodes.
type SharedConfig struct {
	PartiesConfig   []PartyConfig   `yaml:"Parties,omitempty"`
	ConsensusConfig ConsensusConfig `yaml:"Consensus,omitempty"`
	BatchingConfig  BatchingConfig  `yaml:"Batching,omitempty"`
}

func LoadSharedConfig(filePath string) (*config.SharedConfig, error) {
	sharedConfigYaml, err := loadSharedConfigYAML(filePath)
	if err != nil {
		return nil, err
	}
	sharedConfig, err := parseSharedConfigYaml(sharedConfigYaml)
	if err != nil {
		return nil, err
	}
	return sharedConfig, nil
}

// loadSharedConfigYAML reads the boostrap/shared_config.yaml file.
func loadSharedConfigYAML(filePath string) (*SharedConfig, error) {
	if filePath == "" {
		return nil, fmt.Errorf("cannot load shared configuration, path: %s is empty", filePath)
	}

	sharedConfigYaml := SharedConfig{}
	err := utils.ReadFromYAML(&sharedConfigYaml, filePath)
	if err != nil {
		return nil, fmt.Errorf("cannot load shared configuration, failed reading config yaml, err: %s", err)
	}

	return &sharedConfigYaml, nil
}

// parseSharedConfigYaml converts the shared config yaml representation to the config.SharedConfig representation, in which the paths are replaced by certificates.
func parseSharedConfigYaml(sharedConfigYaml *SharedConfig) (*config.SharedConfig, error) {
	var partiesConfig []*config.PartyConfig

	for _, partyConfig := range sharedConfigYaml.PartiesConfig {
		caCerts, tlsCACerts, err := loadCACerts(partyConfig.CACerts, partyConfig.TLSCACerts)
		if err != nil {
			return nil, err
		}

		routerConfig, err := loadRouterConfig(partyConfig.RouterConfig.Host, partyConfig.RouterConfig.Port, partyConfig.RouterConfig.TLSCert)
		if err != nil {
			return nil, err
		}

		batchersConfig, err := loadBatchersConfig(partyConfig.BatchersConfig)
		if err != nil {
			return nil, err
		}

		consenterConfig, err := loadConsenterConfig(partyConfig.ConsenterConfig.Host, partyConfig.ConsenterConfig.Port, partyConfig.ConsenterConfig.TLSCert, partyConfig.ConsenterConfig.PublicKey)
		if err != nil {
			return nil, err
		}

		assemblerConfig, err := loadAssemblerConfig(partyConfig.AssemblerConfig.Host, partyConfig.AssemblerConfig.Port, partyConfig.AssemblerConfig.TLSCert)
		if err != nil {
			return nil, err
		}

		pc := &config.PartyConfig{
			PartyID:         uint32(partyConfig.PartyID),
			CACerts:         caCerts,
			TLSCACerts:      tlsCACerts,
			RouterConfig:    routerConfig,
			BatchersConfig:  batchersConfig,
			ConsenterConfig: consenterConfig,
			AssemblerConfig: assemblerConfig,
		}
		partiesConfig = append(partiesConfig, pc)
	}

	sharedConfig := config.SharedConfig{
		PartiesConfig: partiesConfig,
		ConsensusConfig: &config.ConsensusConfig{SmartBFTConfig: &config.SmartBFTConfig{
			RequestBatchMaxInterval:   uint32(sharedConfigYaml.ConsensusConfig.BFTConfig.RequestBatchMaxInterval),
			RequestForwardTimeout:     uint32(sharedConfigYaml.ConsensusConfig.BFTConfig.RequestForwardTimeout),
			RequestComplainTimeout:    uint32(sharedConfigYaml.ConsensusConfig.BFTConfig.RequestComplainTimeout),
			RequestAutoRemoveTimeout:  uint32(sharedConfigYaml.ConsensusConfig.BFTConfig.RequestAutoRemoveTimeout),
			ViewChangeResendInterval:  uint32(sharedConfigYaml.ConsensusConfig.BFTConfig.ViewChangeResendInterval),
			ViewChangeTimeout:         uint32(sharedConfigYaml.ConsensusConfig.BFTConfig.ViewChangeTimeout),
			LeaderHeartbeatTimeout:    uint32(sharedConfigYaml.ConsensusConfig.BFTConfig.LeaderHeartbeatTimeout),
			CollectTimeout:            uint32(sharedConfigYaml.ConsensusConfig.BFTConfig.CollectTimeout),
			IncomingMessageBufferSize: uint32(sharedConfigYaml.ConsensusConfig.BFTConfig.IncomingMessageBufferSize),
			RequestPoolSize:           uint32(sharedConfigYaml.ConsensusConfig.BFTConfig.RequestPoolSize),
			LeaderHeartbeatCount:      uint32(sharedConfigYaml.ConsensusConfig.BFTConfig.LeaderHeartbeatCount),
		}},
		BatchingConfig: &config.BatchingConfig{
			BatchTimeout: uint32(sharedConfigYaml.BatchingConfig.BatchTimeout),
			BatchSize: &config.BatchSize{
				MaxMessageCount:   sharedConfigYaml.BatchingConfig.BatchSize.MaxMessageCount,
				AbsoluteMaxBytes:  sharedConfigYaml.BatchingConfig.BatchSize.AbsoluteMaxBytes,
				PreferredMaxBytes: sharedConfigYaml.BatchingConfig.BatchSize.PreferredMaxBytes,
			},
		},
	}
	return &sharedConfig, nil
}

func loadCACerts(caCertsPaths []string, tlsCACertsPaths []string) ([][]byte, [][]byte, error) {
	var caCerts [][]byte
	for _, caCertPath := range caCertsPaths {
		caCert, err := readPem(caCertPath)
		if err != nil {
			return nil, nil, fmt.Errorf("load shared config failed, read ca cert failed, err: %v", err)
		}
		caCerts = append(caCerts, caCert)
	}

	var TLSCACerts [][]byte
	for _, TLSCACertPath := range tlsCACertsPaths {
		TLSCACert, err := readPem(TLSCACertPath)
		if err != nil {
			return nil, nil, fmt.Errorf("load shared config failed, read tls ca cert failed, err: %v", err)
		}
		TLSCACerts = append(TLSCACerts, TLSCACert)
	}

	return caCerts, TLSCACerts, nil
}

func loadRouterConfig(host string, port uint32, tlsCertPath string) (*config.RouterNodeConfig, error) {
	TLSCert, err := readPem(tlsCertPath)
	if err != nil {
		return nil, fmt.Errorf("load shared config failed, read router tls cert failed, err: %v", err)
	}
	return &config.RouterNodeConfig{
		Host:    host,
		Port:    port,
		TlsCert: TLSCert,
	}, nil
}

func loadBatchersConfig(batchersConfigYaml []BatcherNodeConfig) ([]*config.BatcherNodeConfig, error) {
	var batchersConfig []*config.BatcherNodeConfig

	for _, batcher := range batchersConfigYaml {
		TLSCert, err := readPem(batcher.TLSCert)
		if err != nil {
			return nil, fmt.Errorf("load shared config failed, read batcher tls cert failed, err: %v", err)
		}

		pubKey, err := readPem(batcher.PublicKey)
		if err != nil {
			return nil, fmt.Errorf("load shared config failed, read batcher public key failed, err: %v", err)
		}
		batcherConfig := &config.BatcherNodeConfig{
			ShardID:   uint32(batcher.ShardID),
			Host:      batcher.Host,
			Port:      batcher.Port,
			PublicKey: pubKey,
			TlsCert:   TLSCert,
		}
		batchersConfig = append(batchersConfig, batcherConfig)
	}
	return batchersConfig, nil
}

func loadConsenterConfig(host string, port uint32, tlsCertPath string, pubKeyPath string) (*config.ConsenterNodeConfig, error) {
	TLSCert, err := readPem(tlsCertPath)
	if err != nil {
		return nil, fmt.Errorf("load shared config failed, read consenster tls cert failed, err: %v", err)
	}

	pubKey, err := readPem(pubKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load shared config failed, read consenster public key failed, err: %v", err)
	}
	return &config.ConsenterNodeConfig{
		Host:      host,
		Port:      port,
		PublicKey: pubKey,
		TlsCert:   TLSCert,
	}, nil
}

func loadAssemblerConfig(host string, port uint32, tlsCertPath string) (*config.AssemblerNodeConfig, error) {
	TLSCert, err := readPem(tlsCertPath)
	if err != nil {
		return nil, fmt.Errorf("load shared config failed, read assembler tls cert failed, err: %v", err)
	}
	return &config.AssemblerNodeConfig{
		Host:    host,
		Port:    port,
		TlsCert: TLSCert,
	}, nil
}

func readPem(path string) ([]byte, error) {
	if path == "" {
		return nil, fmt.Errorf("failed reading pem file, path is empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed reading a pem file from %s, err: %v", path, err)
	}
	pbl, _ := pem.Decode(data)
	certRaw := pbl.Bytes

	return certRaw, nil
}
