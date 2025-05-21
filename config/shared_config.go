/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"fmt"

	"github.ibm.com/decentralized-trust-research/arma/config/protos"

	"github.ibm.com/decentralized-trust-research/arma/common/utils"
)

// SharedConfigYaml holds the initial configuration that will be used to bootstrap new nodes.
// This configuration is common to all Arma nodes.
type SharedConfigYaml struct {
	PartiesConfig   []PartyConfig   `yaml:"Parties,omitempty"`
	ConsensusConfig ConsensusConfig `yaml:"Consensus,omitempty"`
	BatchingConfig  BatchingConfig  `yaml:"Batching,omitempty"`
}

// LoadSharedConfig reads the shared config yaml and translate it to the proto shared config.
func LoadSharedConfig(filePath string) (*protos.SharedConfig, *SharedConfigYaml, error) {
	sharedConfigYaml, err := loadSharedConfigYAML(filePath)
	if err != nil {
		return nil, nil, err
	}
	sharedConfig, err := parseSharedConfigYaml(sharedConfigYaml)
	if err != nil {
		return nil, nil, err
	}
	return sharedConfig, sharedConfigYaml, nil
}

// loadSharedConfigYAML reads the boostrap/shared_config.yaml file.
func loadSharedConfigYAML(filePath string) (*SharedConfigYaml, error) {
	if filePath == "" {
		return nil, fmt.Errorf("cannot load shared configuration, path: %s is empty", filePath)
	}

	sharedConfigYaml := SharedConfigYaml{}
	err := utils.ReadFromYAML(&sharedConfigYaml, filePath)
	if err != nil {
		return nil, fmt.Errorf("cannot load shared configuration, failed reading config yaml, err: %s", err)
	}

	return &sharedConfigYaml, nil
}

// parseSharedConfigYaml converts the shared config yaml representation to the config.SharedConfig representation, in which the paths are replaced by certificates.
func parseSharedConfigYaml(sharedConfigYaml *SharedConfigYaml) (*protos.SharedConfig, error) {
	var partiesConfig []*protos.PartyConfig

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

		pc := &protos.PartyConfig{
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

	sharedConfig := protos.SharedConfig{
		PartiesConfig: partiesConfig,
		ConsensusConfig: &protos.ConsensusConfig{
			SmartBFTConfig: &protos.SmartBFTConfig{
				RequestBatchMaxCount:          sharedConfigYaml.ConsensusConfig.BFTConfig.RequestBatchMaxCount,
				RequestBatchMaxBytes:          sharedConfigYaml.ConsensusConfig.BFTConfig.RequestBatchMaxBytes,
				RequestBatchMaxInterval:       sharedConfigYaml.ConsensusConfig.BFTConfig.RequestBatchMaxInterval.String(),
				IncomingMessageBufferSize:     sharedConfigYaml.ConsensusConfig.BFTConfig.IncomingMessageBufferSize,
				RequestPoolSize:               sharedConfigYaml.ConsensusConfig.BFTConfig.RequestPoolSize,
				RequestForwardTimeout:         sharedConfigYaml.ConsensusConfig.BFTConfig.RequestForwardTimeout.String(),
				RequestComplainTimeout:        sharedConfigYaml.ConsensusConfig.BFTConfig.RequestComplainTimeout.String(),
				RequestAutoRemoveTimeout:      sharedConfigYaml.ConsensusConfig.BFTConfig.RequestAutoRemoveTimeout.String(),
				ViewChangeResendInterval:      sharedConfigYaml.ConsensusConfig.BFTConfig.ViewChangeResendInterval.String(),
				ViewChangeTimeout:             sharedConfigYaml.ConsensusConfig.BFTConfig.ViewChangeTimeout.String(),
				LeaderHeartbeatTimeout:        sharedConfigYaml.ConsensusConfig.BFTConfig.LeaderHeartbeatTimeout.String(),
				LeaderHeartbeatCount:          sharedConfigYaml.ConsensusConfig.BFTConfig.LeaderHeartbeatCount,
				NumOfTicksBehindBeforeSyncing: sharedConfigYaml.ConsensusConfig.BFTConfig.NumOfTicksBehindBeforeSyncing,
				CollectTimeout:                sharedConfigYaml.ConsensusConfig.BFTConfig.CollectTimeout.String(),
				SyncOnStart:                   sharedConfigYaml.ConsensusConfig.BFTConfig.SyncOnStart,
				SpeedUpViewChange:             sharedConfigYaml.ConsensusConfig.BFTConfig.SpeedUpViewChange,
				LeaderRotation:                sharedConfigYaml.ConsensusConfig.BFTConfig.LeaderRotation,
				DecisionsPerLeader:            sharedConfigYaml.ConsensusConfig.BFTConfig.DecisionsPerLeader,
				RequestMaxBytes:               sharedConfigYaml.ConsensusConfig.BFTConfig.RequestMaxBytes,
				RequestPoolSubmitTimeout:      sharedConfigYaml.ConsensusConfig.BFTConfig.RequestPoolSubmitTimeout.String(),
			},
		},
		BatchingConfig: &protos.BatchingConfig{
			BatchTimeouts: &protos.BatchTimeouts{
				BatchCreationTimeout:  sharedConfigYaml.BatchingConfig.BatchTimeouts.BatchCreationTimeout.String(),
				FirstStrikeThreshold:  sharedConfigYaml.BatchingConfig.BatchTimeouts.FirstStrikeThreshold.String(),
				SecondStrikeThreshold: sharedConfigYaml.BatchingConfig.BatchTimeouts.SecondStrikeThreshold.String(),
				AutoRemoveTimeout:     sharedConfigYaml.BatchingConfig.BatchTimeouts.AutoRemoveTimeout.String(),
			},
			BatchSize: &protos.BatchSize{
				MaxMessageCount:   sharedConfigYaml.BatchingConfig.BatchSize.MaxMessageCount,
				AbsoluteMaxBytes:  sharedConfigYaml.BatchingConfig.BatchSize.AbsoluteMaxBytes,
				PreferredMaxBytes: sharedConfigYaml.BatchingConfig.BatchSize.PreferredMaxBytes,
			},
			RequestMaxBytes: sharedConfigYaml.BatchingConfig.RequestMaxBytes,
		},
	}
	return &sharedConfig, nil
}

func loadCACerts(caCertsPaths []string, tlsCACertsPaths []string) ([][]byte, [][]byte, error) {
	var caCerts [][]byte
	for _, caCertPath := range caCertsPaths {
		caCert, err := utils.ReadPem(caCertPath)
		if err != nil {
			return nil, nil, fmt.Errorf("load shared config failed, read ca cert failed, err: %v", err)
		}
		caCerts = append(caCerts, caCert)
	}

	var TLSCACerts [][]byte
	for _, TLSCACertPath := range tlsCACertsPaths {
		TLSCACert, err := utils.ReadPem(TLSCACertPath)
		if err != nil {
			return nil, nil, fmt.Errorf("load shared config failed, read tls ca cert failed, err: %v", err)
		}
		TLSCACerts = append(TLSCACerts, TLSCACert)
	}

	return caCerts, TLSCACerts, nil
}

func loadRouterConfig(host string, port uint32, tlsCertPath string) (*protos.RouterNodeConfig, error) {
	TLSCert, err := utils.ReadPem(tlsCertPath)
	if err != nil {
		return nil, fmt.Errorf("load shared config failed, read router tls cert failed, err: %v", err)
	}
	return &protos.RouterNodeConfig{
		Host:    host,
		Port:    port,
		TlsCert: TLSCert,
	}, nil
}

func loadBatchersConfig(batchersConfigYaml []BatcherNodeConfig) ([]*protos.BatcherNodeConfig, error) {
	var batchersConfig []*protos.BatcherNodeConfig

	for _, batcher := range batchersConfigYaml {
		TLSCert, err := utils.ReadPem(batcher.TLSCert)
		if err != nil {
			return nil, fmt.Errorf("load shared config failed, read batcher tls cert failed, err: %v", err)
		}

		pubKey, err := utils.ReadPem(batcher.PublicKey)
		if err != nil {
			return nil, fmt.Errorf("load shared config failed, read batcher public key failed, err: %v", err)
		}
		batcherConfig := &protos.BatcherNodeConfig{
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

func loadConsenterConfig(host string, port uint32, tlsCertPath string, pubKeyPath string) (*protos.ConsenterNodeConfig, error) {
	TLSCert, err := utils.ReadPem(tlsCertPath)
	if err != nil {
		return nil, fmt.Errorf("load shared config failed, read consenster tls cert failed, err: %v", err)
	}

	pubKey, err := utils.ReadPem(pubKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load shared config failed, read consenster public key failed, err: %v", err)
	}
	return &protos.ConsenterNodeConfig{
		Host:      host,
		Port:      port,
		PublicKey: pubKey,
		TlsCert:   TLSCert,
	}, nil
}

func loadAssemblerConfig(host string, port uint32, tlsCertPath string) (*protos.AssemblerNodeConfig, error) {
	TLSCert, err := utils.ReadPem(tlsCertPath)
	if err != nil {
		return nil, fmt.Errorf("load shared config failed, read assembler tls cert failed, err: %v", err)
	}
	return &protos.AssemblerNodeConfig{
		Host:    host,
		Port:    port,
		TlsCert: TLSCert,
	}, nil
}
