package config

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/common/utils"
	"github.ibm.com/decentralized-trust-research/arma/config/protos"
	nodeconfig "github.ibm.com/decentralized-trust-research/arma/node/config"

	"github.com/pkg/errors"
)

// Configuration holds the complete configuration of a database node.
type Configuration struct {
	LocalConfig  *LocalConfig
	SharedConfig *protos.SharedConfig
}

// ReadConfig reads the configurations from the config file and returns it. The configuration includes both local and shared.
func ReadConfig(configFilePath string) (*Configuration, error) {
	if configFilePath == "" {
		return nil, errors.New("path to the configuration file is empty")
	}

	var err error
	conf := &Configuration{}

	conf.LocalConfig, err = LoadLocalConfig(configFilePath)
	if err != nil {
		return nil, err
	}

	switch conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.Method {
	case "yaml":
		if conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File != "" {
			conf.SharedConfig, err = LoadSharedConfig(conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to read the shared configuration from: %s", conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File)
			}
		}
	case "block":
		// TODO: complete when block is ready
		return nil, errors.Errorf("not implemented yet")
	default:
		return nil, errors.Errorf("bootstrap method %s is invalid", conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.Method)
	}

	return conf, nil
}

func (config *Configuration) ExtractRouterConfig() *nodeconfig.RouterNodeConfig {
	routerConfig := &nodeconfig.RouterNodeConfig{
		PartyID:                       config.LocalConfig.NodeLocalConfig.PartyID,
		TLSCertificateFile:            config.LocalConfig.TLSConfig.Certificate,
		TLSPrivateKeyFile:             config.LocalConfig.TLSConfig.PrivateKey,
		ListenAddress:                 config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress + ":" + strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenPort)),
		Shards:                        config.ExtractShards(),
		NumOfConnectionsForBatcher:    config.LocalConfig.NodeLocalConfig.RouterParams.NumberOfConnectionsPerBatcher,
		NumOfgRPCStreamsPerConnection: config.LocalConfig.NodeLocalConfig.RouterParams.NumberOfStreamsPerConnection,
		UseTLS:                        config.LocalConfig.TLSConfig.Enabled,
	}
	return routerConfig
}

func (config *Configuration) ExtractBatcherConfig() *nodeconfig.BatcherNodeConfig {
	signingPrivateKey, err := utils.ReadPem(filepath.Join(config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "../", "signingPrivateKey.pem"))
	if err != nil {
		panic(err)
	}
	batcherConfig := &nodeconfig.BatcherNodeConfig{
		Shards:             config.ExtractShards(),
		Consenters:         config.ExtractConsenters(),
		Directory:          config.LocalConfig.NodeLocalConfig.FileStore.Path,
		ListenAddress:      config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress + ":" + strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenPort)),
		PartyId:            config.LocalConfig.NodeLocalConfig.PartyID,
		ShardId:            config.LocalConfig.NodeLocalConfig.BatcherParams.ShardID,
		TLSPrivateKeyFile:  config.LocalConfig.TLSConfig.PrivateKey,
		TLSCertificateFile: config.LocalConfig.TLSConfig.Certificate,
		SigningPrivateKey:  signingPrivateKey,
		MemPoolMaxSize:     1000 * 1000, // TODO: add to config
		BatchMaxSize:       config.SharedConfig.BatchingConfig.BatchSize.MaxMessageCount,
		BatchMaxBytes:      config.SharedConfig.BatchingConfig.BatchSize.AbsoluteMaxBytes, // TODO: check param relation RequestMaxBytes < BatchMaxBytes
		RequestMaxBytes:    1024 * 1024,                                                   // TODO: add to config
		BatchTimeout:       time.Duration(config.SharedConfig.BatchingConfig.BatchTimeout),
	}
	return batcherConfig
}

func (config *Configuration) ExtractConsenterConfig() *nodeconfig.ConsenterNodeConfig {
	signingPrivateKey, err := utils.ReadPem(filepath.Join(config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "../", "signingPrivateKey.pem"))
	if err != nil {
		panic(err)
	}
	consenterConfig := &nodeconfig.ConsenterNodeConfig{
		Shards:             config.ExtractShards(),
		Consenters:         config.ExtractConsenters(),
		Directory:          config.LocalConfig.NodeLocalConfig.FileStore.Path,
		ListenAddress:      config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress + ":" + strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenPort)),
		PartyId:            config.LocalConfig.NodeLocalConfig.PartyID,
		TLSPrivateKeyFile:  config.LocalConfig.TLSConfig.PrivateKey,
		TLSCertificateFile: config.LocalConfig.TLSConfig.Certificate,
		SigningPrivateKey:  signingPrivateKey,
		BatchTimeout:       time.Duration(config.SharedConfig.BatchingConfig.BatchTimeout),
	}
	return consenterConfig
}

func (config *Configuration) ExtractAssemblerConfig() *nodeconfig.AssemblerNodeConfig {
	partySharedConfig := getPartyConfigFromSharedConfigByPartyID(config.SharedConfig, config.LocalConfig.NodeLocalConfig.PartyID)

	var tlsCACertsCollection []nodeconfig.RawBytes
	for _, ca := range partySharedConfig.TLSCACerts {
		tlsCACertsCollection = append(tlsCACertsCollection, ca)
	}

	assemblerConfig := &nodeconfig.AssemblerNodeConfig{
		TLSPrivateKeyFile:  config.LocalConfig.TLSConfig.PrivateKey,
		TLSCertificateFile: config.LocalConfig.TLSConfig.Certificate,
		PartyId:            config.LocalConfig.NodeLocalConfig.PartyID,
		Directory:          config.LocalConfig.NodeLocalConfig.FileStore.Path,
		ListenAddress:      config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress + ":" + strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenPort)),
		Shards:             config.ExtractShards(),
		Consenter: nodeconfig.ConsenterInfo{
			PartyID:    config.LocalConfig.NodeLocalConfig.PartyID,
			Endpoint:   partySharedConfig.ConsenterConfig.Host + ":" + strconv.Itoa(int(partySharedConfig.ConsenterConfig.Port)),
			PublicKey:  partySharedConfig.ConsenterConfig.PublicKey,
			TLSCACerts: tlsCACertsCollection,
		},
		UseTLS: config.LocalConfig.TLSConfig.Enabled,
	}
	return assemblerConfig
}

func (config *Configuration) ExtractShards() []nodeconfig.ShardInfo {
	shardToBatchers := make(map[types.ShardID][]nodeconfig.BatcherInfo)
	for _, party := range config.SharedConfig.PartiesConfig {

		var tlsCACertsCollection []nodeconfig.RawBytes
		for _, ca := range party.TLSCACerts {
			tlsCACertsCollection = append(tlsCACertsCollection, ca)
		}

		for _, batcher := range party.BatchersConfig {
			shardId := types.ShardID(batcher.ShardID)

			// Fetch public key from signing certificate
			// NOTE: ARMA's new configuration now uses certificates, which inherently contain the public key, instead of a separate public key field.
			// To ensure backward compatibility until the full new config integration, the public key it extracted.
			block, _ := pem.Decode(batcher.PublicKey)
			if block == nil || block.Bytes == nil {
				panic("Failed decoding batcher signing certificate")
			}

			var pemPublicKey []byte
			if block.Type == "CERTIFICATE" {
				pemPublicKey = blockToPublicKey(block)
			}

			if block.Type == "PUBLIC KEY" {
				pemPublicKey = batcher.PublicKey
			}

			batcher := nodeconfig.BatcherInfo{
				PartyID:    types.PartyID(party.PartyID),
				Endpoint:   batcher.Host + ":" + strconv.Itoa(int(batcher.Port)),
				TLSCACerts: tlsCACertsCollection,
				PublicKey:  pemPublicKey,
				TLSCert:    batcher.TlsCert,
			}
			shardToBatchers[shardId] = append(shardToBatchers[shardId], batcher)
		}
	}

	// build Shards from the map
	var shards []nodeconfig.ShardInfo
	for shardId, batchers := range shardToBatchers {
		shardInfo := nodeconfig.ShardInfo{
			ShardId:  shardId,
			Batchers: batchers,
		}
		shards = append(shards, shardInfo)
	}

	return shards
}

func (config *Configuration) ExtractConsenters() []nodeconfig.ConsenterInfo {
	var consenters []nodeconfig.ConsenterInfo
	for _, party := range config.SharedConfig.PartiesConfig {
		var tlsCACertsCollection []nodeconfig.RawBytes
		for _, ca := range party.TLSCACerts {
			tlsCACertsCollection = append(tlsCACertsCollection, ca)
		}

		// Fetch public key from signing certificate
		// NOTE: ARMA's new configuration now uses certificates, which inherently contain the public key, instead of a separate public key field.
		// To ensure backward compatibility until the full new config integration, the public key it extracted.
		block, _ := pem.Decode(party.ConsenterConfig.PublicKey)
		if block == nil || block.Bytes == nil {
			panic("Failed decoding consenter signing certificate")
		}

		var pemPublicKey []byte
		if block.Type == "CERTIFICATE" {
			pemPublicKey = blockToPublicKey(block)
		}

		if block.Type == "PUBLIC KEY" {
			pemPublicKey = party.ConsenterConfig.PublicKey
		}

		consenterInfo := nodeconfig.ConsenterInfo{
			PartyID:    types.PartyID(party.PartyID),
			Endpoint:   party.ConsenterConfig.Host + ":" + strconv.Itoa(int(party.ConsenterConfig.Port)),
			PublicKey:  pemPublicKey,
			TLSCACerts: tlsCACertsCollection,
		}
		consenters = append(consenters, consenterInfo)
	}

	return consenters
}

func blockToPublicKey(block *pem.Block) []byte {
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		panic(fmt.Sprintf("Failed parsing consenter signing certificate: %v", err))
	}

	pubKey, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		panic(fmt.Sprintf("Failed parsing consenter public key: %v", err))
	}

	publicKeyBytes, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		panic(fmt.Sprintf("Failed marshaling consenter public key: %v", err))
	}

	pemPublicKey := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	})

	return pemPublicKey
}

func getPartyConfigFromSharedConfigByPartyID(sharedConfig *protos.SharedConfig, partyID types.PartyID) *protos.PartyConfig {
	for _, partyConfig := range sharedConfig.PartiesConfig {
		if partyConfig.PartyID == uint32(partyID) {
			return partyConfig
		}
	}
	return nil
}
