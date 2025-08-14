/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/hyperledger/fabric-x-common/protoutil"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/common/utils"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	nodeconfig "github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// Configuration holds the complete configuration of a database node.
type Configuration struct {
	LocalConfig  *LocalConfig
	SharedConfig *protos.SharedConfig
}

// ReadConfig reads the configurations from the config file and returns it. The configuration includes both local and shared.
func ReadConfig(configFilePath string) (*Configuration, *common.Block, error) {
	if configFilePath == "" {
		return nil, nil, errors.New("path to the configuration file is empty")
	}

	var err error
	conf := &Configuration{
		LocalConfig:  &LocalConfig{},
		SharedConfig: &protos.SharedConfig{},
	}

	conf.LocalConfig, err = LoadLocalConfig(configFilePath)
	if err != nil {
		return nil, nil, err
	}

	var genesisBlock *common.Block
	switch conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.Method {
	case "yaml":
		if conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File != "" {
			conf.SharedConfig, _, err = LoadSharedConfig(conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to read the shared configuration from: %s", conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File)
			}
		} else {
			return nil, nil, errors.Wrapf(err, "failed to read shared config, path is empty")
		}
	case "block":
		if conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File != "" {
			blockPath := conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File
			data, err := os.ReadFile(conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.File)
			if err != nil {
				return nil, nil, fmt.Errorf("could not read block %s", blockPath)
			}
			genesisBlock, err = protoutil.UnmarshalBlock(data)
			if err != nil {
				return nil, nil, fmt.Errorf("error unmarshalling to block: %s", err)
			}

			consensusMetaData, err := ReadSharedConfigFromBootstrapConfigBlock(genesisBlock)
			if err != nil {
				return nil, nil, err
			}

			err = proto.Unmarshal(consensusMetaData, conf.SharedConfig)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to unmarshal consensus metadata to a shared configuration")
			}
		} else {
			return nil, nil, errors.Wrapf(err, "failed to read a cofig block, path is empty")
		}
	default:
		return nil, nil, errors.Errorf("bootstrap method %s is invalid", conf.LocalConfig.NodeLocalConfig.GeneralConfig.Bootstrap.Method)
	}

	return conf, genesisBlock, nil
}

func (config *Configuration) GetBFTConfig(partyID types.PartyID) (smartbft_types.Configuration, error) {
	smartBFTConfigFetchedFromSharedConfig := config.SharedConfig.ConsensusConfig.SmartBFTConfig

	var err error
	var BFTConfig smartbft_types.Configuration

	if BFTConfig.RequestBatchMaxInterval, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.RequestBatchMaxInterval); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option RequestBatchMaxInterval")
	}
	if BFTConfig.RequestForwardTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.RequestForwardTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option RequestForwardTimeout")
	}
	if BFTConfig.RequestComplainTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.RequestComplainTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option RequestComplainTimeout")
	}
	if BFTConfig.RequestAutoRemoveTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.RequestAutoRemoveTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option RequestAutoRemoveTimeout")
	}
	if BFTConfig.ViewChangeResendInterval, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.ViewChangeResendInterval); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option ViewChangeResendInterval")
	}
	if BFTConfig.ViewChangeTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.ViewChangeTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option ViewChangeTimeout")
	}
	if BFTConfig.LeaderHeartbeatTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.LeaderHeartbeatTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option LeaderHeartbeatTimeout")
	}
	if BFTConfig.CollectTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.CollectTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option CollectTimeout")
	}
	if BFTConfig.RequestPoolSubmitTimeout, err = time.ParseDuration(smartBFTConfigFetchedFromSharedConfig.RequestPoolSubmitTimeout); err != nil {
		return BFTConfig, errors.Wrap(err, "bad config metadata option RequestPoolSubmitTimeout")
	}

	BFTConfig.SelfID = uint64(partyID)
	BFTConfig.RequestBatchMaxCount = smartBFTConfigFetchedFromSharedConfig.RequestBatchMaxCount
	BFTConfig.RequestBatchMaxBytes = smartBFTConfigFetchedFromSharedConfig.RequestBatchMaxBytes
	BFTConfig.IncomingMessageBufferSize = smartBFTConfigFetchedFromSharedConfig.IncomingMessageBufferSize
	BFTConfig.RequestPoolSize = smartBFTConfigFetchedFromSharedConfig.RequestPoolSize
	BFTConfig.LeaderHeartbeatCount = smartBFTConfigFetchedFromSharedConfig.LeaderHeartbeatCount
	BFTConfig.NumOfTicksBehindBeforeSyncing = smartBFTConfigFetchedFromSharedConfig.NumOfTicksBehindBeforeSyncing
	BFTConfig.SyncOnStart = smartBFTConfigFetchedFromSharedConfig.SyncOnStart
	BFTConfig.SpeedUpViewChange = smartBFTConfigFetchedFromSharedConfig.SpeedUpViewChange
	BFTConfig.LeaderRotation = smartBFTConfigFetchedFromSharedConfig.LeaderRotation
	BFTConfig.DecisionsPerLeader = smartBFTConfigFetchedFromSharedConfig.DecisionsPerLeader
	BFTConfig.RequestMaxBytes = smartBFTConfigFetchedFromSharedConfig.RequestMaxBytes

	return BFTConfig, nil
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
		ClientAuthRequired:            config.LocalConfig.TLSConfig.ClientAuthRequired,
		RequestMaxBytes:               config.SharedConfig.BatchingConfig.RequestMaxBytes,
	}
	return routerConfig
}

func (config *Configuration) ExtractBatcherConfig() *nodeconfig.BatcherNodeConfig {
	signingPrivateKey, err := utils.ReadPem(filepath.Join(config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "keystore", "priv_sk"))
	if err != nil {
		panic(fmt.Sprintf("error launching batcher, failed extracting batcher config: %s", err))
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
		MemPoolMaxSize:     config.LocalConfig.NodeLocalConfig.BatcherParams.MemPoolMaxSize,
		BatchMaxSize:       config.SharedConfig.BatchingConfig.BatchSize.MaxMessageCount,
		BatchMaxBytes:      config.SharedConfig.BatchingConfig.BatchSize.AbsoluteMaxBytes,
		RequestMaxBytes:    config.SharedConfig.BatchingConfig.RequestMaxBytes,
		SubmitTimeout:      config.LocalConfig.NodeLocalConfig.BatcherParams.SubmitTimeout,
		BatchSequenceGap:   types.BatchSequence(config.LocalConfig.NodeLocalConfig.BatcherParams.BatchSequenceGap),
	}

	if batcherConfig.FirstStrikeThreshold, err = time.ParseDuration(config.SharedConfig.BatchingConfig.BatchTimeouts.FirstStrikeThreshold); err != nil {
		panic(fmt.Sprintf("error launching batcher, failed extracting batcher config, bad config metadata option FirstStrikeThreshold, err: %s", err))
	}

	if batcherConfig.SecondStrikeThreshold, err = time.ParseDuration(config.SharedConfig.BatchingConfig.BatchTimeouts.SecondStrikeThreshold); err != nil {
		panic(fmt.Sprintf("error launching batcher, failed extracting batcher config, bad config metadata option SecondStrikeThreshold, err: %s", err))
	}

	if batcherConfig.AutoRemoveTimeout, err = time.ParseDuration(config.SharedConfig.BatchingConfig.BatchTimeouts.AutoRemoveTimeout); err != nil {
		panic(fmt.Sprintf("error launching batcher, failed extracting batcher config, bad config metadata option AutoRemoveTimeout, err: %s", err))
	}

	if batcherConfig.BatchCreationTimeout, err = time.ParseDuration(config.SharedConfig.BatchingConfig.BatchTimeouts.BatchCreationTimeout); err != nil {
		panic(fmt.Sprintf("error launching batcher, failed extracting batcher config, bad config metadata option BatchCreationTimeout, err: %s", err))
	}

	return batcherConfig
}

func (config *Configuration) ExtractConsenterConfig() *nodeconfig.ConsenterNodeConfig {
	signingPrivateKey, err := utils.ReadPem(filepath.Join(config.LocalConfig.NodeLocalConfig.GeneralConfig.LocalMSPDir, "keystore", "priv_sk"))
	if err != nil {
		panic(fmt.Sprintf("error launching consenter, failed extracting consenter config: %s", err))
	}
	BFTConfig, err := config.GetBFTConfig(config.LocalConfig.NodeLocalConfig.PartyID)
	if err != nil {
		panic(fmt.Sprintf("error launching consenter, failed extracting consenter config: %s", err))
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
		WALDir:             DefaultConsenterNodeConfigParams(config.LocalConfig.NodeLocalConfig.FileStore.Path).WALDir,
		BFTConfig:          BFTConfig,
	}
	return consenterConfig
}

func (config *Configuration) ExtractAssemblerConfig() *nodeconfig.AssemblerNodeConfig {
	consenters := config.ExtractConsenters()
	var consenterFromMyParty nodeconfig.ConsenterInfo
	for _, consenter := range consenters {
		if consenter.PartyID == config.LocalConfig.NodeLocalConfig.PartyID {
			consenterFromMyParty = consenter
			break
		}
	}

	assemblerConfig := &nodeconfig.AssemblerNodeConfig{
		TLSPrivateKeyFile:         config.LocalConfig.TLSConfig.PrivateKey,
		TLSCertificateFile:        config.LocalConfig.TLSConfig.Certificate,
		PartyId:                   config.LocalConfig.NodeLocalConfig.PartyID,
		Directory:                 config.LocalConfig.NodeLocalConfig.FileStore.Path,
		ListenAddress:             config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenAddress + ":" + strconv.Itoa(int(config.LocalConfig.NodeLocalConfig.GeneralConfig.ListenPort)),
		PrefetchBufferMemoryBytes: config.LocalConfig.NodeLocalConfig.AssemblerParams.PrefetchBufferMemoryBytes,
		RestartLedgerScanTimeout:  config.LocalConfig.NodeLocalConfig.AssemblerParams.RestartLedgerScanTimeout,
		PrefetchEvictionTtl:       config.LocalConfig.NodeLocalConfig.AssemblerParams.PrefetchEvictionTtl,
		ReplicationChannelSize:    config.LocalConfig.NodeLocalConfig.AssemblerParams.ReplicationChannelSize,
		BatchRequestsChannelSize:  config.LocalConfig.NodeLocalConfig.AssemblerParams.BatchRequestsChannelSize,
		Shards:                    config.ExtractShards(),
		Consenter:                 consenterFromMyParty,
		UseTLS:                    config.LocalConfig.TLSConfig.Enabled,
		ClientAuthRequired:        config.LocalConfig.TLSConfig.ClientAuthRequired,
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
			// NOTE: ARMA's new configuration uses certificates, which inherently contain the public key, instead of a separate public key field.
			// To ensure backward compatibility until the full new config integration, the public key it enabled.
			block, _ := pem.Decode(batcher.SignCert)
			if block == nil || block.Bytes == nil {
				panic("Failed decoding batcher signing certificate")
			}

			var pemPublicKey []byte
			if block.Type == "CERTIFICATE" {
				pemPublicKey = blockToPublicKey(block)
			}

			if block.Type == "PUBLIC KEY" {
				pemPublicKey = batcher.SignCert
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

	sort.Slice(shards, func(i, j int) bool {
		return int(shards[i].ShardId) < int(shards[j].ShardId)
	})

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
		// To ensure backward compatibility until the full new config integration, the public key it enabled.
		block, _ := pem.Decode(party.ConsenterConfig.SignCert)
		if block == nil || block.Bytes == nil {
			panic("Failed decoding consenter signing certificate")
		}

		var pemPublicKey []byte
		if block.Type == "CERTIFICATE" {
			pemPublicKey = blockToPublicKey(block)
		}

		if block.Type == "PUBLIC KEY" {
			pemPublicKey = party.ConsenterConfig.SignCert
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
