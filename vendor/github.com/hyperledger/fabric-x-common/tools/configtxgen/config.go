/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package configtxgen

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	smartbfttypes "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer/etcdraft"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer/smartbft"

	"github.com/hyperledger/fabric-x-common/api/types"
	"github.com/hyperledger/fabric-x-common/common/viperutil"
	cf "github.com/hyperledger/fabric-x-common/core/config"
	"github.com/hyperledger/fabric-x-common/msp"
)

const (
	// EtcdRaft The type key for etcd based RAFT consensus.
	EtcdRaft = "etcdraft"
	BFT      = "BFT"
	Arma     = "arma"
)

const (
	// SampleInsecureSoloProfile references the sample profile which does not
	// include any MSPs and uses solo for ordering.
	SampleInsecureSoloProfile = "SampleInsecureSolo"
	// SampleDevModeSoloProfile references the sample profile which requires
	// only basic membership for admin privileges and uses solo for ordering.
	SampleDevModeSoloProfile = "SampleDevModeSolo"
	// SampleSingleMSPSoloProfile references the sample profile which includes
	// only the sample MSP and uses solo for ordering.
	SampleSingleMSPSoloProfile = "SampleSingleMSPSolo"

	// SampleDevModeEtcdRaftProfile references the sample profile used for testing
	// the etcd/raft-based ordering service.
	SampleDevModeEtcdRaftProfile = "SampleDevModeEtcdRaft"

	// SampleAppChannelInsecureSoloProfile references the sample profile which
	// does not include any MSPs and uses solo for ordering.
	SampleAppChannelInsecureSoloProfile = "SampleAppChannelInsecureSolo"

	// SampleAppChannelEtcdRaftProfile references the sample profile used for
	// testing the etcd/raft-based ordering service using the channel
	// participation API.
	SampleAppChannelEtcdRaftProfile = "SampleAppChannelEtcdRaft"

	// SampleAppChannelSmartBftProfile references the sample profile used for
	// testing the smartbft-based ordering service using the channel
	// participation API.
	SampleAppChannelSmartBftProfile = "SampleAppChannelSmartBft"

	// SampleSingleMSPChannelProfile references the sample profile which
	// includes only the sample MSP and is used to create a channel.
	SampleSingleMSPChannelProfile = "SampleSingleMSPChannel"

	// SampleFabricX references the sample profile used for
	// testing the arma-based ordering service and the scalable committer.
	SampleFabricX = "SampleFabricX"

	// TwoOrgsSampleFabricX references the sample two organizations profile used for
	// testing the arma-based ordering service and the scalable committer.
	TwoOrgsSampleFabricX = "TwoOrgsSampleFabricX"

	// SampleConsortiumName is the sample consortium from the
	// sample configtx.yaml.
	SampleConsortiumName = "SampleConsortium"
	// SampleOrgName is the name of the sample org in the sample profiles.
	SampleOrgName = "SampleOrg"

	// AdminRoleAdminPrincipal is set as AdminRole to cause the MSP role of
	// type Admin to be used as the admin principal default.
	AdminRoleAdminPrincipal = "Role.ADMIN"
)

// TopLevel consists of the structs used by the configtxgen tool.
type TopLevel struct {
	Profiles      map[string]*Profile        `yaml:"Profiles"`
	Organizations []*Organization            `yaml:"Organizations"`
	Channel       *Profile                   `yaml:"Channel"`
	Application   *Application               `yaml:"Application"`
	Orderer       *Orderer                   `yaml:"Orderer"`
	Capabilities  map[string]map[string]bool `yaml:"Capabilities"`
}

// Profile encodes orderer/application configuration combinations for the
// configtxgen tool.
type Profile struct {
	Consortium   string                 `yaml:"Consortium"`
	Application  *Application           `yaml:"Application"`
	Orderer      *Orderer               `yaml:"Orderer"`
	Consortiums  map[string]*Consortium `yaml:"Consortiums"`
	Capabilities map[string]bool        `yaml:"Capabilities"`
	Policies     map[string]*Policy     `yaml:"Policies"`
}

// Policy encodes a channel config policy.
type Policy struct {
	Type string `yaml:"Type"`
	Rule string `yaml:"Rule"`
}

// Consortium represents a group of organizations which may create channels with each other.
type Consortium struct {
	Organizations []*Organization `yaml:"Organizations"`
}

// Application encodes the application-level configuration needed in config transactions.
type Application struct {
	Organizations []*Organization    `yaml:"Organizations"`
	Capabilities  map[string]bool    `yaml:"Capabilities"`
	Policies      map[string]*Policy `yaml:"Policies"`
	ACLs          map[string]string  `yaml:"ACLs"`
}

// Organization encodes the organization-level configuration needed in config transactions.
type Organization struct {
	Name     string             `yaml:"Name"`
	ID       string             `yaml:"ID"`
	MSPDir   string             `yaml:"MSPDir"`
	MSPType  string             `yaml:"MSPType"`
	Policies map[string]*Policy `yaml:"Policies"`

	// Note: Viper deserialization does not seem to care for
	// embedding of types, so we use one organization struct
	// for both orderers and applications.
	AnchorPeers      []*AnchorPeer            `yaml:"AnchorPeers"`
	OrdererEndpoints []*types.OrdererEndpoint `yaml:"OrdererEndpoints"`

	// AdminPrincipal is deprecated and may be removed in a future release
	// it was used for modifying the default policy generation, but policies
	// may now be specified explicitly so it is redundant and unnecessary
	AdminPrincipal string `yaml:"AdminPrincipal"`

	// SkipAsForeign indicates that this org definition is actually unknown to this
	// instance of the tool, so, parsing of this org's parameters should be ignored.
	SkipAsForeign bool
}

// AnchorPeer encodes the necessary fields to identify an anchor peer.
type AnchorPeer struct {
	Host string `yaml:"Host"`
	Port int    `yaml:"Port"`
}

// Orderer contains configuration associated to a channel.
type Orderer struct {
	OrdererType      string                   `yaml:"OrdererType"`
	Addresses        []string                 `yaml:"Addresses"`
	BatchTimeout     time.Duration            `yaml:"BatchTimeout"`
	BatchSize        BatchSize                `yaml:"BatchSize"`
	ConsenterMapping []*Consenter             `yaml:"ConsenterMapping"`
	EtcdRaft         *etcdraft.ConfigMetadata `yaml:"EtcdRaft"`
	SmartBFT         *smartbft.Options        `yaml:"SmartBFT"`
	Arma             *ConsensusMetadata       `yaml:"Arma"`
	Organizations    []*Organization          `yaml:"Organizations"`
	MaxChannels      uint64                   `yaml:"MaxChannels"`
	Capabilities     map[string]bool          `yaml:"Capabilities"`
	Policies         map[string]*Policy       `yaml:"Policies"`
}

// BatchSize contains configuration affecting the size of batches.
type BatchSize struct {
	MaxMessageCount   uint32 `yaml:"MaxMessageCount"`
	AbsoluteMaxBytes  uint32 `yaml:"AbsoluteMaxBytes"`
	PreferredMaxBytes uint32 `yaml:"PreferredMaxBytes"`
}

type Consenter struct {
	ID            uint32 `yaml:"ID"`
	Host          string `yaml:"Host"`
	Port          uint32 `yaml:"Port"`
	MSPID         string `yaml:"MSPID"`
	Identity      string `yaml:"Identity"`
	ClientTLSCert string `yaml:"ClientTLSCert"`
	ServerTLSCert string `yaml:"ServerTLSCert"`
}

type ConsensusMetadata struct {
	Path string `yaml:"Path"`
}

func genesisOrdererDefaults() Orderer {
	return Orderer{
		OrdererType:  "solo",
		BatchTimeout: 2 * time.Second,
		BatchSize: BatchSize{
			MaxMessageCount:   500,
			AbsoluteMaxBytes:  10 * 1024 * 1024,
			PreferredMaxBytes: 2 * 1024 * 1024,
		},
		EtcdRaft: &etcdraft.ConfigMetadata{
			Options: &etcdraft.Options{
				TickInterval:         "500ms",
				ElectionTick:         10,
				HeartbeatTick:        1,
				MaxInflightBlocks:    5,
				SnapshotIntervalSize: 16 * 1024 * 1024, // 16 MB
			},
		},
		SmartBFT: &smartbft.Options{
			RequestBatchMaxCount:      smartbfttypes.DefaultConfig.RequestBatchMaxCount,
			RequestBatchMaxBytes:      smartbfttypes.DefaultConfig.RequestBatchMaxBytes,
			RequestBatchMaxInterval:   smartbfttypes.DefaultConfig.RequestBatchMaxInterval.String(),
			IncomingMessageBufferSize: smartbfttypes.DefaultConfig.IncomingMessageBufferSize,
			RequestPoolSize:           smartbfttypes.DefaultConfig.RequestPoolSize,
			RequestForwardTimeout:     smartbfttypes.DefaultConfig.RequestForwardTimeout.String(),
			RequestComplainTimeout:    smartbfttypes.DefaultConfig.RequestComplainTimeout.String(),
			RequestAutoRemoveTimeout:  smartbfttypes.DefaultConfig.RequestAutoRemoveTimeout.String(),
			ViewChangeResendInterval:  smartbfttypes.DefaultConfig.ViewChangeResendInterval.String(),
			ViewChangeTimeout:         smartbfttypes.DefaultConfig.ViewChangeTimeout.String(),
			LeaderHeartbeatTimeout:    smartbfttypes.DefaultConfig.LeaderHeartbeatTimeout.String(),
			LeaderHeartbeatCount:      smartbfttypes.DefaultConfig.LeaderHeartbeatCount,
			CollectTimeout:            smartbfttypes.DefaultConfig.CollectTimeout.String(),
			SyncOnStart:               smartbfttypes.DefaultConfig.SyncOnStart,
			SpeedUpViewChange:         smartbfttypes.DefaultConfig.SpeedUpViewChange,
		},
		Arma: &ConsensusMetadata{},
	}
}

// LoadTopLevel simply loads the configtx.yaml file into the structs above and
// completes their initialization. Config paths may optionally be provided and
// will be used in place of the FABRIC_CFG_PATH env variable.
//
// Note, for environment overrides to work properly within a profile, Load
// should be used instead.
func LoadTopLevel(configPaths ...string) *TopLevel {
	config := viperutil.New()
	config.AddConfigPaths(configPaths...)
	config.SetConfigName("configtx")

	err := config.ReadInConfig()
	if err != nil {
		logger.Panicf("Error reading configuration: %s", err)
	}
	logger.Debugf("Using config file: %s", config.ConfigFileUsed())

	uconf, err := cache.load(config, config.ConfigFileUsed())
	if err != nil {
		logger.Panicf("failed to load configCache: %s", err)
	}
	uconf.completeInitialization(filepath.Dir(config.ConfigFileUsed()))
	logger.Infof("Loaded configuration: %s", config.ConfigFileUsed())

	return uconf
}

// Load returns the orderer/application config combination that corresponds to
// a given profile. Config paths may optionally be provided and will be used
// in place of the FABRIC_CFG_PATH env variable.
func Load(profile string, configPaths ...string) *Profile {
	config := viperutil.New()
	config.AddConfigPaths(configPaths...)
	config.SetConfigName("configtx")

	err := config.ReadInConfig()
	if err != nil {
		logger.Panicf("Error reading configuration: %s", err)
	}
	logger.Debugf("Using config file: %s", config.ConfigFileUsed())

	uconf, err := cache.load(config, config.ConfigFileUsed())
	if err != nil {
		logger.Panicf("Error loading config from config cache: %s", err)
	}

	result, ok := uconf.Profiles[profile]
	if !ok {
		logger.Panicf("Could not find profile: %s", profile)
	}

	result.CompleteInitialization(filepath.Dir(config.ConfigFileUsed()))

	logger.Infof("Loaded configuration: %s", config.ConfigFileUsed())

	return result
}

func (t *TopLevel) completeInitialization(configDir string) {
	for _, org := range t.Organizations {
		org.completeInitialization(configDir)
	}

	if t.Orderer != nil {
		t.Orderer.completeInitialization(configDir)
	}
}

// CompleteInitialization fills default values and amend relative paths to absolute according to the given directory.
func (p *Profile) CompleteInitialization(configDir string) {
	if p.Application != nil {
		for _, org := range p.Application.Organizations {
			org.completeInitialization(configDir)
		}
	}

	if p.Consortiums != nil {
		for _, consortium := range p.Consortiums {
			for _, org := range consortium.Organizations {
				org.completeInitialization(configDir)
			}
		}
	}

	if p.Orderer != nil {
		for _, org := range p.Orderer.Organizations {
			org.completeInitialization(configDir)
		}
		// Some profiles will not define orderer parameters
		p.Orderer.completeInitialization(configDir)
	}
}

func (org *Organization) completeInitialization(configDir string) {
	// set the MSP type; if none is specified we assume BCCSP
	if org.MSPType == "" {
		org.MSPType = msp.ProviderTypeToString(msp.FABRIC)
	}

	if org.AdminPrincipal == "" {
		org.AdminPrincipal = AdminRoleAdminPrincipal
	}
	translatePaths(configDir, org)
}

func (ord *Orderer) completeInitialization(configDir string) {
	d := genesisOrdererDefaults()
	if ord.OrdererType == "" {
		logger.Infof("Orderer.OrdererType unset, setting to %v", d.OrdererType)
		ord.OrdererType = d.OrdererType
	}
	if ord.BatchTimeout == 0 {
		logger.Infof("Orderer.BatchTimeout unset, setting to %s", d.BatchTimeout)
		ord.BatchTimeout = d.BatchTimeout
	}
	if ord.BatchSize.MaxMessageCount == 0 {
		logger.Infof("Orderer.BatchSize.MaxMessageCount unset, setting to %v", d.BatchSize.MaxMessageCount)
		ord.BatchSize.MaxMessageCount = d.BatchSize.MaxMessageCount
	}
	if ord.BatchSize.AbsoluteMaxBytes == 0 {
		logger.Infof("Orderer.BatchSize.AbsoluteMaxBytes unset, setting to %v", d.BatchSize.AbsoluteMaxBytes)
		ord.BatchSize.AbsoluteMaxBytes = d.BatchSize.AbsoluteMaxBytes
	}
	if ord.BatchSize.PreferredMaxBytes == 0 {
		logger.Infof("Orderer.BatchSize.PreferredMaxBytes unset, setting to %v", d.BatchSize.PreferredMaxBytes)
		ord.BatchSize.PreferredMaxBytes = d.BatchSize.PreferredMaxBytes
	}

	logger.Infof("orderer type: %s", ord.OrdererType)
	// Additional, consensus type-dependent initialization goes here
	// Also using this to panic on unknown orderer type.
	switch ord.OrdererType {
	case "solo":
		// nothing to be done here
	case EtcdRaft:
		completeInitializationOfEtcdRaft(ord.EtcdRaft, d.EtcdRaft, configDir)
	case BFT:
		if ord.SmartBFT == nil {
			logger.Infof("Orderer.SmartBFT.Options unset, setting to %v", d.SmartBFT)
			ord.SmartBFT = d.SmartBFT
		}
		ord.translateConsenterMapping(configDir, BFT)
	case Arma:
		if ord.Arma == nil {
			logger.Infof("Orderer.Arma unset, setting to %v", d.Arma)
			ord.Arma = d.Arma
		}
		if ord.Arma.Path != "" {
			cf.TranslatePathInPlace(configDir, &ord.Arma.Path)
		}
		ord.translateConsenterMapping(configDir, Arma)
	default:
		logger.Panicf("unknown orderer type: %s", ord.OrdererType)
	}
}

//nolint:gocognit // cognitive complexity 19.
func completeInitializationOfEtcdRaft(c, d *etcdraft.ConfigMetadata, configDir string) {
	if c == nil {
		logger.Panicf("%s configuration missing", EtcdRaft)
		return
	}
	if c.Options == nil {
		logger.Infof("Orderer.EtcdRaft.Options unset, setting to %v", d.Options)
		c.Options = d.Options
	}
	if c.Options.TickInterval == "" {
		logger.Infof("Orderer.EtcdRaft.Options.TickInterval unset, setting to %v", d.Options.TickInterval)
		c.Options.TickInterval = d.Options.TickInterval
	}
	if c.Options.ElectionTick == 0 {
		logger.Infof("Orderer.EtcdRaft.Options.ElectionTick unset, setting to %v", d.Options.ElectionTick)
		c.Options.ElectionTick = d.Options.ElectionTick
	}
	if c.Options.HeartbeatTick == 0 {
		logger.Infof("Orderer.EtcdRaft.Options.HeartbeatTick unset, setting to %v",
			d.Options.HeartbeatTick)
		c.Options.HeartbeatTick = d.Options.HeartbeatTick
	}
	if c.Options.MaxInflightBlocks == 0 {
		logger.Infof("Orderer.EtcdRaft.Options.MaxInflightBlocks unset, setting to %v",
			d.Options.MaxInflightBlocks)
		c.Options.MaxInflightBlocks = d.Options.MaxInflightBlocks
	}
	if c.Options.SnapshotIntervalSize == 0 {
		logger.Infof("Orderer.EtcdRaft.Options.SnapshotIntervalSize unset, setting to %v",
			d.Options.SnapshotIntervalSize)
		c.Options.SnapshotIntervalSize = d.Options.SnapshotIntervalSize
	}
	if len(c.Consenters) == 0 {
		logger.Panicf("%s configuration did not specify any consenter", EtcdRaft)
	}
	if _, err := time.ParseDuration(c.Options.TickInterval); err != nil {
		logger.Panicf("Etcdraft TickInterval (%s) must be in time duration format", c.Options.TickInterval)
	}

	// validate the specified members for Options
	if c.Options.ElectionTick <= c.Options.HeartbeatTick {
		logger.Panic("election tick must be greater than heartbeat tick")
	}

	for _, consenter := range c.GetConsenters() {
		if consenter.Host == "" {
			logger.Panicf("consenter info in %s configuration did not specify host", EtcdRaft)
		}
		if consenter.Port == 0 {
			logger.Panicf("consenter info in %s configuration did not specify port", EtcdRaft)
		}
		if consenter.ClientTlsCert == nil {
			logger.Panicf("consenter info in %s configuration did not specify client TLS cert", EtcdRaft)
		}
		if consenter.ServerTlsCert == nil {
			logger.Panicf("consenter info in %s configuration did not specify server TLS cert", EtcdRaft)
		}
		clientCertPath := string(consenter.GetClientTlsCert())
		cf.TranslatePathInPlace(configDir, &clientCertPath)
		consenter.ClientTlsCert = []byte(clientCertPath)
		serverCertPath := string(consenter.GetServerTlsCert())
		cf.TranslatePathInPlace(configDir, &serverCertPath)
		consenter.ServerTlsCert = []byte(serverCertPath)
	}
}

func (ord *Orderer) translateConsenterMapping(configDir, ordererType string) {
	if len(ord.ConsenterMapping) == 0 {
		logger.Panicf("%s configuration did not specify any consenter", ordererType)
	}

	for _, c := range ord.ConsenterMapping {
		if c.Host == "" {
			logger.Panicf("consenter info in %s configuration did not specify host", ordererType)
		}
		if c.Port == 0 {
			logger.Panicf("consenter info in %s configuration did not specify port", ordererType)
		}
		if c.ClientTLSCert == "" {
			logger.Panicf("consenter info in %s configuration did not specify client TLS cert", ordererType)
		}
		if c.ServerTLSCert == "" {
			logger.Panicf("consenter info in %s configuration did not specify server TLS cert", ordererType)
		}
		if len(c.MSPID) == 0 {
			logger.Panicf("consenter info in %s configuration did not specify MSP ID", ordererType)
		}
		if len(c.Identity) == 0 {
			logger.Panicf("consenter info in %s configuration did not specify identity certificate", ordererType)
		}

		cf.TranslatePathInPlace(configDir, &c.ClientTLSCert)
		cf.TranslatePathInPlace(configDir, &c.ServerTLSCert)
		cf.TranslatePathInPlace(configDir, &c.Identity)
	}
}

func translatePaths(configDir string, org *Organization) {
	cf.TranslatePathInPlace(configDir, &org.MSPDir)
}

// configCache stores marshalled bytes of config structures that produced from
// EnhancedExactUnmarshal. Cache key is the path of the configuration file that was used.
type configCache struct {
	mutex sync.Mutex
	cache map[string][]byte
}

var cache = &configCache{
	cache: make(map[string][]byte),
}

// load loads the TopLevel config structure from configCache.
// if not successful, it unmarshal a config file, and populate configCache
// with marshaled TopLevel struct.
func (c *configCache) load(config *viperutil.ConfigParser, configPath string) (*TopLevel, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	conf := &TopLevel{}
	serializedConf, ok := c.cache[configPath]
	logger.Debugf("Loading configuration from cache: %t", ok)
	if !ok {
		err := config.EnhancedExactUnmarshal(conf)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling config into struct: %w", err)
		}

		serializedConf, err = json.Marshal(conf)
		if err != nil {
			return nil, err
		}
		c.cache[configPath] = serializedConf
	}

	err := json.Unmarshal(serializedConf, conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}
