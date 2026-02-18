/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"time"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

//go:generate counterfeiter -o mocks/orderer_rules.go . OrdererRules
type OrdererRules interface {
	ValidateNewConfig(envelope *common.Envelope, bccsp bccsp.BCCSP, partyID types.PartyID) error
	ValidateTransition(current channelconfig.Resources, next *common.Envelope, bccsp bccsp.BCCSP) error
}

type DefaultOrdererRules struct{}

// ValidateNewConfig validates that the rules of the new config are valid before it is applied.
func (or *DefaultOrdererRules) ValidateNewConfig(envelope *common.Envelope, bccsp bccsp.BCCSP, partyID types.PartyID) error {
	bundle, err := channelconfig.NewBundleFromEnvelope(envelope, bccsp)
	if err != nil {
		return errors.Wrap(err, "failed to create bundle from new envelope config")
	}

	ordererConfig, exists := bundle.OrdererConfig()
	if !exists {
		return errors.Errorf("orderer entry in the config block is empty")
	}

	sharedConfig := &config_protos.SharedConfig{}
	if err := proto.Unmarshal(ordererConfig.ConsensusMetadata(), sharedConfig); err != nil {
		return errors.Wrap(err, "failed to unmarshal consensus metadata")
	}

	// 1. Validate batch timeouts
	if err := validateBatchTimeout(sharedConfig.BatchingConfig.BatchTimeouts); err != nil {
		return err
	}

	// 2. Validate that BatchSize AbsoluteMaxBytes is consistent between shared and orderer config
	if sharedConfig.BatchingConfig.BatchSize.AbsoluteMaxBytes != ordererConfig.BatchSize().AbsoluteMaxBytes {
		return errors.Errorf("batch size differs between shared and orderer config")
	}

	// 3. Validate that BFT RequestMaxBytes is positive and >= BatchingConfig RequestMaxBytes,
	// to ensure config requests accepted by the router are not rejected by SmartBFT.
	bftConfig := sharedConfig.ConsensusConfig.SmartBFTConfig
	if sharedConfig.BatchingConfig.RequestMaxBytes <= 0 {
		return errors.Errorf("invalid BatchingConfig RequestMaxBytes must be greater than zero")
	}
	if bftConfig.RequestMaxBytes < sharedConfig.BatchingConfig.RequestMaxBytes {
		return errors.Errorf("smartbft RequestMaxBytes must be equal or greater than BatchingConfig RequestMaxBytes")
	}

	// 4. Validate BFT parameters
	if err := ValidateSmartBFTConfig(uint64(partyID), bftConfig); err != nil {
		return errors.Wrap(err, "smartbft config validation failed")
	}
	// TODO: Validate endpoints in the Orderer Org definitions.
	// TODO: Validate certificates.
	// TODO: Validate consenter mapping.
	// TODO: Validate block validation policy.

	return nil
}

// ValidateTransition validates ordering service config transition rules
// from the current config to the next config.
func (DefaultOrdererRules) ValidateTransition(current channelconfig.Resources, next *common.Envelope, bccsp bccsp.BCCSP) error {
	// extract current shared config
	currOrdererCfg, ok := current.OrdererConfig()
	if !ok {
		return errors.New("no orderer config found")
	}

	currCfg := &config_protos.SharedConfig{}
	if err := proto.Unmarshal(currOrdererCfg.ConsensusMetadata(), currCfg); err != nil {
		return errors.Wrap(err, "failed to unmarshal current consensus metadata")
	}

	// extract next shared config
	nextBundle, err := channelconfig.NewBundleFromEnvelope(next, bccsp)
	if err != nil {
		return errors.Wrap(err, "failed to create bundle from next envelope config")
	}

	nextOrdererCfg, ok := nextBundle.OrdererConfig()
	if !ok {
		return errors.New("orderer entry in the config block is empty")
	}

	nextCfg := &config_protos.SharedConfig{}
	if err := proto.Unmarshal(nextOrdererCfg.ConsensusMetadata(), nextCfg); err != nil {
		return errors.Wrap(err, "failed to unmarshal next consensus metadata")
	}

	currMap := make(map[uint32]*config_protos.PartyConfig)
	nextMap := make(map[uint32]*config_protos.PartyConfig)

	for _, p := range currCfg.PartiesConfig {
		currMap[p.PartyID] = p
	}
	for _, p := range nextCfg.PartiesConfig {
		nextMap[p.PartyID] = p
	}

	// 1. Validate that at most one party is added
	added := 0
	var newID uint32
	for id := range nextMap {
		if _, exists := currMap[id]; !exists {
			added++
			newID = id
			if added > 1 {
				return errors.New("more than one party added in config tx")
			}
		}
	}

	// 2. Validate MaxPartyID rules in the next config:
	// when a party is added, next MaxPartyID equals the new PartyID, which equals current MaxPartyID+1.
	// otherwise, next MaxPartyID equals current MaxPartyID.
	if added == 1 {
		if newID <= currCfg.MaxPartyID {
			return errors.Errorf("proposed party ID %d must be greater than previous MaxPartyID %d", newID, currCfg.MaxPartyID)
		}
		if nextCfg.MaxPartyID != newID {
			return errors.Errorf("proposed MaxPartyID %d must equal the newly added PartyID %d", nextCfg.MaxPartyID, newID)
		}
		if nextCfg.MaxPartyID != currCfg.MaxPartyID+1 {
			return errors.Errorf("proposed MaxPartyID %d must be greater than previous MaxPartyID %d by one", nextCfg.MaxPartyID, currCfg.MaxPartyID)
		}
	} else {
		if nextCfg.MaxPartyID != currCfg.MaxPartyID {
			return errors.Errorf("MaxPartyID cannot change if no new party is added (current=%d, next=%d)", currCfg.MaxPartyID, nextCfg.MaxPartyID)
		}
	}

	// 3. Validate that at most one party is removed
	removed := 0
	for id := range currMap {
		if _, exists := nextMap[id]; !exists {
			removed++
			if removed > 1 {
				return errors.New("more than one party removed in config tx")
			}
		}
	}

	// Ensure at most one membership change per config tx
	if added+removed > 1 {
		return errors.Errorf("only one party can be changed in a config tx (added=%d, removed=%d)", added, removed)
	}

	// TODO: Validate party modifications (only one certificate / endpoint change in a config tx).
	// TODO: Validate ordering service remains live after the change (no quorum loss / no liveness loss).

	return nil
}

func validateBatchTimeout(bt *config_protos.BatchTimeouts) error {
	batchCreation, err := time.ParseDuration(bt.BatchCreationTimeout)
	if err != nil {
		return err
	}
	if batchCreation <= 0 {
		return errors.Errorf("attempted to set the batch creation timeout to a non-positive value: %s", batchCreation)
	}

	firstStrike, err := time.ParseDuration(bt.FirstStrikeThreshold)
	if err != nil {
		return err
	}
	if firstStrike <= 0 {
		return errors.Errorf("attempted to set the first strike threshold to a non-positive value: %s", firstStrike)
	}

	secondStrike, err := time.ParseDuration(bt.SecondStrikeThreshold)
	if err != nil {
		return err
	}
	if secondStrike <= 0 {
		return errors.Errorf("attempted to set the second strike threshold to a non-positive value: %s", secondStrike)
	}

	autoRemove, err := time.ParseDuration(bt.AutoRemoveTimeout)
	if err != nil {
		return err
	}
	if autoRemove <= 0 {
		return errors.Errorf("attempted to set auto remove timeout to a non-positive value: %s", autoRemove)
	}
	return nil
}

func ValidateSmartBFTConfig(id uint64, cfg *config_protos.SmartBFTConfig) error {
	if cfg == nil {
		return errors.New("smartbft config is nil")
	}

	c := smartbft_types.DefaultConfig
	c.SelfID = id
	c.RequestBatchMaxCount = cfg.RequestBatchMaxCount
	c.RequestBatchMaxBytes = cfg.RequestBatchMaxBytes
	c.IncomingMessageBufferSize = cfg.IncomingMessageBufferSize
	c.RequestPoolSize = cfg.RequestPoolSize
	c.LeaderHeartbeatCount = cfg.LeaderHeartbeatCount
	c.NumOfTicksBehindBeforeSyncing = cfg.NumOfTicksBehindBeforeSyncing
	c.SyncOnStart = cfg.SyncOnStart
	c.SpeedUpViewChange = cfg.SpeedUpViewChange

	var err error
	if c.RequestBatchMaxInterval, err = time.ParseDuration(cfg.RequestBatchMaxInterval); err != nil {
		return errors.Wrap(err, "invalid smartbft config RequestBatchMaxInterval")
	}
	if c.RequestForwardTimeout, err = time.ParseDuration(cfg.RequestForwardTimeout); err != nil {
		return errors.Wrap(err, "invalid smartbft config RequestForwardTimeout")
	}
	if c.RequestComplainTimeout, err = time.ParseDuration(cfg.RequestComplainTimeout); err != nil {
		return errors.Wrap(err, "invalid smartbft config RequestComplainTimeout")
	}
	if c.RequestAutoRemoveTimeout, err = time.ParseDuration(cfg.RequestAutoRemoveTimeout); err != nil {
		return errors.Wrap(err, "invalid smartbft config RequestAutoRemoveTimeout")
	}
	if c.ViewChangeResendInterval, err = time.ParseDuration(cfg.ViewChangeResendInterval); err != nil {
		return errors.Wrap(err, "invalid smartbft config ViewChangeResendInterval")
	}
	if c.ViewChangeTimeout, err = time.ParseDuration(cfg.ViewChangeTimeout); err != nil {
		return errors.Wrap(err, "invalid smartbft config ViewChangeTimeout")
	}
	if c.LeaderHeartbeatTimeout, err = time.ParseDuration(cfg.LeaderHeartbeatTimeout); err != nil {
		return errors.Wrap(err, "invalid smartbft config LeaderHeartbeatTimeout")
	}
	if c.CollectTimeout, err = time.ParseDuration(cfg.CollectTimeout); err != nil {
		return errors.Wrap(err, "invalid smartbft config CollectTimeout")
	}
	if c.RequestPoolSubmitTimeout, err = time.ParseDuration(cfg.RequestPoolSubmitTimeout); err != nil {
		return errors.Wrap(err, "invalid smartbft config RequestPoolSubmitTimeout")
	}
	c.LeaderRotation = cfg.LeaderRotation
	c.DecisionsPerLeader = cfg.DecisionsPerLeader
	c.RequestMaxBytes = cfg.RequestMaxBytes

	if err := c.Validate(); err != nil {
		return err
	}

	return nil
}
