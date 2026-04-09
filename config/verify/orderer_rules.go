/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"slices"
	"time"

	smartbft_types "github.com/hyperledger-labs/SmartBFT/pkg/types"
	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/types"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// partyChanges keeps information about membership changes introduced during configuration update.
type partyChanges struct {
	Added    []*config_protos.PartyConfig
	Removed  []*config_protos.PartyConfig
	Modified []*config_protos.PartyConfig
}

//go:generate counterfeiter -o mocks/orderer_rules.go . OrdererRules
type OrdererRules interface {
	ValidateNewConfig(envelope *common.Envelope, bccsp bccsp.BCCSP, partyID arma_types.PartyID) error
	ValidateTransition(current channelconfig.Resources, next *common.Envelope, bccsp bccsp.BCCSP) error
}

type DefaultOrdererRules struct{}

// ValidateNewConfig validates the new ordering service configuration before it is applied.
// It performs internal validation of the proposed config, including consistency
// checks for parameters defined in multiple places. The rules are as follows:
//
//  1. All batching timeouts must be valid durations and positive.
//  2. BatchSize.AbsoluteMaxBytes must match between OrdererConfig and SharedConfig.
//  3. SmartBFTConfig.RequestMaxBytes must be positive and >= SharedConfig.BatchingConfig.RequestMaxBytes.
//     This ensures config requests accepted by the router are not rejected by SmartBFT.
//  4. SmartBFTConfig must pass SmartBFT validation.
//  5. OrdererEndpoints for each organization must be defined, non-empty,
//     and include both "broadcast" and "deliver" roles.
//  6. ConsenterMapping must be consistent with the consenters defined in the shared config.
//  7. Each consenter in the ConsenterMapping must have a matching organization.
//
// TODO: Validate that ca certificates in the sharedConfig are the same as in the ordererOrganization ca certificates.
// TODO: Validate BlockValidationPolicy.
func (or *DefaultOrdererRules) ValidateNewConfig(envelope *common.Envelope, bccsp bccsp.BCCSP, partyID arma_types.PartyID) error {
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
	if sharedConfig.BatchingConfig == nil {
		return errors.New("batching config is nil")
	}
	if sharedConfig.ConsensusConfig == nil {
		return errors.New("consensus config is nil")
	}

	// 1.
	if err := validateBatchTimeout(sharedConfig.BatchingConfig.BatchTimeouts); err != nil {
		return err
	}

	// 2.
	if sharedConfig.BatchingConfig.BatchSize.AbsoluteMaxBytes != ordererConfig.BatchSize().AbsoluteMaxBytes {
		return errors.Errorf("batch size differs between shared and orderer config")
	}

	// 3.
	if sharedConfig.BatchingConfig.RequestMaxBytes <= 0 {
		return errors.Errorf("BatchingConfig RequestMaxBytes must be greater than zero")
	}

	bftConfig := sharedConfig.ConsensusConfig.SmartBFTConfig
	if bftConfig == nil {
		return errors.New("smartbft config is nil")
	}
	if bftConfig.RequestMaxBytes < sharedConfig.BatchingConfig.RequestMaxBytes {
		return errors.Errorf("smartbft RequestMaxBytes must be equal or greater than BatchingConfig RequestMaxBytes")
	}

	// 4.
	if err := validateSmartBFTConfig(uint64(partyID), bftConfig); err != nil {
		return errors.Wrap(err, "smartbft config validation failed")
	}

	// 5.
	for _, org := range ordererConfig.Organizations() {
		if err := validateOrdererOrgEndpoints(org.Endpoints()); err != nil {
			return errors.Wrapf(err, "invalid endpoints for orderer organization %s", org.Name())
		}
	}

	// 6.
	if err := validateConsenterConsistency(ordererConfig.Consenters(), sharedConfig.PartiesConfig); err != nil {
		return errors.Wrap(err, "consenter mapping is inconsistent with shared config parties")
	}

	// 7.
	orgMap := make(map[string]channelconfig.OrdererOrg, len(ordererConfig.Organizations()))
	for _, org := range ordererConfig.Organizations() {
		if org == nil {
			return errors.New("orderer organization is nil")
		}
		orgMap[org.MSPID()] = org
	}

	for _, consenter := range ordererConfig.Consenters() {
		if consenter == nil {
			return errors.New("consenter config is nil")
		}

		if _, exists := orgMap[consenter.MspId]; !exists {
			return errors.Errorf("missing orderer organization for party %d", consenter.Id)
		}
	}

	return nil
}

// ValidateTransition validates ordering service config transition rules from the
// current configuration to the next proposed configuration, before
// the new configuration is applied. The rules are as follows:
//
//  1. At most one party can be added in a config tx.
//  2. MaxPartyID transition rules:
//     - If a party is added, its PartyID must equal current MaxPartyID + 1.
//     - If a party is added, next MaxPartyID must equal that PartyID.
//     - If no party is added, MaxPartyID must remain unchanged.
//     This ensures PartyIDs are strictly increasing and never reused.
//  3. At most one party can be removed in a config tx.
//  4. At most one party can be modified in a config tx.
//  5. Only one membership change is allowed per config tx (add, remove, or modify).
//  6. Certificate validation:
//     - validate certificate chain of trust for all parties while ignoring expiration.
//     - enforce expiration checks for newly added and modified parties.
//
// TODO: Validate ordering service remains live after the change (no quorum loss / no liveness loss).
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
		if p == nil {
			return errors.New("party config is nil in current shared config")
		}
		currMap[p.PartyID] = p
	}
	for _, p := range nextCfg.PartiesConfig {
		if p == nil {
			return errors.New("party config is nil in next shared config")
		}
		nextMap[p.PartyID] = p
	}

	// Compute party changes
	changes, err := computePartyChanges(currMap, nextMap)
	if err != nil {
		return err
	}

	// 1.
	if len(changes.Added) > 1 {
		return errors.New("more than one party added in config tx")
	}

	// 2.
	if len(changes.Added) == 1 {
		newID := changes.Added[0].PartyID
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

	// 3.
	if len(changes.Removed) > 1 {
		return errors.New("more than one party removed in config tx")
	}

	// 4.
	if len(changes.Modified) > 1 {
		return errors.New("more than one party modified in config tx")
	}

	// 5.
	if len(changes.Added)+len(changes.Removed)+len(changes.Modified) > 1 {
		return errors.Errorf("only one party can be changed in a config tx (added=%d, removed=%d, modified=%d)", len(changes.Added), len(changes.Removed), len(changes.Modified))
	}

	// 6.
	for _, party := range nextMap {
		if err := validatePartyCertificates(party, true); err != nil {
			return errors.Wrapf(err, "certificate validation failed for party ID %d", party.PartyID)
		}
	}

	for _, party := range changes.Added {
		if err := validatePartyCertificates(party, false); err != nil {
			return errors.Wrapf(err, "certificate validation failed for added party ID %d", party.PartyID)
		}
	}

	for _, party := range changes.Modified {
		if err := validatePartyCertificates(party, false); err != nil {
			return errors.Wrapf(err, "certificate validation failed for modified party ID %d", party.PartyID)
		}
	}

	return nil
}

func validateBatchTimeout(bt *config_protos.BatchTimeouts) error {
	if bt == nil {
		return errors.New("batch timeouts are nil")
	}
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

func validateSmartBFTConfig(id uint64, cfg *config_protos.SmartBFTConfig) error {
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

func validateOrdererOrgEndpoints(endpoints []string) error {
	if len(endpoints) == 0 {
		return errors.New("endpoints are empty")
	}

	hasBroadcast := false
	hasDeliver := false
	for _, raw := range endpoints {
		ep, err := types.ParseOrdererEndpoint(raw)
		if err != nil {
			return err
		}
		if slices.Contains(ep.API, types.Broadcast) {
			hasBroadcast = true
		}
		if slices.Contains(ep.API, types.Deliver) {
			hasDeliver = true
		}
	}

	if !hasBroadcast {
		return errors.New("missing broadcast endpoint")
	}
	if !hasDeliver {
		return errors.New("missing deliver endpoint")
	}

	return nil
}

func validateConsenterConsistency(consenters []*common.Consenter, parties []*config_protos.PartyConfig) error {
	if len(consenters) != len(parties) {
		return errors.Errorf("number of parties in Orderer consenters mapping (%d) does not match number of parties in Shared config (%d)", len(consenters), len(parties))
	}

	partiesMap := make(map[uint32]*config_protos.PartyConfig)
	for _, p := range parties {
		if p == nil {
			return errors.New("party config is nil in shared config")
		}
		partiesMap[p.PartyID] = p
	}

	for _, consenter := range consenters {
		if consenter == nil {
			return errors.New("consenter config is nil in shared config")
		}
		party, exists := partiesMap[consenter.Id]
		if !exists {
			return errors.Errorf("party ID %d missing from shared config", consenter.Id)
		}
		if party.ConsenterConfig == nil {
			return errors.Errorf("consenter config missing in shared config for party %d", consenter.Id)
		}
		nodeCfg := party.ConsenterConfig
		if consenter.Host != nodeCfg.Host {
			return errors.Errorf("host mismatch for party %d: %s != %s", consenter.Id, consenter.Host, nodeCfg.Host)
		}

		if consenter.Port != nodeCfg.Port {
			return errors.Errorf("port mismatch for party %d: %d != %d", consenter.Id, consenter.Port, nodeCfg.Port)
		}

		if !slices.Equal(consenter.Identity, nodeCfg.SignCert) {
			return errors.Errorf("identity/sign_cert mismatch for party %d", consenter.Id)
		}

		if len(consenter.ServerTlsCert) > 0 && !slices.Equal(consenter.ServerTlsCert, nodeCfg.TlsCert) {
			return errors.Errorf("server TLS certificate mismatch for party %d", consenter.Id)
		}

		if len(consenter.ClientTlsCert) > 0 && !slices.Equal(consenter.ClientTlsCert, nodeCfg.TlsCert) {
			return errors.Errorf("client TLS certificate mismatch for party %d", consenter.Id)
		}
	}

	return nil
}

// computePartyChanges computes the membership changes between the current and next parties config.
func computePartyChanges(currMap map[uint32]*config_protos.PartyConfig, nextMap map[uint32]*config_protos.PartyConfig) (*partyChanges, error) {
	changes := &partyChanges{}

	// detect added
	for _, nextParty := range nextMap {
		if _, exists := currMap[nextParty.PartyID]; !exists {
			changes.Added = append(changes.Added, nextParty)
		}
	}

	// detect removed + modified
	for _, currParty := range currMap {
		nextParty, exists := nextMap[currParty.PartyID]
		if !exists {
			changes.Removed = append(changes.Removed, currParty)
			continue
		}

		changed, err := validatePartyModification(currParty, nextParty)
		if err != nil {
			return nil, errors.Errorf("invalid modification for party ID %d: %v", currParty.PartyID, err)
		}
		if changed {
			changes.Modified = append(changes.Modified, nextParty)
		}
	}

	return changes, nil
}

// validatePartyModification checks whether a party was modified.
// Certificates and endpoints may change, but batcher shard IDs must not.
func validatePartyModification(curr, next *config_protos.PartyConfig) (bool, error) {
	// no change
	if proto.Equal(curr, next) {
		return false, nil
	}

	if len(curr.BatchersConfig) != len(next.BatchersConfig) {
		return false, errors.Errorf("batcher shards cannot change for party %d", curr.PartyID)
	}

	nextShardIDs := make(map[uint32]struct{}, len(next.BatchersConfig))
	for _, b := range next.BatchersConfig {
		if b == nil {
			return false, errors.Errorf("batcher config is nil for party %d in next config", curr.PartyID)
		}
		nextShardIDs[b.ShardID] = struct{}{}
	}

	for _, b := range curr.BatchersConfig {
		if b == nil {
			return false, errors.Errorf("batcher config is nil for party %d in current config", curr.PartyID)
		}
		if _, ok := nextShardIDs[b.ShardID]; !ok {
			return false, errors.Errorf("batcher shard IDs cannot change for party %d", curr.PartyID)
		}
	}

	return true, nil
}
