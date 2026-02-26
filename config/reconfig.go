/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"bytes"
	"errors"
	"net"
	"strconv"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
)

type NodeConfig interface {
	GetHost() string
	GetPort() uint32
	GetTlsCert() []byte
}

type NodeConfigWithSign interface {
	NodeConfig
	GetSignCert() []byte
}

// IsPartyEvicted returns true if the given party does not appear in the provided configuration.
func IsPartyEvicted(partyID types.PartyID, newConfig *Configuration) (bool, error) {
	newSharedPartyConfig, err := FindParty(partyID, newConfig)
	if err != nil {
		return false, err
	}
	return newSharedPartyConfig == nil, nil
}

// FindParty returns the PartyConfig associated with the given partyID from the shared configuration.
// It returns nil if the party is not found and returns error if the provided configuration is nil or incomplete.
func FindParty(partyID types.PartyID, config *Configuration) (*config_protos.PartyConfig, error) {
	if config == nil {
		return nil, errors.New("the provided configuration is nil")
	}
	if config.SharedConfig == nil {
		return nil, errors.New("the provided configuration has nil shared config")
	}
	for _, party := range config.SharedConfig.PartiesConfig {
		if types.PartyID(party.PartyID) == partyID {
			return party, nil
		}
	}
	return nil, nil
}

// IsNodeConfigChangeRestartRequired reports whether a restart is required due to configuration updates.
// A restart is required if any of the following parts were updated:
//   - host or port
//   - TLS certificate
//   - sign certificate (this is checked if both configs implement NodeConfigWithSign)
//
// Both arguments must represent the same node type and be non-nil.
func IsNodeConfigChangeRestartRequired(currentConfig, newConfig NodeConfig) (bool, error) {
	if currentConfig == nil {
		return false, errors.New("current config is nil")
	}

	if newConfig == nil {
		return false, errors.New("new config is nil")
	}

	extendedCurrConfig, currOK := currentConfig.(NodeConfigWithSign)
	extendedNewConfig, newOK := newConfig.(NodeConfigWithSign)
	if currOK != newOK {
		return false, errors.New("type mismatch: current node config and new node config are not from the same type")
	}

	currAddr := net.JoinHostPort(currentConfig.GetHost(), strconv.Itoa(int(currentConfig.GetPort())))
	newAddr := net.JoinHostPort(newConfig.GetHost(), strconv.Itoa(int(newConfig.GetPort())))

	if currAddr != newAddr || !bytes.Equal(currentConfig.GetTlsCert(), newConfig.GetTlsCert()) {
		return true, nil
	}

	if currOK && !bytes.Equal(extendedCurrConfig.GetSignCert(), extendedNewConfig.GetSignCert()) {
		return true, nil
	}

	return false, nil
}
