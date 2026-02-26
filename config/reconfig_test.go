/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/config"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/stretchr/testify/require"
)

func TestIsPartyEvicted(t *testing.T) {
	partyID := types.PartyID(1)
	partyConfig := &protos.PartyConfig{
		PartyID: 2,
	}
	conf := &config.Configuration{
		SharedConfig: &protos.SharedConfig{
			PartiesConfig: []*protos.PartyConfig{partyConfig},
			MaxPartyID:    1,
		},
	}

	isPartyEvicted, err := config.IsPartyEvicted(partyID, conf)
	require.NoError(t, err)
	require.True(t, isPartyEvicted)
	partyID = types.PartyID(2)
	isPartyEvicted, err = config.IsPartyEvicted(partyID, conf)
	require.NoError(t, err)
	require.False(t, isPartyEvicted)
}

func TestIsNodeConfigChangeRestartRequired(t *testing.T) {
	// Test Router
	currRouterConfig := &protos.RouterNodeConfig{
		Host:    "127.0.0.1",
		Port:    5060,
		TlsCert: []byte("cert"),
	}

	newRouterConfig := &protos.RouterNodeConfig{
		Host:    "127.0.0.1",
		Port:    5060,
		TlsCert: []byte("cert"),
	}

	isRestartRequired, err := config.IsNodeConfigChangeRestartRequired(currRouterConfig, newRouterConfig)
	require.NoError(t, err)
	require.False(t, isRestartRequired)

	newRouterConfig.Port = 5070

	isRestartRequired, err = config.IsNodeConfigChangeRestartRequired(currRouterConfig, newRouterConfig)
	require.NoError(t, err)
	require.True(t, isRestartRequired)

	newRouterConfig.Port = 5060
	newRouterConfig.TlsCert = []byte("TLSCert")

	isRestartRequired, err = config.IsNodeConfigChangeRestartRequired(currRouterConfig, newRouterConfig)
	require.NoError(t, err)
	require.True(t, isRestartRequired)

	// Test Batcher
	currBatcherConfig := &protos.BatcherNodeConfig{
		ShardID:  1,
		Host:     "127.0.0.1",
		Port:     5060,
		SignCert: []byte("SignCert"),
		TlsCert:  []byte("TLSCert"),
	}

	newBatcherConfig := &protos.BatcherNodeConfig{
		ShardID:  1,
		Host:     "127.0.0.1",
		Port:     5060,
		SignCert: []byte("SignCert"),
		TlsCert:  []byte("TLSCert"),
	}

	isRestartRequired, err = config.IsNodeConfigChangeRestartRequired(currBatcherConfig, newBatcherConfig)
	require.NoError(t, err)
	require.False(t, isRestartRequired)

	newBatcherConfig.SignCert = []byte("NewSignCert")
	isRestartRequired, err = config.IsNodeConfigChangeRestartRequired(currBatcherConfig, newBatcherConfig)
	require.NoError(t, err)
	require.True(t, isRestartRequired)
}
