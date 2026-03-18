/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"testing"

	config_protos "github.com/hyperledger/fabric-x-orderer/config/protos"
	"github.com/stretchr/testify/require"
)

func TestValidatePartyModification(t *testing.T) {
	curr := &config_protos.PartyConfig{
		PartyID: 1,
		CACerts: [][]byte{[]byte("cert")},
		BatchersConfig: []*config_protos.BatcherNodeConfig{
			{ShardID: 1},
			{ShardID: 2},
		},
	}

	t.Run("batcher shard count changed", func(t *testing.T) {
		next := &config_protos.PartyConfig{
			PartyID: 1,
			CACerts: [][]byte{[]byte("cert")},
			BatchersConfig: []*config_protos.BatcherNodeConfig{
				{ShardID: 1},
			},
		}

		modified, err := validatePartyModification(curr, next)
		require.Error(t, err)
		require.Contains(t, err.Error(), "batcher shards cannot change")
		require.False(t, modified)
	})

	t.Run("batcher shard id changed", func(t *testing.T) {
		next := &config_protos.PartyConfig{
			PartyID: 1,
			CACerts: [][]byte{[]byte("cert")},
			BatchersConfig: []*config_protos.BatcherNodeConfig{
				{ShardID: 1},
				{ShardID: 3},
			},
		}

		modified, err := validatePartyModification(curr, next)
		require.Error(t, err)
		require.Contains(t, err.Error(), "batcher shard IDs cannot change")
		require.False(t, modified)
	})

	t.Run("next batcher config is nil", func(t *testing.T) {
		next := &config_protos.PartyConfig{
			PartyID: 1,
			CACerts: [][]byte{[]byte("cert")},
			BatchersConfig: []*config_protos.BatcherNodeConfig{
				{ShardID: 1},
				nil,
			},
		}

		_, err := validatePartyModification(curr, next)
		require.Error(t, err)
		require.Contains(t, err.Error(), "batcher config is nil")
	})

	t.Run("certificate changed", func(t *testing.T) {
		next := &config_protos.PartyConfig{
			PartyID: 1,
			CACerts: [][]byte{[]byte("new-cert")},
			BatchersConfig: []*config_protos.BatcherNodeConfig{
				{ShardID: 1},
				{ShardID: 2},
			},
		}

		modified, err := validatePartyModification(curr, next)
		require.NoError(t, err)
		require.True(t, modified)
	})
}
