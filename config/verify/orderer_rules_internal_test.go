/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/common/policydsl"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/stretchr/testify/require"
)

func TestValidatePartyModification(t *testing.T) {
	curr := &ordererpb.PartyConfig{
		PartyID: 1,
		CACerts: [][]byte{[]byte("cert")},
		BatchersConfig: []*ordererpb.BatcherNodeConfig{
			{ShardID: 1},
			{ShardID: 2},
		},
	}

	t.Run("batcher shard count changed", func(t *testing.T) {
		next := &ordererpb.PartyConfig{
			PartyID: 1,
			CACerts: [][]byte{[]byte("cert")},
			BatchersConfig: []*ordererpb.BatcherNodeConfig{
				{ShardID: 1},
			},
		}

		modified, err := validatePartyModification(curr, next)
		require.Error(t, err)
		require.Contains(t, err.Error(), "batcher shards cannot change")
		require.False(t, modified)
	})

	t.Run("batcher shard id changed", func(t *testing.T) {
		next := &ordererpb.PartyConfig{
			PartyID: 1,
			CACerts: [][]byte{[]byte("cert")},
			BatchersConfig: []*ordererpb.BatcherNodeConfig{
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
		next := &ordererpb.PartyConfig{
			PartyID: 1,
			CACerts: [][]byte{[]byte("cert")},
			BatchersConfig: []*ordererpb.BatcherNodeConfig{
				{ShardID: 1},
				nil,
			},
		}

		_, err := validatePartyModification(curr, next)
		require.Error(t, err)
		require.Contains(t, err.Error(), "batcher config is nil")
	})

	t.Run("certificate changed", func(t *testing.T) {
		next := &ordererpb.PartyConfig{
			PartyID: 1,
			CACerts: [][]byte{[]byte("new-cert")},
			BatchersConfig: []*ordererpb.BatcherNodeConfig{
				{ShardID: 1},
				{ShardID: 2},
			},
		}

		modified, err := validatePartyModification(curr, next)
		require.NoError(t, err)
		require.True(t, modified)
	})
}

func TestValidateBlockValidationPolicy(t *testing.T) {
	consenters := []*common.Consenter{
		{MspId: "org1", Identity: []byte("identity1")},
		{MspId: "org2", Identity: []byte("identity2")},
		{MspId: "org3", Identity: []byte("identity3")},
		{MspId: "org4", Identity: []byte("identity4")},
	}

	t.Run("block validation policy is nil", func(t *testing.T) {
		err := validateBlockValidationPolicy(nil, consenters)
		require.ErrorContains(t, err, "block validation policy is missing from orderer group")
	})

	t.Run("valid block validation policy", func(t *testing.T) {
		err := validateBlockValidationPolicy(buildBlockValidationPolicy(consenters), consenters)
		require.NoError(t, err)
	})

	t.Run("block validation policy does not match consenters", func(t *testing.T) {
		wrongConsenters := []*common.Consenter{
			{MspId: "org1", Identity: []byte("wrong-identity")},
			{MspId: "org2", Identity: []byte("identity2")},
			{MspId: "org3", Identity: []byte("identity3")},
			{MspId: "org4", Identity: []byte("identity4")},
		}

		err := validateBlockValidationPolicy(buildBlockValidationPolicy(wrongConsenters), consenters)
		require.ErrorContains(t, err, "unexpected identity in policy")
	})
}

func buildBlockValidationPolicy(consenters []*common.Consenter) *common.ConfigPolicy {
	n := len(consenters)
	f := (n - 1) / 3

	identities := make([]*msp.MSPPrincipal, 0, n)
	signedBy := make([]*common.SignaturePolicy, 0, n)

	for i, consenter := range consenters {
		signedBy = append(signedBy, policydsl.SignedBy(int32(i)))
		identities = append(identities, &msp.MSPPrincipal{
			PrincipalClassification: msp.MSPPrincipal_IDENTITY,
			Principal: protoutil.MarshalOrPanic(
				msppb.NewIdentity(consenter.MspId, consenter.Identity),
			),
		})
	}

	return &common.ConfigPolicy{
		Policy: &common.Policy{
			Type: int32(common.Policy_SIGNATURE),
			Value: protoutil.MarshalOrPanic(&common.SignaturePolicyEnvelope{
				Rule:       policydsl.NOutOf(int32(policies.ComputeBFTQuorum(n, f)), signedBy),
				Identities: identities,
			}),
		},
	}
}
