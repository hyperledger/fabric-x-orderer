/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package verify

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp/factory"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/common/policydsl"
	fabmsp "github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	mspmock "github.com/hyperledger/fabric-x-orderer/common/msputils/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

	t.Run("nested NOutOf is rejected", func(t *testing.T) {
		policy := buildBlockValidationPolicy(
			consenters,
			policydsl.SignedBy(0),
			policydsl.SignedBy(1),
			policydsl.SignedBy(2),
			policydsl.NOutOf(0, nil),
		)
		err := validateBlockValidationPolicy(policy, consenters)
		require.ErrorContains(t, err, "policy rule 3 is not SignedBy")
	})

	t.Run("duplicate SignedBy is rejected", func(t *testing.T) {
		policy := buildBlockValidationPolicy(
			consenters,
			policydsl.SignedBy(0),
			policydsl.SignedBy(1),
			policydsl.SignedBy(2),
			policydsl.SignedBy(2),
		)
		err := validateBlockValidationPolicy(policy, consenters)
		require.ErrorContains(t, err, "duplicate SignedBy index 2")
	})

	t.Run("short rule list is rejected", func(t *testing.T) {
		policy := buildBlockValidationPolicy(
			consenters,
			policydsl.SignedBy(0),
			policydsl.SignedBy(1),
			policydsl.SignedBy(2),
		)
		err := validateBlockValidationPolicy(policy, consenters)
		require.ErrorContains(t, err, "unexpected number of policy rules: expected 4 got 3")
	})

	t.Run("out-of-range SignedBy index is rejected", func(t *testing.T) {
		policy := buildBlockValidationPolicy(
			consenters,
			policydsl.SignedBy(0),
			policydsl.SignedBy(1),
			policydsl.SignedBy(2),
			policydsl.SignedBy(4),
		)
		err := validateBlockValidationPolicy(policy, consenters)
		require.ErrorContains(t, err, "invalid SignedBy index 4")
	})

	t.Run("nil policy rule is rejected", func(t *testing.T) {
		policy := buildBlockValidationPolicy(
			consenters,
			policydsl.SignedBy(0),
			policydsl.SignedBy(1),
			policydsl.SignedBy(2),
			nil,
		)

		err := validateBlockValidationPolicy(policy, consenters)
		require.ErrorContains(t, err, "policy rule 3 is not SignedBy")
	})

	t.Run("root rule not NOutOf is rejected", func(t *testing.T) {
		policy := buildBlockValidationPolicy(consenters)

		envelope := &common.SignaturePolicyEnvelope{}
		require.NoError(t, proto.Unmarshal(policy.Policy.Value, envelope))

		envelope.Rule = policydsl.SignedBy(0)
		policy.Policy.Value = protoutil.MarshalOrPanic(envelope)

		err := validateBlockValidationPolicy(policy, consenters)
		require.ErrorContains(t, err, "block validation policy rule is not NOutOf")
	})
}

func TestValidateTLSCACertsConsistency(t *testing.T) {
	tlsRoot := []byte("tls-root")

	fakeMSP := &mspmock.MSP{}
	fakeMSP.GetTLSRootCertsStub = func() [][]byte {
		return [][]byte{tlsRoot}
	}
	fakeMSP.GetTLSIntermediateCertsStub = func() [][]byte {
		return nil
	}

	partyOrgMap := map[uint32]channelconfig.OrdererOrg{
		1: &testOrdererOrg{mspImpl: fakeMSP},
	}

	require.NoError(t, validateTLSCACertsConsistency([]*ordererpb.PartyConfig{{PartyID: 1, TLSCACerts: [][]byte{tlsRoot}}}, partyOrgMap))

	err := validateTLSCACertsConsistency([]*ordererpb.PartyConfig{{PartyID: 1, TLSCACerts: [][]byte{[]byte("other-cert")}}}, partyOrgMap)
	require.ErrorContains(t, err, "TLS CA certificates mismatch for party 1")
	require.ErrorContains(t, err, `certificate exists in orderer organization MSP but is missing from shared config: "tls-root"`)

	// verify error for empty org TLS CA list
	emptyMSP := &mspmock.MSP{}
	emptyMSP.GetTLSRootCertsStub = func() [][]byte {
		return nil
	}
	emptyMSP.GetTLSIntermediateCertsStub = func() [][]byte {
		return nil
	}

	err = validateTLSCACertsConsistency([]*ordererpb.PartyConfig{{PartyID: 1, TLSCACerts: [][]byte{tlsRoot}}}, map[uint32]channelconfig.OrdererOrg{1: &testOrdererOrg{mspImpl: emptyMSP}})
	require.ErrorContains(t, err, "orderer organization for party 1 has no TLS root or intermediate CA certificates")
}

func TestCertificateSetsEqual(t *testing.T) {
	t.Run("same certificates in different order", func(t *testing.T) {
		err := certificateSetsEqual(
			[][]byte{[]byte("cert1"), []byte("cert2")},
			[][]byte{[]byte("cert2"), []byte("cert1")},
		)
		require.NoError(t, err)
	})

	t.Run("certificate missing from shared config", func(t *testing.T) {
		err := certificateSetsEqual(
			[][]byte{[]byte("cert1")},
			[][]byte{[]byte("cert1"), []byte("cert2")},
		)
		require.ErrorContains(t, err, `certificate exists in orderer organization MSP but is missing from shared config: "cert2"`)
	})

	t.Run("certificate missing from orderer organization MSP", func(t *testing.T) {
		err := certificateSetsEqual(
			[][]byte{[]byte("cert1"), []byte("cert2")},
			[][]byte{[]byte("cert1")},
		)
		require.ErrorContains(t, err, `certificate exists in shared config but is missing from orderer organization MSP: "cert2"`)
	})
}

func TestValidatePartyAndShardIDs(t *testing.T) {
	partyWithShard := func(partyID, shardID uint32) []*ordererpb.PartyConfig {
		return []*ordererpb.PartyConfig{{
			PartyID:        partyID,
			BatchersConfig: []*ordererpb.BatcherNodeConfig{{ShardID: shardID}},
		}}
	}

	t.Run("boundary IDs are valid", func(t *testing.T) {
		require.NoError(t, validatePartyAndShardIDs(partyWithShard(1, 1), 1))
		require.NoError(t, validatePartyAndShardIDs(partyWithShard(math.MaxUint16, math.MaxUint16-1), math.MaxUint16))
	})

	t.Run("party ID out of range", func(t *testing.T) {
		for _, partyID := range []uint32{0, math.MaxUint16 + 1, math.MaxUint16 + 2} {
			err := validatePartyAndShardIDs(partyWithShard(partyID, 1), math.MaxUint16)
			require.EqualError(t, err, fmt.Sprintf("party ID %d out of range [1, %d]", partyID, math.MaxUint16))
		}
	})

	t.Run("shard ID out of range", func(t *testing.T) {
		for _, shardID := range []uint32{0, math.MaxUint16, math.MaxUint16 + 1, math.MaxUint16 + 2} {
			err := validatePartyAndShardIDs(partyWithShard(1, shardID), 1)
			require.EqualError(t, err, fmt.Sprintf("shard ID %d of party 1 out of range [1, %d]", shardID, math.MaxUint16-1))
		}
	})

	t.Run("party ID exceeds MaxPartyID", func(t *testing.T) {
		err := validatePartyAndShardIDs(partyWithShard(2, 1), 1)
		require.EqualError(t, err, "party ID 2 exceeds MaxPartyID 1")
	})

	t.Run("party config is nil", func(t *testing.T) {
		err := validatePartyAndShardIDs([]*ordererpb.PartyConfig{nil}, 1)
		require.EqualError(t, err, "party config is nil")
	})

	t.Run("batcher config is nil", func(t *testing.T) {
		err := validatePartyAndShardIDs([]*ordererpb.PartyConfig{{PartyID: 1, BatchersConfig: []*ordererpb.BatcherNodeConfig{nil}}}, 1)
		require.EqualError(t, err, "nil batcher config for party 1")
	})
}

func TestValidateSigningCAConsistency(t *testing.T) {
	now := time.Now()
	oldCA, oldKey, oldCAPEM := generateTestCAWithKey(t, now.Add(-time.Hour), now.Add(time.Hour))
	newCA, newKey, newCAPEM := generateTestCAWithKey(t, now.Add(-time.Hour), now.Add(time.Hour))

	oldNode := generateTestNodeCert(t, oldCA, oldKey, "orderer")
	newNode := generateTestNodeCert(t, newCA, newKey, "orderer")
	nodeWithoutOU := generateTestNodeCert(t, oldCA, oldKey)
	bothCAs := [][]byte{oldCAPEM, newCAPEM}

	tests := []struct {
		name          string
		trustedCAs    [][]byte
		partyCAs      [][]byte // defaults to trustedCAs
		pinnedCA      []byte
		adminPinnedCA []byte // defaults to pinnedCA
		nodeCert      []byte
		batcherCert   []byte
		wantErrors    []string
	}{
		{
			name:       "add new CA",
			trustedCAs: bothCAs,
			pinnedCA:   oldCAPEM,
			nodeCert:   oldNode,
		},
		{
			name:       "switch nodes and OU pin to new CA",
			trustedCAs: bothCAs,
			pinnedCA:   newCAPEM,
			nodeCert:   newNode,
		},
		{
			name:       "remove old CA",
			trustedCAs: [][]byte{newCAPEM},
			pinnedCA:   newCAPEM,
			nodeCert:   newNode,
		},
		{
			name:          "OU pins differ while both CAs are trusted",
			trustedCAs:    bothCAs,
			pinnedCA:      newCAPEM,
			adminPinnedCA: oldCAPEM,
			nodeCert:      newNode,
		},
		{
			name:          "admin OU pin missing from party CAs",
			trustedCAs:    bothCAs,
			partyCAs:      [][]byte{newCAPEM},
			pinnedCA:      newCAPEM,
			adminPinnedCA: oldCAPEM,
			nodeCert:      newNode,
			wantErrors:    []string{`the certificate of node OU "admin" is not one of the party's signing CA certificates`},
		},
		{
			name:       "nodes switched before OU pin",
			trustedCAs: bothCAs,
			pinnedCA:   oldCAPEM,
			nodeCert:   newNode,
			wantErrors: []string{"certifiersIdentifier does not match"},
		},
		{
			name:       "missing node OU",
			trustedCAs: [][]byte{oldCAPEM},
			pinnedCA:   oldCAPEM,
			nodeCert:   nodeWithoutOU,
			wantErrors: []string{"does not have an OU that resolves"},
		},
		{
			name:        "batcher does not match OU pin",
			trustedCAs:  bothCAs,
			pinnedCA:    oldCAPEM,
			nodeCert:    oldNode,
			batcherCert: newNode,
			wantErrors: []string{
				"failed to validate signing certificate of batcher",
				"certifiersIdentifier does not match",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodeOU := func(role string) *msp.FabricOUIdentifier {
				pin := tc.pinnedCA
				if role == "admin" && tc.adminPinnedCA != nil {
					pin = tc.adminPinnedCA
				}
				return &msp.FabricOUIdentifier{
					OrganizationalUnitIdentifier: role,
					Certificate:                  pin,
				}
			}

			mspConfig := &msppb.FabricMSPConfig{
				Name:      "Org1MSP",
				RootCerts: tc.trustedCAs,
				FabricNodeOus: &msp.FabricNodeOUs{
					Enable:              true,
					ClientOuIdentifier:  nodeOU("client"),
					PeerOuIdentifier:    nodeOU("peer"),
					AdminOuIdentifier:   nodeOU("admin"),
					OrdererOuIdentifier: nodeOU("orderer"),
				},
			}

			rawMSP := &msp.MSPConfig{
				Type:   int32(fabmsp.FABRIC),
				Config: protoutil.MarshalOrPanic(mspConfig),
			}

			fabMSP, err := channelconfig.NewMSPConfigHandler(
				fabmsp.MSPv3_0, factory.GetDefault(),
			).ProposeMSP(rawMSP)
			require.NoError(t, err)

			batcherCert := tc.nodeCert
			if tc.batcherCert != nil {
				batcherCert = tc.batcherCert
			}

			partyCAs := tc.trustedCAs
			if tc.partyCAs != nil {
				partyCAs = tc.partyCAs
			}

			party := &ordererpb.PartyConfig{
				PartyID:         1,
				CACerts:         partyCAs,
				RouterConfig:    &ordererpb.RouterNodeConfig{SignCert: tc.nodeCert},
				AssemblerConfig: &ordererpb.AssemblerNodeConfig{SignCert: tc.nodeCert},
				ConsenterConfig: &ordererpb.ConsenterNodeConfig{SignCert: tc.nodeCert},
				BatchersConfig:  []*ordererpb.BatcherNodeConfig{{ShardID: 1, SignCert: batcherCert}},
			}

			ordererGroup := &common.ConfigGroup{
				Groups: map[string]*common.ConfigGroup{
					"org1": {
						Values: map[string]*common.ConfigValue{
							channelconfig.MSPKey: {
								Value: protoutil.MarshalOrPanic(rawMSP),
							},
						},
					},
				},
			}

			org := &testOrdererOrg{
				mspImpl: fabMSP,
				name:    "org1",
				mspID:   "Org1MSP",
			}

			err = validateSigningCAConsistency(
				[]*ordererpb.PartyConfig{party},
				map[uint32]channelconfig.OrdererOrg{1: org},
				ordererGroup,
			)

			if len(tc.wantErrors) == 0 {
				require.NoError(t, err)
				return
			}

			for _, expected := range tc.wantErrors {
				require.ErrorContains(t, err, expected)
			}
		})
	}
}

type testOrdererOrg struct {
	mspImpl fabmsp.MSP
	name    string
	mspID   string
}

func (o *testOrdererOrg) Name() string        { return o.name }
func (o *testOrdererOrg) MSPID() string       { return o.mspID }
func (o *testOrdererOrg) MSP() fabmsp.MSP     { return o.mspImpl }
func (o *testOrdererOrg) Endpoints() []string { return nil }

func buildBlockValidationPolicy(consenters []*common.Consenter, rules ...*common.SignaturePolicy) *common.ConfigPolicy {
	n := len(consenters)
	f := (n - 1) / 3

	identities := make([]*msp.MSPPrincipal, 0, n)
	for _, consenter := range consenters {
		identities = append(identities, &msp.MSPPrincipal{
			PrincipalClassification: msp.MSPPrincipal_IDENTITY,
			Principal: protoutil.MarshalOrPanic(
				msppb.NewIdentity(consenter.MspId, consenter.Identity),
			),
		})
	}

	if len(rules) == 0 {
		rules = make([]*common.SignaturePolicy, n)
		for i := range consenters {
			rules[i] = policydsl.SignedBy(int32(i))
		}
	}

	return &common.ConfigPolicy{
		Policy: &common.Policy{
			Type: int32(common.Policy_SIGNATURE),
			Value: protoutil.MarshalOrPanic(&common.SignaturePolicyEnvelope{
				Rule:       policydsl.NOutOf(int32(policies.ComputeBFTQuorum(n, f)), rules),
				Identities: identities,
			}),
		},
	}
}

func generateTestNodeCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, ous ...string) []byte {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               pkix.Name{CommonName: "node", OrganizationalUnit: ous},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, caCert, &priv.PublicKey, caKey)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
}
