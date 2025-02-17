/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package encoder_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"arma/internal/configtxgen/encoder"
	"arma/internal/configtxgen/genesisconfig"
	"arma/internal/pkg/identity"

	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer/etcdraft"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer/smartbft"
	"github.com/hyperledger/fabric/protoutil"
	"google.golang.org/protobuf/proto"
)

//go:generate counterfeiter -o fakes/signer_serializer.go --fake-name SignerSerializer . signerSerializer

//lint:ignore U1000 fabric
type signerSerializer interface {
	identity.SignerSerializer
}

func CreateStandardPolicies() map[string]*genesisconfig.Policy {
	return map[string]*genesisconfig.Policy{
		"Admins": {
			Type: "ImplicitMeta",
			Rule: "ANY Admins",
		},
		"Readers": {
			Type: "ImplicitMeta",
			Rule: "ANY Readers",
		},
		"Writers": {
			Type: "ImplicitMeta",
			Rule: "ANY Writers",
		},
	}
}

func CreateStandardOrdererPolicies() map[string]*genesisconfig.Policy {
	policies := CreateStandardPolicies()

	policies["BlockValidation"] = &genesisconfig.Policy{
		Type: "ImplicitMeta",
		Rule: "ANY Admins",
	}

	return policies
}

var _ = Describe("Encoder", func() {
	Describe("AddOrdererPolicies", func() {
		var (
			cg       *cb.ConfigGroup
			policies map[string]*genesisconfig.Policy
		)

		BeforeEach(func() {
			cg = protoutil.NewConfigGroup()
			policies = CreateStandardOrdererPolicies()
		})

		It("adds the block validation policy to the group", func() {
			err := encoder.AddOrdererPolicies(cg, policies, "Admins")
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cg.Policies)).To(Equal(4))

			Expect(cg.Policies["BlockValidation"].Policy).To(Equal(&cb.Policy{
				Type: int32(cb.Policy_IMPLICIT_META),
				Value: protoutil.MarshalOrPanic(&cb.ImplicitMetaPolicy{
					SubPolicy: "Admins",
					Rule:      cb.ImplicitMetaPolicy_ANY,
				}),
			}))
		})

		Context("when the policy map is nil", func() {
			BeforeEach(func() {
				policies = nil
			})

			It("returns an error", func() {
				err := encoder.AddOrdererPolicies(cg, policies, "Admins")
				Expect(err).To(MatchError("no policies defined"))
			})
		})

		Context("when the policy map is missing 'BlockValidation'", func() {
			BeforeEach(func() {
				delete(policies, "BlockValidation")
			})

			It("returns an error", func() {
				err := encoder.AddOrdererPolicies(cg, policies, "Admins")
				Expect(err).To(MatchError("no BlockValidation policy defined"))
			})
		})
	})

	Describe("AddPolicies", func() {
		var (
			cg       *cb.ConfigGroup
			policies map[string]*genesisconfig.Policy
		)

		BeforeEach(func() {
			cg = protoutil.NewConfigGroup()
			policies = CreateStandardPolicies()
		})

		It("adds the standard policies to the group", func() {
			err := encoder.AddPolicies(cg, policies, "Admins")
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cg.Policies)).To(Equal(3))

			Expect(cg.Policies["Admins"].Policy).To(Equal(&cb.Policy{
				Type: int32(cb.Policy_IMPLICIT_META),
				Value: protoutil.MarshalOrPanic(&cb.ImplicitMetaPolicy{
					SubPolicy: "Admins",
					Rule:      cb.ImplicitMetaPolicy_ANY,
				}),
			}))

			Expect(cg.Policies["Readers"].Policy).To(Equal(&cb.Policy{
				Type: int32(cb.Policy_IMPLICIT_META),
				Value: protoutil.MarshalOrPanic(&cb.ImplicitMetaPolicy{
					SubPolicy: "Readers",
					Rule:      cb.ImplicitMetaPolicy_ANY,
				}),
			}))

			Expect(cg.Policies["Writers"].Policy).To(Equal(&cb.Policy{
				Type: int32(cb.Policy_IMPLICIT_META),
				Value: protoutil.MarshalOrPanic(&cb.ImplicitMetaPolicy{
					SubPolicy: "Writers",
					Rule:      cb.ImplicitMetaPolicy_ANY,
				}),
			}))
		})

		Context("when the policy map is nil", func() {
			BeforeEach(func() {
				policies = nil
			})

			It("returns an error", func() {
				err := encoder.AddPolicies(cg, policies, "Admins")
				Expect(err).To(MatchError("no policies defined"))
			})
		})

		Context("when the policy map is missing 'Admins'", func() {
			BeforeEach(func() {
				delete(policies, "Admins")
			})

			It("returns an error", func() {
				err := encoder.AddPolicies(cg, policies, "Admins")
				Expect(err).To(MatchError("no Admins policy defined"))
			})
		})

		Context("when the policy map is missing 'Readers'", func() {
			BeforeEach(func() {
				delete(policies, "Readers")
			})

			It("returns an error", func() {
				err := encoder.AddPolicies(cg, policies, "Readers")
				Expect(err).To(MatchError("no Readers policy defined"))
			})
		})

		Context("when the policy map is missing 'Writers'", func() {
			BeforeEach(func() {
				delete(policies, "Writers")
			})

			It("returns an error", func() {
				err := encoder.AddPolicies(cg, policies, "Writers")
				Expect(err).To(MatchError("no Writers policy defined"))
			})
		})

		Context("when the signature policy definition is bad", func() {
			BeforeEach(func() {
				policies["Readers"].Type = "Signature"
				policies["Readers"].Rule = "garbage"
			})

			It("wraps and returns the error", func() {
				err := encoder.AddPolicies(cg, policies, "Readers")
				Expect(err).To(MatchError("invalid signature policy rule 'garbage': unrecognized token 'garbage' in policy string"))
			})
		})

		Context("when the implicit policy definition is bad", func() {
			BeforeEach(func() {
				policies["Readers"].Type = "ImplicitMeta"
				policies["Readers"].Rule = "garbage"
			})

			It("wraps and returns the error", func() {
				err := encoder.AddPolicies(cg, policies, "Readers")
				Expect(err).To(MatchError("invalid implicit meta policy rule 'garbage': expected two space separated tokens, but got 1"))
			})
		})

		Context("when the policy type is unknown", func() {
			BeforeEach(func() {
				policies["Readers"].Type = "garbage"
			})

			It("returns an error", func() {
				err := encoder.AddPolicies(cg, policies, "Readers")
				Expect(err).To(MatchError("unknown policy type: garbage"))
			})
		})
	})

	Describe("NewChannelGroup", func() {
		var conf *genesisconfig.Profile

		BeforeEach(func() {
			conf = &genesisconfig.Profile{
				Consortium: "MyConsortium",
				Policies:   CreateStandardPolicies(),
				Application: &genesisconfig.Application{
					Policies: CreateStandardPolicies(),
				},
				Orderer: &genesisconfig.Orderer{
					OrdererType: "solo",
					Policies:    CreateStandardOrdererPolicies(),
				},
				Consortiums: map[string]*genesisconfig.Consortium{
					"SampleConsortium": {},
				},
				Capabilities: map[string]bool{
					"FakeCapability": true,
				},
			}
		})

		It("translates the config into a config group", func() {
			cg, err := encoder.NewChannelGroup(conf)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cg.Values)).To(Equal(4))
			Expect(cg.Values["BlockDataHashingStructure"]).NotTo(BeNil())
			Expect(cg.Values["Consortium"]).NotTo(BeNil())
			Expect(cg.Values["Capabilities"]).NotTo(BeNil())
			Expect(cg.Values["HashingAlgorithm"]).NotTo(BeNil())
			Expect(cg.Values["OrdererAddresses"]).To(BeNil())
		})

		Context("when the policy definition is bad", func() {
			BeforeEach(func() {
				conf.Policies["Admins"].Rule = "garbage"
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewChannelGroup(conf)
				Expect(err).To(MatchError("error adding policies to channel group: invalid implicit meta policy rule 'garbage': expected two space separated tokens, but got 1"))
			})
		})

		Context("when the orderer addresses are omitted", func() {
			BeforeEach(func() {
				conf.Orderer.Addresses = []string{}
			})

			It("does not create the config value", func() {
				cg, err := encoder.NewChannelGroup(conf)
				Expect(err).NotTo(HaveOccurred())
				Expect(cg.Values["OrdererAddresses"]).To(BeNil())
			})
		})

		Context("when the orderer config is bad", func() {
			BeforeEach(func() {
				conf.Orderer.OrdererType = "bad-type"
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewChannelGroup(conf)
				Expect(err).To(MatchError("could not create orderer group: unknown orderer type: bad-type"))
			})

			Context("when the orderer config is missing", func() {
				BeforeEach(func() {
					conf.Orderer = nil
				})

				It("handles it gracefully", func() {
					_, err := encoder.NewChannelGroup(conf)
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})

		Context("when the application config is bad", func() {
			BeforeEach(func() {
				conf.Application.Policies["Admins"] = &genesisconfig.Policy{
					Type: "garbage",
				}
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewChannelGroup(conf)
				Expect(err).To(MatchError("could not create application group: error adding policies to application group: unknown policy type: garbage"))
			})
		})

		Context("when the consortium config is bad", func() {
			BeforeEach(func() {
				conf.Consortiums["SampleConsortium"].Organizations = []*genesisconfig.Organization{
					{
						Policies: map[string]*genesisconfig.Policy{
							"garbage-policy": {
								Type: "garbage",
							},
						},
					},
				}
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewChannelGroup(conf)
				Expect(err).To(MatchError("could not create consortiums group: failed to create consortium SampleConsortium: failed to create consortium org: 1 - Error loading MSP configuration for org: : unknown MSP type ''"))
			})
		})
	})

	Describe("NewOrdererGroup", func() {
		var conf *genesisconfig.Orderer
		var channelCapabilities map[string]bool

		BeforeEach(func() {
			conf = &genesisconfig.Orderer{
				OrdererType: "solo",
				Organizations: []*genesisconfig.Organization{
					{
						MSPDir:           "../../../testutil/fabric/sampleconfig/msp",
						ID:               "SampleMSP",
						MSPType:          "bccsp",
						Name:             "SampleOrg",
						Policies:         CreateStandardPolicies(),
						OrdererEndpoints: []string{"foo:7050", "bar:8050"},
					},
				},
				Policies: CreateStandardOrdererPolicies(),
				Capabilities: map[string]bool{
					"FakeCapability": true,
				},
			}
			channelCapabilities = map[string]bool{
				"V3_0": true,
			}
		})

		It("translates the config into a config group", func() {
			cg, err := encoder.NewOrdererGroup(conf, channelCapabilities)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cg.Policies)).To(Equal(4)) // BlockValidation automatically added
			Expect(cg.Policies["Admins"]).NotTo(BeNil())
			Expect(cg.Policies["Readers"]).NotTo(BeNil())
			Expect(cg.Policies["Writers"]).NotTo(BeNil())
			Expect(cg.Policies["BlockValidation"]).NotTo(BeNil())
			Expect(len(cg.Groups)).To(Equal(1))
			Expect(cg.Groups["SampleOrg"]).NotTo(BeNil())
			Expect(len(cg.Values)).To(Equal(5))
			Expect(cg.Values["BatchSize"]).NotTo(BeNil())
			Expect(cg.Values["BatchTimeout"]).NotTo(BeNil())
			Expect(cg.Values["ChannelRestrictions"]).NotTo(BeNil())
			Expect(cg.Values["Capabilities"]).NotTo(BeNil())
		})

		Context("when the policy definition is bad", func() {
			BeforeEach(func() {
				conf.Policies["Admins"].Rule = "garbage"
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewOrdererGroup(conf, channelCapabilities)
				Expect(err).To(MatchError("error adding policies to orderer group: invalid implicit meta policy rule 'garbage': expected two space separated tokens, but got 1"))
			})
		})

		Context("when the consensus type is etcd/raft", func() {
			BeforeEach(func() {
				conf.OrdererType = "etcdraft"
				conf.EtcdRaft = &etcdraft.ConfigMetadata{
					Options: &etcdraft.Options{
						TickInterval: "500ms",
					},
				}
			})

			It("adds the raft metadata", func() {
				cg, err := encoder.NewOrdererGroup(conf, channelCapabilities)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(cg.Values)).To(Equal(5))
				consensusType := &ab.ConsensusType{}
				err = proto.Unmarshal(cg.Values["ConsensusType"].Value, consensusType)
				Expect(err).NotTo(HaveOccurred())
				Expect(consensusType.Type).To(Equal("etcdraft"))
				metadata := &etcdraft.ConfigMetadata{}
				err = proto.Unmarshal(consensusType.Metadata, metadata)
				Expect(err).NotTo(HaveOccurred())
				Expect(metadata.Options.TickInterval).To(Equal("500ms"))
			})

			Context("when the raft configuration is bad", func() {
				BeforeEach(func() {
					conf.EtcdRaft = &etcdraft.ConfigMetadata{
						Consenters: []*etcdraft.Consenter{
							{},
						},
					}
				})

				It("wraps and returns the error", func() {
					_, err := encoder.NewOrdererGroup(conf, channelCapabilities)
					Expect(err).To(MatchError("cannot marshal metadata for orderer type etcdraft: cannot load client cert for consenter :0: open : no such file or directory"))
				})
			})
		})

		Context("when the consensus type is BFT", func() {
			BeforeEach(func() {
				conf.OrdererType = "BFT"
				conf.ConsenterMapping = []*genesisconfig.Consenter{
					{
						ID:    1,
						Host:  "host1",
						Port:  1001,
						MSPID: "MSP1",
					},
					{
						ID:            2,
						Host:          "host2",
						Port:          1002,
						MSPID:         "MSP2",
						ClientTLSCert: "../../../testutil/fabric/sampleconfig/msp/admincerts/admincert.pem",
						ServerTLSCert: "../../../testutil/fabric/sampleconfig/msp/admincerts/admincert.pem",
						Identity:      "../../../testutil/fabric/sampleconfig/msp/admincerts/admincert.pem",
					},
				}
				conf.SmartBFT = &smartbft.Options{}
			})

			It("adds the Orderers key", func() {
				cg, err := encoder.NewOrdererGroup(conf, channelCapabilities)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(cg.Values)).To(Equal(6))
				Expect(cg.Values["Orderers"]).NotTo(BeNil())
				orderersType := &cb.Orderers{}
				err = proto.Unmarshal(cg.Values["Orderers"].Value, orderersType)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(orderersType.ConsenterMapping)).To(Equal(2))
				consenter1 := orderersType.ConsenterMapping[0]
				Expect(consenter1.Id).To(Equal(uint32(1)))
				Expect(consenter1.ClientTlsCert).To(BeNil())
				consenter2 := orderersType.ConsenterMapping[1]
				Expect(consenter2.Id).To(Equal(uint32(2)))
				Expect(consenter2.ClientTlsCert).ToNot(BeNil())
			})

			It("requires V3_0", func() {
				delete(channelCapabilities, "V3_0")
				channelCapabilities["V2_0"] = true
				_, err := encoder.NewOrdererGroup(conf, channelCapabilities)
				Expect(err).To(MatchError("orderer type BFT must be used with V3_0 channel capability: map[V2_0:true]"))
			})
		})

		Context("when the consensus type is unknown", func() {
			BeforeEach(func() {
				conf.OrdererType = "bad-type"
			})

			It("returns an error", func() {
				_, err := encoder.NewOrdererGroup(conf, channelCapabilities)
				Expect(err).To(MatchError("unknown orderer type: bad-type"))
			})
		})

		Context("when the org definition is bad", func() {
			BeforeEach(func() {
				conf.Organizations[0].MSPType = "garbage"
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewOrdererGroup(conf, channelCapabilities)
				Expect(err).To(MatchError("failed to create orderer org: 1 - Error loading MSP configuration for org: SampleOrg: unknown MSP type 'garbage'"))
			})
		})

		Context("when global endpoints exist", func() {
			BeforeEach(func() {
				conf.Addresses = []string{"addr1", "addr2"}
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewOrdererGroup(conf, channelCapabilities)
				Expect(err).To(MatchError("global orderer endpoints exist, but can not be used with V3_0 capability: [addr1 addr2]"))
			})

			It("is permitted when V3_0 is false", func() {
				delete(channelCapabilities, "V3_0")
				channelCapabilities["V2_0"] = true
				_, err := encoder.NewOrdererGroup(conf, channelCapabilities)
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})

	Describe("NewApplicationGroup", func() {
		var conf *genesisconfig.Application

		BeforeEach(func() {
			conf = &genesisconfig.Application{
				Organizations: []*genesisconfig.Organization{
					{
						MSPDir:   "../../../testutil/fabric/sampleconfig/msp",
						ID:       "SampleMSP",
						MSPType:  "bccsp",
						Name:     "SampleOrg",
						Policies: CreateStandardPolicies(),
					},
				},
				ACLs: map[string]string{
					"SomeACL": "SomePolicy",
				},
				Policies: CreateStandardPolicies(),
				Capabilities: map[string]bool{
					"FakeCapability": true,
				},
			}
		})

		It("translates the config into a config group", func() {
			cg, err := encoder.NewApplicationGroup(conf)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cg.Policies)).To(Equal(3))
			Expect(cg.Policies["Admins"]).NotTo(BeNil())
			Expect(cg.Policies["Readers"]).NotTo(BeNil())
			Expect(cg.Policies["Writers"]).NotTo(BeNil())
			Expect(len(cg.Groups)).To(Equal(1))
			Expect(cg.Groups["SampleOrg"]).NotTo(BeNil())
			Expect(len(cg.Values)).To(Equal(2))
			Expect(cg.Values["ACLs"]).NotTo(BeNil())
			Expect(cg.Values["Capabilities"]).NotTo(BeNil())
		})

		Context("when the policy definition is bad", func() {
			BeforeEach(func() {
				conf.Policies["Admins"].Rule = "garbage"
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewApplicationGroup(conf)
				Expect(err).To(MatchError("error adding policies to application group: invalid implicit meta policy rule 'garbage': expected two space separated tokens, but got 1"))
			})
		})

		Context("when the org definition is bad", func() {
			BeforeEach(func() {
				conf.Organizations[0].MSPType = "garbage"
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewApplicationGroup(conf)
				Expect(err).To(MatchError("failed to create application org: 1 - Error loading MSP configuration for org SampleOrg: unknown MSP type 'garbage'"))
			})
		})
	})

	Describe("NewConsortiumOrgGroup", func() {
		var conf *genesisconfig.Organization

		BeforeEach(func() {
			conf = &genesisconfig.Organization{
				MSPDir:   "../../../testutil/fabric/sampleconfig/msp",
				ID:       "SampleMSP",
				MSPType:  "bccsp",
				Name:     "SampleOrg",
				Policies: CreateStandardPolicies(),
			}
		})

		It("translates the config into a config group", func() {
			cg, err := encoder.NewConsortiumOrgGroup(conf)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cg.Values)).To(Equal(1))
			Expect(cg.Values["MSP"]).NotTo(BeNil())
			Expect(len(cg.Policies)).To(Equal(3))
			Expect(cg.Policies["Admins"]).NotTo(BeNil())
			Expect(cg.Policies["Readers"]).NotTo(BeNil())
			Expect(cg.Policies["Writers"]).NotTo(BeNil())
		})

		Context("when the org is marked to be skipped as foreign", func() {
			BeforeEach(func() {
				conf.SkipAsForeign = true
			})

			It("returns an empty org group with mod policy set", func() {
				cg, err := encoder.NewConsortiumOrgGroup(conf)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(cg.Values)).To(Equal(0))
				Expect(len(cg.Policies)).To(Equal(0))
			})

			Context("even when the MSP dir is invalid/corrupt", func() {
				BeforeEach(func() {
					conf.MSPDir = "garbage"
				})

				It("returns without error", func() {
					_, err := encoder.NewConsortiumOrgGroup(conf)
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})

		Context("when dev mode is enabled", func() {
			BeforeEach(func() {
				conf.AdminPrincipal = "Member"
			})

			It("does not produce an error", func() {
				_, err := encoder.NewConsortiumOrgGroup(conf)
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when the policy definition is bad", func() {
			BeforeEach(func() {
				conf.Policies["Admins"].Rule = "garbage"
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewConsortiumOrgGroup(conf)
				Expect(err).To(MatchError("error adding policies to consortiums org group 'SampleOrg': invalid implicit meta policy rule 'garbage': expected two space separated tokens, but got 1"))
			})
		})
	})

	Describe("NewOrdererOrgGroup", func() {
		var conf *genesisconfig.Organization

		BeforeEach(func() {
			conf = &genesisconfig.Organization{
				MSPDir:   "../../../testutil/fabric/sampleconfig/msp",
				ID:       "SampleMSP",
				MSPType:  "bccsp",
				Name:     "SampleOrg",
				Policies: CreateStandardPolicies(),
				OrdererEndpoints: []string{
					"foo:7050",
					"bar:8050",
				},
			}
		})

		It("translates the config into a config group", func() {
			cg, err := encoder.NewOrdererOrgGroup(conf, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cg.Values)).To(Equal(2))
			Expect(cg.Values["MSP"]).NotTo(BeNil())
			Expect(len(cg.Policies)).To(Equal(3))
			Expect(cg.Values["Endpoints"]).NotTo(BeNil())
			Expect(cg.Policies["Admins"]).NotTo(BeNil())
			Expect(cg.Policies["Readers"]).NotTo(BeNil())
			Expect(cg.Policies["Writers"]).NotTo(BeNil())
		})

		Context("when the org is marked to be skipped as foreign", func() {
			BeforeEach(func() {
				conf.SkipAsForeign = true
			})

			It("returns an empty org group with mod policy set", func() {
				cg, err := encoder.NewOrdererOrgGroup(conf, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(cg.Values)).To(Equal(0))
				Expect(len(cg.Policies)).To(Equal(0))
			})

			Context("even when the MSP dir is invalid/corrupt", func() {
				BeforeEach(func() {
					conf.MSPDir = "garbage"
				})

				It("returns without error", func() {
					_, err := encoder.NewOrdererOrgGroup(conf, nil)
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})

		Context("when there are no ordering endpoints", func() {
			BeforeEach(func() {
				conf.OrdererEndpoints = []string{}
			})

			It("does not include the endpoints in the config group with v2_0", func() {
				channelCapabilities := map[string]bool{"V2_0": true}
				cg, err := encoder.NewOrdererOrgGroup(conf, channelCapabilities)
				Expect(err).NotTo(HaveOccurred())
				Expect(cg.Values["Endpoints"]).To(BeNil())
			})

			It("emits an error with v3_0", func() {
				channelCapabilities := map[string]bool{"V3_0": true}
				cg, err := encoder.NewOrdererOrgGroup(conf, channelCapabilities)
				Expect(err).To(HaveOccurred())
				Expect(cg).To(BeNil())
			})
		})

		Context("when dev mode is enabled", func() {
			BeforeEach(func() {
				conf.AdminPrincipal = "Member"
			})

			It("does not produce an error", func() {
				_, err := encoder.NewOrdererOrgGroup(conf, nil)
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("when the policy definition is bad", func() {
			BeforeEach(func() {
				conf.Policies["Admins"].Rule = "garbage"
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewOrdererOrgGroup(conf, nil)
				Expect(err).To(MatchError("error adding policies to orderer org group 'SampleOrg': invalid implicit meta policy rule 'garbage': expected two space separated tokens, but got 1"))
			})
		})
	})

	Describe("NewApplicationOrgGroup", func() {
		var conf *genesisconfig.Organization

		BeforeEach(func() {
			conf = &genesisconfig.Organization{
				MSPDir:   "../../../testutil/fabric/sampleconfig/msp",
				ID:       "SampleMSP",
				MSPType:  "bccsp",
				Name:     "SampleOrg",
				Policies: CreateStandardPolicies(),
				AnchorPeers: []*genesisconfig.AnchorPeer{
					{
						Host: "hostname",
						Port: 5555,
					},
				},
			}
		})

		It("translates the config into a config group", func() {
			cg, err := encoder.NewApplicationOrgGroup(conf)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(cg.Values)).To(Equal(2))
			Expect(cg.Values["MSP"]).NotTo(BeNil())
			Expect(cg.Values["AnchorPeers"]).NotTo(BeNil())
			Expect(len(cg.Policies)).To(Equal(3))
			Expect(cg.Policies["Admins"]).NotTo(BeNil())
			Expect(cg.Policies["Readers"]).NotTo(BeNil())
			Expect(cg.Policies["Writers"]).NotTo(BeNil())
			Expect(len(cg.Values)).To(Equal(2))
			Expect(cg.Values["MSP"]).NotTo(BeNil())
			Expect(cg.Values["AnchorPeers"]).NotTo(BeNil())
		})

		Context("when the org is marked to be skipped as foreign", func() {
			BeforeEach(func() {
				conf.SkipAsForeign = true
			})

			It("returns an empty org group with mod policy set", func() {
				cg, err := encoder.NewApplicationOrgGroup(conf)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(cg.Values)).To(Equal(0))
				Expect(len(cg.Policies)).To(Equal(0))
			})

			Context("even when the MSP dir is invalid/corrupt", func() {
				BeforeEach(func() {
					conf.MSPDir = "garbage"
				})

				It("returns without error", func() {
					_, err := encoder.NewApplicationOrgGroup(conf)
					Expect(err).NotTo(HaveOccurred())
				})
			})
		})

		Context("when the policy definition is bad", func() {
			BeforeEach(func() {
				conf.Policies["Admins"].Type = "garbage"
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewApplicationOrgGroup(conf)
				Expect(err).To(MatchError("error adding policies to application org group SampleOrg: unknown policy type: garbage"))
			})
		})

		Context("when the MSP definition is bad", func() {
			BeforeEach(func() {
				conf.MSPDir = "garbage"
			})

			It("wraps and returns the error", func() {
				_, err := encoder.NewApplicationOrgGroup(conf)
				Expect(err).To(MatchError("1 - Error loading MSP configuration for org SampleOrg: could not load a valid ca certificate from directory garbage/cacerts: stat garbage/cacerts: no such file or directory"))
			})
		})

		Context("when there are no anchor peers defined", func() {
			BeforeEach(func() {
				conf.AnchorPeers = nil
			})

			It("does not encode the anchor peers", func() {
				cg, err := encoder.NewApplicationOrgGroup(conf)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(cg.Values)).To(Equal(1))
				Expect(cg.Values["AnchorPeers"]).To(BeNil())
			})
		})
	})

	Describe("Bootstrapper", func() {
		Describe("NewBootstrapper", func() {
			var conf *genesisconfig.Profile

			BeforeEach(func() {
				conf = &genesisconfig.Profile{
					Policies: CreateStandardPolicies(),
					Orderer: &genesisconfig.Orderer{
						OrdererType: "solo",
						Policies:    CreateStandardOrdererPolicies(),
					},
				}
			})

			It("creates a new bootstrapper for the given config", func() {
				bs, err := encoder.NewBootstrapper(conf)
				Expect(err).NotTo(HaveOccurred())
				Expect(bs).NotTo(BeNil())
			})

			Context("when the channel config is bad", func() {
				BeforeEach(func() {
					conf.Orderer.OrdererType = "bad-type"
				})

				It("wraps and returns the error", func() {
					_, err := encoder.NewBootstrapper(conf)
					Expect(err).To(MatchError("could not create channel group: could not create orderer group: unknown orderer type: bad-type"))
				})
			})

			Context("when the channel config contains a foreign org", func() {
				BeforeEach(func() {
					conf.Orderer.Organizations = []*genesisconfig.Organization{
						{
							Name:          "MyOrg",
							SkipAsForeign: true,
						},
					}
				})

				It("wraps and returns the error", func() {
					_, err := encoder.NewBootstrapper(conf)
					Expect(err).To(MatchError("all org definitions must be local during bootstrapping: organization 'MyOrg' is marked to be skipped as foreign"))
				})
			})
		})

		Describe("New", func() {
			var conf *genesisconfig.Profile

			BeforeEach(func() {
				conf = &genesisconfig.Profile{
					Policies: CreateStandardPolicies(),
					Orderer: &genesisconfig.Orderer{
						OrdererType: "solo",
						Policies:    CreateStandardOrdererPolicies(),
					},
				}
			})

			It("creates a new bootstrapper for the given config", func() {
				bs := encoder.New(conf)
				Expect(bs).NotTo(BeNil())
			})

			Context("when the channel config is bad", func() {
				BeforeEach(func() {
					conf.Orderer.OrdererType = "bad-type"
				})

				It("panics", func() {
					Expect(func() { encoder.New(conf) }).To(Panic())
				})
			})
		})

		Describe("Functions", func() {
			var bs *encoder.Bootstrapper

			BeforeEach(func() {
				bs = encoder.New(&genesisconfig.Profile{
					Policies: CreateStandardPolicies(),
					Orderer: &genesisconfig.Orderer{
						Policies:    CreateStandardOrdererPolicies(),
						OrdererType: "solo",
					},
				})
			})

			Describe("GenesisBlock", func() {
				It("produces a new genesis block with a default channel ID", func() {
					block := bs.GenesisBlock()
					Expect(block).NotTo(BeNil())
				})
			})

			Describe("GenesisBlockForChannel", func() {
				It("produces a new genesis block with a default channel ID", func() {
					block := bs.GenesisBlockForChannel("channel-id")
					Expect(block).NotTo(BeNil())
				})
			})
		})
	})
})
