/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package orderers_test

import (
	"bytes"
	"os"
	"sort"
	"strings"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/orderers"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type comparableEndpoint struct {
	Address   string
	RootCerts [][]byte
}

// stripEndpoints makes a comparable version of the endpoints specified.  This
// is necessary because the endpoint contains a channel which is not
// comparable.
func stripEndpoints(endpoints []*orderers.Endpoint) []comparableEndpoint {
	endpointsWithChannelStripped := make([]comparableEndpoint, len(endpoints))
	for i, endpoint := range endpoints {
		certs := endpoint.RootCerts
		sort.Slice(certs, func(i, j int) bool {
			return bytes.Compare(certs[i], certs[j]) >= 0
		})
		endpointsWithChannelStripped[i].Address = endpoint.Address
		endpointsWithChannelStripped[i].RootCerts = certs
	}
	return endpointsWithChannelStripped
}

var _ = Describe("Connection", func() {
	var (
		cert1 []byte
		cert2 []byte
		cert3 []byte

		party1Certs [][]byte
		party2Certs [][]byte
		party3Certs [][]byte
		party4Certs [][]byte

		partyEndpoint1 *orderers.Endpoint
		partyEndpoint2 *orderers.Endpoint
		partyEndpoint3 *orderers.Endpoint
		partyEndpoint4 *orderers.Endpoint

		cs *orderers.ConnectionSource

		endpoints []*orderers.Endpoint
	)

	BeforeEach(func() {
		//TODO generate certs on the fly instead of reading from files, which will make the test more self-contained and easier to understand.

		var err error
		cert1, err = os.ReadFile("testdata/tlsca.example.com-cert.pem")
		Expect(err).NotTo(HaveOccurred())

		cert2, err = os.ReadFile("testdata/tlsca.org1.example.com-cert.pem")
		Expect(err).NotTo(HaveOccurred())

		cert3, err = os.ReadFile("testdata/tlsca.org2.example.com-cert.pem")
		Expect(err).NotTo(HaveOccurred())

		party1Certs = [][]byte{cert1}
		party2Certs = [][]byte{cert2}
		party3Certs = [][]byte{cert3}
		party4Certs = [][]byte{cert1, cert2}

		partyEndpoint1 = &orderers.Endpoint{
			Address:   "party1-endpoint",
			RootCerts: party1Certs,
		}
		partyEndpoint2 = &orderers.Endpoint{
			Address:   "party2-endpoint",
			RootCerts: party2Certs,
		}
		partyEndpoint3 = &orderers.Endpoint{
			Address:   "party3-endpoint",
			RootCerts: party3Certs,
		}
		partyEndpoint4 = &orderers.Endpoint{
			Address:   "party4-endpoint",
			RootCerts: party4Certs,
		}

		Expect(partyEndpoint1.String()).To(Equal("Address: party1-endpoint, RootCertHash: 410E5D5D97CD67AD489B2A8B8A181348CA3AA14AFE36B160D69C81274B5BA15E"))
		Expect(partyEndpoint2.String()).To(Equal("Address: party2-endpoint, RootCertHash: 1518B4AA6592CAC80EEFD2CAFBA534B77AC2B5460DB0E89606D0188AF7A1ACB4"))
		Expect(partyEndpoint3.String()).To(Equal("Address: party3-endpoint, RootCertHash: 16536C05487CBF530E923E5A986CDA4A99B48C625DEF0776E3FEB2F3429ED387"))
		Expect(partyEndpoint4.String()).To(Equal("Address: party4-endpoint, RootCertHash: 72163911AA82A098F5F9116949733746BCD4E21944D80E746C289D9CDE3B0FA0"))

		cs = orderers.NewConnectionSource(flogging.MustGetLogger("peer.orderers"), types.PartyID(0)) // << no self endpoint, as in the peer
		cs.Update2(orderers.Party2Endpoint{
			types.PartyID(1): partyEndpoint1,
			types.PartyID(2): partyEndpoint2,
			types.PartyID(3): partyEndpoint3,
			types.PartyID(4): partyEndpoint4,
		})

		endpoints = cs.Endpoints()
	})

	It("holds endpoints for all of the defined orderers", func() {
		Expect(stripEndpoints(endpoints)).To(ConsistOf(
			stripEndpoints([]*orderers.Endpoint{
				{
					Address:   "party1-endpoint",
					RootCerts: party1Certs,
				},
				{
					Address:   "party2-endpoint",
					RootCerts: party2Certs,
				},
				{
					Address:   "party3-endpoint",
					RootCerts: party3Certs,
				},
				{
					Address:   "party4-endpoint",
					RootCerts: party4Certs,
				},
			}),
		))
	})

	It("does not mark any of the endpoints as refreshed", func() {
		for _, endpoint := range endpoints {
			Expect(endpoint.Refreshed).NotTo(BeClosed())
		}
	})

	It("endpoint stringer works", func() {
		for _, endpoint := range endpoints {
			Expect(endpoint.String()).To(MatchRegexp("Address: party[1-4]-endpoint"))
			Expect(endpoint.String()).To(MatchRegexp("RootCertHash: [A-F0-9]+"))
		}
		e := &orderers.Endpoint{Address: "localhost"}
		Expect(e.String()).To(Equal("Address: localhost, RootCertHash: <nil>"))
		e = nil
		Expect(e.String()).To(Equal("<nil>"))
	})

	It("returns shuffled endpoints", func() { // there is a chance of failure here, but it is very small.
		combinationSet := make(map[string]bool)
		for i := 0; i < 10000; i++ {
			shuffledEndpoints := cs.ShuffledEndpoints()
			Expect(stripEndpoints(shuffledEndpoints)).To(ConsistOf(
				stripEndpoints(endpoints),
			))
			key := strings.Builder{}
			for _, ep := range shuffledEndpoints {
				key.WriteString(ep.Address)
				key.WriteString(" ")
			}
			combinationSet[key.String()] = true
		}

		Expect(len(combinationSet)).To(Equal(4 * 3 * 2 * 1))
	})

	It("returns random endpoint", func() { // there is a chance of failure here, but it is very small.
		combinationMap := make(map[string]*orderers.Endpoint)
		for i := 0; i < 10000; i++ {
			r, _ := cs.RandomEndpoint()
			combinationMap[r.Address] = r
		}
		var all []*orderers.Endpoint
		for _, ep := range combinationMap {
			all = append(all, ep)
		}
		Expect(stripEndpoints(all)).To(ConsistOf(
			stripEndpoints(endpoints),
		))
	})

	When("an update does not modify the endpoint set", func() {
		BeforeEach(func() {
			cs.Update2(orderers.Party2Endpoint{
				types.PartyID(1): &orderers.Endpoint{
					Address:   "party1-endpoint",
					RootCerts: party1Certs,
				},
				types.PartyID(2): &orderers.Endpoint{
					Address:   "party2-endpoint",
					RootCerts: party2Certs,
				},
				types.PartyID(3): &orderers.Endpoint{
					Address:   "party3-endpoint",
					RootCerts: party3Certs,
				},
				types.PartyID(4): &orderers.Endpoint{
					Address:   "party4-endpoint",
					RootCerts: party4Certs,
				},
			})
		})

		It("does not update the endpoints", func() {
			newEndpoints := cs.Endpoints()
			Expect(newEndpoints).To(Equal(endpoints))
		})

		It("does not close any of the refresh channels", func() {
			for _, endpoint := range endpoints {
				Expect(endpoint.Refreshed).NotTo(BeClosed())
			}
		})
	})

	When("an update change's an party's TLS CA", func() {
		BeforeEach(func() {
			party3Certs = [][]byte{cert3, cert1}

			cs.Update2(orderers.Party2Endpoint{
				types.PartyID(1): partyEndpoint1,
				types.PartyID(2): partyEndpoint2,
				types.PartyID(3): &orderers.Endpoint{
					Address:   "party3-endpoint",
					RootCerts: party3Certs,
				},
				types.PartyID(4): partyEndpoint4,
			})
		})

		It("creates a new set of orderer endpoints", func() {
			newParty3Certs := [][]byte{cert3, cert1}

			newEndpoints := cs.Endpoints()
			Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
				stripEndpoints([]*orderers.Endpoint{
					{
						Address:   "party1-endpoint",
						RootCerts: party1Certs,
					},
					{
						Address:   "party2-endpoint",
						RootCerts: party2Certs,
					},
					{
						Address:   "party3-endpoint",
						RootCerts: newParty3Certs,
					},
					{
						Address:   "party4-endpoint",
						RootCerts: party4Certs,
					},
				}),
			))
		})

		It("closes the refresh channel for all of the old endpoints", func() {
			for _, endpoint := range endpoints {
				Expect(endpoint.Refreshed).To(BeClosed())
			}
		})
	})

	When("an update change's an org's endpoint addresses", func() {
		var party1endpointAlt *orderers.Endpoint
		BeforeEach(func() {
			party1endpointAlt = &orderers.Endpoint{
				Address:   "party1-endpoint-alt",
				RootCerts: party1Certs,
			}

			cs.Update2(orderers.Party2Endpoint{
				types.PartyID(1): party1endpointAlt,
				types.PartyID(2): &orderers.Endpoint{
					Address:   "party2-endpoint",
					RootCerts: party2Certs,
				},
				types.PartyID(3): &orderers.Endpoint{
					Address:   "party3-endpoint",
					RootCerts: party3Certs,
				},
				types.PartyID(4): &orderers.Endpoint{
					Address:   "party4-endpoint",
					RootCerts: party4Certs,
				},
			})
		})

		It("creates a new set of orderer endpoints", func() {
			newEndpoints := cs.Endpoints()
			Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
				stripEndpoints([]*orderers.Endpoint{
					{
						Address:   "party1-endpoint-alt",
						RootCerts: party1Certs,
					},
					{
						Address:   "party2-endpoint",
						RootCerts: party2Certs,
					},
					{
						Address:   "party3-endpoint",
						RootCerts: party3Certs,
					},
					{
						Address:   "party4-endpoint",
						RootCerts: party4Certs,
					},
				}),
			))
		})

		It("closes the refresh channel for all of the old endpoints", func() {
			for _, endpoint := range endpoints {
				Expect(endpoint.Refreshed).To(BeClosed())
			}
		})
	})

	When("an update removes an ordering party", func() {
		BeforeEach(func() {
			cs.Update2(orderers.Party2Endpoint{
				types.PartyID(2): &orderers.Endpoint{
					Address:   "party2-endpoint",
					RootCerts: party2Certs,
				},
				types.PartyID(3): &orderers.Endpoint{
					Address:   "party3-endpoint",
					RootCerts: party3Certs,
				},
				types.PartyID(4): &orderers.Endpoint{
					Address:   "party4-endpoint",
					RootCerts: party4Certs,
				},
				// party 1 is removed
			})
		})

		It("creates a new set of orderer endpoints", func() {
			newEndpoints := cs.Endpoints()
			Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
				stripEndpoints([]*orderers.Endpoint{
					{
						Address:   "party2-endpoint",
						RootCerts: party2Certs,
					},
					{
						Address:   "party3-endpoint",
						RootCerts: party3Certs,
					},
					{
						Address:   "party4-endpoint",
						RootCerts: party4Certs,
					},
				}),
			))
		})

		It("closes the refresh channel for all of the old endpoints", func() {
			for _, endpoint := range endpoints {
				Expect(endpoint.Refreshed).To(BeClosed())
			}
		})

		When("a new party is added", func() {
			BeforeEach(func() {
				cs.Update2(orderers.Party2Endpoint{
					types.PartyID(2): &orderers.Endpoint{
						Address:   "party2-endpoint",
						RootCerts: party2Certs,
					},
					types.PartyID(3): &orderers.Endpoint{
						Address:   "party3-endpoint",
						RootCerts: party3Certs,
					},
					types.PartyID(4): &orderers.Endpoint{
						Address:   "party4-endpoint",
						RootCerts: party4Certs,
					},
					types.PartyID(5): &orderers.Endpoint{
						Address:   "party5-endpoint",
						RootCerts: party1Certs,
					},
				})
			})

			It("returns to the set of orderer endpoints", func() {
				newEndpoints := cs.Endpoints()
				Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
					stripEndpoints([]*orderers.Endpoint{
						{
							Address:   "party2-endpoint",
							RootCerts: party2Certs,
						},
						{
							Address:   "party3-endpoint",
							RootCerts: party3Certs,
						},
						{
							Address:   "party4-endpoint",
							RootCerts: party4Certs,
						},
						{
							Address:   "party5-endpoint",
							RootCerts: party1Certs,
						},
					}),
				))
			})
		})
	})

	When("a self-endpoint exists as in the orderer", func() {
		BeforeEach(func() {
			cs = orderers.NewConnectionSource(flogging.MustGetLogger("peer.orderers"), types.PartyID(1)) //<< self-party
			cs.Update2(orderers.Party2Endpoint{
				types.PartyID(1): partyEndpoint1, // self-endpoint
				types.PartyID(2): partyEndpoint2,
				types.PartyID(3): partyEndpoint3,
				types.PartyID(4): partyEndpoint4,
			})

			endpoints = cs.Endpoints()
		})

		It("does not include the self-endpoint in endpoints", func() {
			Expect(len(endpoints)).To(Equal(3))
			Expect(stripEndpoints(endpoints)).To(ConsistOf(
				stripEndpoints([]*orderers.Endpoint{
					{
						Address:   "party2-endpoint",
						RootCerts: party2Certs,
					},
					{
						Address:   "party3-endpoint",
						RootCerts: party3Certs,
					},
					{
						Address:   "party4-endpoint",
						RootCerts: party4Certs,
					},
				}),
			))
		})

		It("does not include the self endpoint in shuffled endpoints", func() {
			shuffledEndpoints := cs.ShuffledEndpoints()
			Expect(len(shuffledEndpoints)).To(Equal(3))
			Expect(stripEndpoints(endpoints)).To(ConsistOf(
				stripEndpoints([]*orderers.Endpoint{
					{
						Address:   "party2-endpoint",
						RootCerts: party2Certs,
					},
					{
						Address:   "party3-endpoint",
						RootCerts: party3Certs,
					},
					{
						Address:   "party4-endpoint",
						RootCerts: party4Certs,
					},
				}),
			))
		})

		It("does not include the self endpoint in random endpoint", func() { // there is a chance of failure here, but it is very small.
			combinationMap := make(map[string]*orderers.Endpoint)
			for i := 0; i < 10000; i++ {
				r, _ := cs.RandomEndpoint()
				combinationMap[r.Address] = r
			}
			var all []*orderers.Endpoint
			for _, ep := range combinationMap {
				all = append(all, ep)
			}
			Expect(stripEndpoints(all)).To(ConsistOf(
				stripEndpoints(endpoints),
			))
		})

		It("does not mark any of the endpoints as refreshed", func() {
			for _, endpoint := range endpoints {
				Expect(endpoint.Refreshed).NotTo(BeClosed())
			}
		})

		When("an update does not modify the endpoint set", func() {
			BeforeEach(func() {
				cs.Update2(orderers.Party2Endpoint{
					types.PartyID(1): partyEndpoint1, // self-endpoint
					types.PartyID(2): partyEndpoint2,
					types.PartyID(3): partyEndpoint3,
					types.PartyID(4): partyEndpoint4,
				})
			})

			It("does not update the endpoints", func() {
				newEndpoints := cs.Endpoints()
				Expect(newEndpoints).To(Equal(endpoints))
			})

			It("does not close any of the refresh channels", func() {
				for _, endpoint := range endpoints {
					Expect(endpoint.Refreshed).NotTo(BeClosed())
				}
			})
		})

		When("an update changes an org's TLS CA", func() {
			BeforeEach(func() {
				party3Certs = [][]byte{cert3, cert1}

				cs.Update2(orderers.Party2Endpoint{
					types.PartyID(1): partyEndpoint1,
					types.PartyID(2): partyEndpoint2,
					types.PartyID(3): &orderers.Endpoint{
						Address:   "party3-endpoint",
						RootCerts: party3Certs,
					},
					types.PartyID(4): partyEndpoint4,
				})
			})

			It("creates a new set of orderer endpoints yet skips the self-endpoint", func() {
				newEndpoints := cs.Endpoints()
				Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
					stripEndpoints([]*orderers.Endpoint{
						{
							Address:   "party2-endpoint",
							RootCerts: party2Certs,
						},
						{
							Address:   "party3-endpoint",
							RootCerts: party3Certs,
						},
						{
							Address:   "party4-endpoint",
							RootCerts: party4Certs,
						},
					}),
				))
			})

			It("closes the refresh channel for all of the old endpoints", func() {
				for _, endpoint := range endpoints {
					Expect(endpoint.Refreshed).To(BeClosed())
				}
			})
		})

		When("an update changes an org's endpoint addresses", func() {
			var party2endpointAlt *orderers.Endpoint
			BeforeEach(func() {
				party2endpointAlt = &orderers.Endpoint{
					Address:   "party2-endpoint-alt",
					RootCerts: party2Certs,
				}

				cs.Update2(orderers.Party2Endpoint{
					types.PartyID(1): partyEndpoint1, // self-endpoint
					types.PartyID(2): party2endpointAlt,
					types.PartyID(3): partyEndpoint3,
					types.PartyID(4): partyEndpoint4,
				})
			})

			It("creates a new set of orderer endpoints, without self-endpoint", func() {
				newEndpoints := cs.Endpoints()
				Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
					stripEndpoints([]*orderers.Endpoint{
						{
							Address:   "party2-endpoint-alt",
							RootCerts: party2Certs,
						},
						{
							Address:   "party3-endpoint",
							RootCerts: party3Certs,
						},
						{
							Address:   "party4-endpoint",
							RootCerts: party4Certs,
						},
					}),
				))
			})

			It("closes the refresh channel for all of the old endpoints", func() {
				for _, endpoint := range endpoints {
					Expect(endpoint.Refreshed).To(BeClosed())
				}
			})
		})

		When("an update removes an ordering organization", func() {
			BeforeEach(func() {
				cs.Update2(orderers.Party2Endpoint{
					types.PartyID(1): partyEndpoint1, // self-endpoint
					types.PartyID(3): partyEndpoint3,
					types.PartyID(4): partyEndpoint4,
				})
			})

			It("creates a new set of orderer endpoints, self-endpoint matches nothing", func() {
				newEndpoints := cs.Endpoints()
				Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
					stripEndpoints([]*orderers.Endpoint{
						{
							Address:   "party3-endpoint",
							RootCerts: party3Certs,
						},
						{
							Address:   "party4-endpoint",
							RootCerts: party4Certs,
						},
					}),
				))
			})

			It("closes the refresh channel for all of the old endpoints", func() {
				for _, endpoint := range endpoints {
					Expect(endpoint.Refreshed).To(BeClosed())
				}
			})

			When("a party is added", func() {
				BeforeEach(func() {
					cs.Update2(orderers.Party2Endpoint{
						types.PartyID(1): partyEndpoint1, // self-endpoint
						types.PartyID(3): partyEndpoint3,
						types.PartyID(4): partyEndpoint4,
						types.PartyID(5): &orderers.Endpoint{
							Address:   "party5-endpoint",
							RootCerts: party1Certs,
						},
					})
				})

				It("returns to the set of orderer endpoints, yet skips the self-endpoint", func() {
					newEndpoints := cs.Endpoints()
					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
						stripEndpoints([]*orderers.Endpoint{
							{
								Address:   "party3-endpoint",
								RootCerts: party3Certs,
							},
							{
								Address:   "party4-endpoint",
								RootCerts: party4Certs,
							},
							{
								Address:   "party5-endpoint",
								RootCerts: party1Certs,
							},
						}),
					))
				})
			})
		})

	})
})
