/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package orderers_test

import (
	"bytes"
	"crypto/rand"
	"sort"
	"strings"
	"testing"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/delivery/client/orderers"
	"github.com/stretchr/testify/assert"
)

type testSetup struct {
	cert1 []byte
	cert2 []byte
	cert3 []byte
	cert4 []byte
	cert5 []byte
	cert6 []byte

	party2source map[types.PartyID]*orderers.SourceEndpoint
}

func newTestSetup(t *testing.T) *testSetup {
	s := &testSetup{
		cert1:        make([]byte, 847),
		cert2:        make([]byte, 847),
		cert3:        make([]byte, 847),
		cert4:        make([]byte, 847),
		cert5:        make([]byte, 847),
		cert6:        make([]byte, 847),
		party2source: make(map[types.PartyID]*orderers.SourceEndpoint),
	}

	rand.Read(s.cert1)
	rand.Read(s.cert2)
	rand.Read(s.cert3)
	rand.Read(s.cert4)
	rand.Read(s.cert5)
	rand.Read(s.cert6)

	s.party2source[1] = &orderers.SourceEndpoint{
		Address:   "party1-address",
		RootCerts: [][]byte{s.cert1, s.cert5},
	}

	s.party2source[2] = &orderers.SourceEndpoint{
		Address:   "party2-address",
		RootCerts: [][]byte{s.cert2, s.cert6},
	}

	s.party2source[3] = &orderers.SourceEndpoint{
		Address:   "party3-address",
		RootCerts: [][]byte{s.cert3},
	}

	s.party2source[4] = &orderers.SourceEndpoint{
		Address:   "party4-address",
		RootCerts: [][]byte{s.cert4},
	}

	return s
}

func TestConnectionSource_NotPartOf(t *testing.T) {
	lg := flogging.MustGetLogger("arma.test")
	setup := newTestSetup(t)

	cs := orderers.NewConnectionSource(lg, 1, 2, false)
	assert.NotNil(t, cs)

	cs.Update(setup.party2source)
	endpoints := cs.Endpoints()
	expectedEndpoints := []*orderers.Endpoint{
		{
			Address:   "party1-address",
			RootCerts: [][]byte{setup.cert1, setup.cert5},
		},
		{
			Address:   "party2-address",
			RootCerts: [][]byte{setup.cert2, setup.cert6},
		},
		{
			Address:   "party3-address",
			RootCerts: [][]byte{setup.cert3},
		},
		{
			Address:   "party4-address",
			RootCerts: [][]byte{setup.cert4},
		},
	}

	t.Run("all endpoints are as defined", func(t *testing.T) {
		consistsOf(t, expectedEndpoints, endpoints)
	})

	t.Run("endpoints are not marked as refreshed", func(t *testing.T) {
		for _, ep := range endpoints {
			select {
			case <-ep.Refreshed:
				t.FailNow()
			default:
			}
		}
	})

	t.Run("endpoint stringer works", func(t *testing.T) {
		for _, endpoint := range endpoints {
			assert.Regexp(t, "Address: party[1234]-address", endpoint.String())
			assert.Regexp(t, "CertHash: [A-F0-9]+", endpoint.String())
		}
		e := &orderers.Endpoint{Address: "localhost"}
		assert.Equal(t, "Address: localhost, CertHash: <nil>", e.String())
		e = nil
		assert.Equal(t, "<nil>", e.String())
	})

	t.Run("returns shuffled endpoints", func(t *testing.T) { // there is a chance of failure here, but it is very small.
		combinationSet := make(map[string]bool)
		for i := 0; i < 10000; i++ {
			shuffledEndpoints := cs.ShuffledEndpoints()

			consistsOf(t, endpoints, shuffledEndpoints)
			key := strings.Builder{}
			for _, ep := range shuffledEndpoints {
				key.WriteString(ep.Address)
				key.WriteString(" ")
			}
			combinationSet[key.String()] = true
		}

		assert.Len(t, combinationSet, (4 * 3 * 2 * 1))
	})

	t.Run("returns random endpoint", func(t *testing.T) { // there is a chance of failure here, but it is very small.
		combinationMap := make(map[string]*orderers.Endpoint)
		for i := 0; i < 10000; i++ {
			r, _ := cs.RandomEndpoint()
			combinationMap[r.Address] = r
		}
		var all []*orderers.Endpoint
		for _, ep := range combinationMap {
			all = append(all, ep)
		}
		consistsOf(t, endpoints, all)
	})

	t.Run("returns endpoint by party", func(t *testing.T) {
		var all []*orderers.Endpoint
		for i := 1; i <= 4; i++ {
			ep, err := cs.PartyEndpoint(types.PartyID(i))
			assert.NoError(t, err)
			all = append(all, ep)
		}
		consistsOf(t, endpoints, all)

		ep, err := cs.PartyEndpoint(5)
		assert.Nil(t, ep)
		assert.EqualError(t, err, "not found")
	})

	t.Run("an update that does not change the endpoints", func(t *testing.T) {
		cs.Update(setup.party2source)

		t.Log("endpoints are not refreshed")
		endpointsNotRefreshed(t, endpoints)

		t.Log("endpoints do not change")
		newEndpoints := cs.Endpoints()
		consistsOf(t, expectedEndpoints, newEndpoints)
	})
}

func TestConnectionSource_Update(t *testing.T) {
	lg := flogging.MustGetLogger("arma.test")

	t.Run("an update to a party CA cert", func(t *testing.T) {
		setup := newTestSetup(t)
		cs := orderers.NewConnectionSource(lg, 1, 2, false)
		assert.NotNil(t, cs)

		cs.Update(setup.party2source)
		endpoints := cs.Endpoints()
		t.Log("drop cert5 from party 1")
		setup.party2source[1].RootCerts = [][]byte{setup.cert1} // drop cert5
		cs.Update(setup.party2source)

		t.Log("endpoints are refreshed")
		endpointsRefreshed(t, endpoints)

		expectedEndpoints := []*orderers.Endpoint{
			{
				Address:   "party1-address",
				RootCerts: [][]byte{setup.cert1},
			},
			{
				Address:   "party2-address",
				RootCerts: [][]byte{setup.cert2, setup.cert6},
			},
			{
				Address:   "party3-address",
				RootCerts: [][]byte{setup.cert3},
			},
			{
				Address:   "party4-address",
				RootCerts: [][]byte{setup.cert4},
			},
		}

		t.Log("new endpoints are as expected")
		newEndpoints := cs.Endpoints()
		consistsOf(t, expectedEndpoints, newEndpoints)
		endpointsNotRefreshed(t, newEndpoints)
	})

	t.Run("an update to a party address", func(t *testing.T) {
		setup := newTestSetup(t)
		cs := orderers.NewConnectionSource(lg, 1, 2, false)
		assert.NotNil(t, cs)

		cs.Update(setup.party2source)
		endpoints := cs.Endpoints()
		t.Log("change party 1 address")
		setup.party2source[1].Address = "party1-address-new"
		cs.Update(setup.party2source)

		t.Log("endpoints are refreshed")
		endpointsRefreshed(t, endpoints)

		expectedEndpoints := []*orderers.Endpoint{
			{
				Address:   "party1-address-new",
				RootCerts: [][]byte{setup.cert1, setup.cert5},
			},
			{
				Address:   "party2-address",
				RootCerts: [][]byte{setup.cert2, setup.cert6},
			},
			{
				Address:   "party3-address",
				RootCerts: [][]byte{setup.cert3},
			},
			{
				Address:   "party4-address",
				RootCerts: [][]byte{setup.cert4},
			},
		}

		t.Log("new endpoints are as expected")
		newEndpoints := cs.Endpoints()
		consistsOf(t, expectedEndpoints, newEndpoints)
		endpointsNotRefreshed(t, newEndpoints)
	})

	t.Run("remove a party", func(t *testing.T) {
		setup := newTestSetup(t)
		cs := orderers.NewConnectionSource(lg, 1, 2, false)
		assert.NotNil(t, cs)

		cs.Update(setup.party2source)
		endpoints := cs.Endpoints()
		t.Log("delete party 1")
		delete(setup.party2source, 1)
		cs.Update(setup.party2source)

		t.Log("endpoints are refreshed")
		endpointsRefreshed(t, endpoints)

		expectedEndpoints := []*orderers.Endpoint{
			{
				Address:   "party2-address",
				RootCerts: [][]byte{setup.cert2, setup.cert6},
			},
			{
				Address:   "party3-address",
				RootCerts: [][]byte{setup.cert3},
			},
			{
				Address:   "party4-address",
				RootCerts: [][]byte{setup.cert4},
			},
		}

		t.Log("new endpoints are as expected")
		newEndpoints := cs.Endpoints()
		consistsOf(t, expectedEndpoints, newEndpoints)
		endpointsNotRefreshed(t, newEndpoints)
		ep, err := cs.PartyEndpoint(1)
		assert.EqualError(t, err, "not found")
		assert.Nil(t, ep)
	})

	t.Run("add a party", func(t *testing.T) {
		setup := newTestSetup(t)
		cs := orderers.NewConnectionSource(lg, 1, 2, false)
		assert.NotNil(t, cs)

		cs.Update(setup.party2source)
		endpoints := cs.Endpoints()
		t.Log("add party 5")
		setup.party2source[5] = &orderers.SourceEndpoint{
			Address:   "party5-address",
			RootCerts: [][]byte{setup.cert5},
		}
		cs.Update(setup.party2source)

		t.Log("endpoints are refreshed")
		endpointsRefreshed(t, endpoints)

		expectedEndpoints := []*orderers.Endpoint{
			{
				Address:   "party1-address",
				RootCerts: [][]byte{setup.cert1, setup.cert5},
			},
			{
				Address:   "party2-address",
				RootCerts: [][]byte{setup.cert2, setup.cert6},
			},
			{
				Address:   "party3-address",
				RootCerts: [][]byte{setup.cert3},
			},
			{
				Address:   "party4-address",
				RootCerts: [][]byte{setup.cert4},
			},
			{
				Address:   "party5-address",
				RootCerts: [][]byte{setup.cert5},
			},
		}

		t.Log("new endpoints are as expected")
		newEndpoints := cs.Endpoints()
		consistsOf(t, expectedEndpoints, newEndpoints)
		endpointsNotRefreshed(t, newEndpoints)

		ep, err := cs.PartyEndpoint(5)
		assert.NoError(t, err)
		assert.Equal(t, "party5-address", ep.Address)
	})
}

func TestConnectionSource_PartOf(t *testing.T) {
	lg := flogging.MustGetLogger("arma.test")
	setup := newTestSetup(t)

	cs := orderers.NewConnectionSource(lg, 1, 2, true)
	assert.NotNil(t, cs)

	cs.Update(setup.party2source)
	endpoints := cs.Endpoints()
	expectedEndpoints := []*orderers.Endpoint{
		{
			Address:   "party1-address",
			RootCerts: [][]byte{setup.cert1, setup.cert5},
		},
		{
			Address:   "party2-address",
			RootCerts: [][]byte{setup.cert2, setup.cert6},
		},
		{
			Address:   "party3-address",
			RootCerts: [][]byte{setup.cert3},
		},
		{
			Address:   "party4-address",
			RootCerts: [][]byte{setup.cert4},
		},
	}

	t.Run("all endpoints do not include self party", func(t *testing.T) {
		consistsOf(t, expectedEndpoints[1:], endpoints)
	})

	t.Run("endpoints are not marked as refreshed", func(t *testing.T) {
		for _, ep := range endpoints {
			select {
			case <-ep.Refreshed:
				t.FailNow()
			default:
			}
		}
	})

	t.Run("shuffled endpoints do not include self party", func(t *testing.T) { // there is a chance of failure here, but it is very small.
		combinationSet := make(map[string]bool)
		for i := 0; i < 10000; i++ {
			shuffledEndpoints := cs.ShuffledEndpoints()

			consistsOf(t, endpoints, shuffledEndpoints)
			key := strings.Builder{}
			for _, ep := range shuffledEndpoints {
				key.WriteString(ep.Address)
				key.WriteString(" ")
			}
			combinationSet[key.String()] = true
		}

		assert.Len(t, combinationSet, (3 * 2 * 1))
	})

	t.Run("random endpoint does not include self party", func(t *testing.T) { // there is a chance of failure here, but it is very small.
		combinationMap := make(map[string]*orderers.Endpoint)
		for i := 0; i < 10000; i++ {
			r, _ := cs.RandomEndpoint()
			combinationMap[r.Address] = r
		}
		var all []*orderers.Endpoint
		for _, ep := range combinationMap {
			all = append(all, ep)
		}
		consistsOf(t, endpoints, all)
	})

	t.Run("returns endpoint by party including self party", func(t *testing.T) {
		var all []*orderers.Endpoint
		for i := 1; i <= 4; i++ {
			ep, err := cs.PartyEndpoint(types.PartyID(i))
			assert.NoError(t, err)
			all = append(all, ep)
		}
		consistsOf(t, expectedEndpoints, all)

		ep, err := cs.PartyEndpoint(5)
		assert.Nil(t, ep)
		assert.EqualError(t, err, "not found")
	})

	t.Run("an update that does not change the endpoints", func(t *testing.T) {
		cs.Update(setup.party2source)

		t.Log("endpoints are not refreshed")
		endpointsNotRefreshed(t, endpoints)

		t.Log("endpoints do not change")
		newEndpoints := cs.Endpoints()
		consistsOf(t, expectedEndpoints[1:], newEndpoints)
		ep, err := cs.PartyEndpoint(1)
		assert.NoError(t, err)
		assert.Equal(t, "party1-address", ep.Address)
	})
}

func TestConnectionSource_Update_PartOf(t *testing.T) {
	lg := flogging.MustGetLogger("arma.test")

	t.Run("an update to a party CA cert", func(t *testing.T) {
		setup := newTestSetup(t)
		cs := orderers.NewConnectionSource(lg, 1, 2, true)
		assert.NotNil(t, cs)

		cs.Update(setup.party2source)
		endpoints := cs.Endpoints()
		t.Log("drop cert5 from party 1")
		setup.party2source[1].RootCerts = [][]byte{setup.cert1} // drop cert5
		cs.Update(setup.party2source)

		t.Log("endpoints are refreshed")
		endpointsRefreshed(t, endpoints)

		expectedEndpoints := []*orderers.Endpoint{
			{
				Address:   "party1-address",
				RootCerts: [][]byte{setup.cert1},
			},
			{
				Address:   "party2-address",
				RootCerts: [][]byte{setup.cert2, setup.cert6},
			},
			{
				Address:   "party3-address",
				RootCerts: [][]byte{setup.cert3},
			},
			{
				Address:   "party4-address",
				RootCerts: [][]byte{setup.cert4},
			},
		}

		t.Log("new endpoints are as expected")
		newEndpoints := cs.Endpoints()
		consistsOf(t, expectedEndpoints[1:], newEndpoints)
		endpointsNotRefreshed(t, newEndpoints)
		ep, err := cs.PartyEndpoint(1)
		assert.NoError(t, err)
		assert.Len(t, ep.RootCerts, 1)
		assert.Equal(t, setup.cert1, ep.RootCerts[0])
	})

	t.Run("an update to a party address", func(t *testing.T) {
		setup := newTestSetup(t)
		cs := orderers.NewConnectionSource(lg, 1, 2, true)
		assert.NotNil(t, cs)

		cs.Update(setup.party2source)
		endpoints := cs.Endpoints()
		t.Log("change party 2 address")
		setup.party2source[2].Address = "party2-address-new"
		cs.Update(setup.party2source)

		t.Log("endpoints are refreshed")
		endpointsRefreshed(t, endpoints)

		expectedEndpoints := []*orderers.Endpoint{
			{
				Address:   "party1-address",
				RootCerts: [][]byte{setup.cert1, setup.cert5},
			},
			{
				Address:   "party2-address-new",
				RootCerts: [][]byte{setup.cert2, setup.cert6},
			},
			{
				Address:   "party3-address",
				RootCerts: [][]byte{setup.cert3},
			},
			{
				Address:   "party4-address",
				RootCerts: [][]byte{setup.cert4},
			},
		}

		t.Log("new endpoints are as expected")
		newEndpoints := cs.Endpoints()
		consistsOf(t, expectedEndpoints[1:], newEndpoints)
		endpointsNotRefreshed(t, newEndpoints)
	})

	t.Run("remove a party", func(t *testing.T) {
		setup := newTestSetup(t)
		cs := orderers.NewConnectionSource(lg, 1, 2, true)
		assert.NotNil(t, cs)

		cs.Update(setup.party2source)
		endpoints := cs.Endpoints()
		t.Log("delete party 1")
		delete(setup.party2source, 1)
		cs.Update(setup.party2source)

		t.Log("endpoints are refreshed")
		endpointsRefreshed(t, endpoints)

		expectedEndpoints := []*orderers.Endpoint{
			{
				Address:   "party2-address",
				RootCerts: [][]byte{setup.cert2, setup.cert6},
			},
			{
				Address:   "party3-address",
				RootCerts: [][]byte{setup.cert3},
			},
			{
				Address:   "party4-address",
				RootCerts: [][]byte{setup.cert4},
			},
		}

		t.Log("new endpoints are as expected")
		newEndpoints := cs.Endpoints()
		consistsOf(t, expectedEndpoints, newEndpoints)
		endpointsNotRefreshed(t, newEndpoints)
		ep, err := cs.PartyEndpoint(1)
		assert.EqualError(t, err, "not found")
		assert.Nil(t, ep)
	})

	t.Run("add a party", func(t *testing.T) {
		setup := newTestSetup(t)
		cs := orderers.NewConnectionSource(lg, 1, 2, true)
		assert.NotNil(t, cs)

		cs.Update(setup.party2source)
		endpoints := cs.Endpoints()
		t.Log("add party 5")
		setup.party2source[5] = &orderers.SourceEndpoint{
			Address:   "party5-address",
			RootCerts: [][]byte{setup.cert5},
		}
		cs.Update(setup.party2source)

		t.Log("endpoints are refreshed")
		endpointsRefreshed(t, endpoints)

		expectedEndpoints := []*orderers.Endpoint{
			{
				Address:   "party1-address",
				RootCerts: [][]byte{setup.cert1, setup.cert5},
			},
			{
				Address:   "party2-address",
				RootCerts: [][]byte{setup.cert2, setup.cert6},
			},
			{
				Address:   "party3-address",
				RootCerts: [][]byte{setup.cert3},
			},
			{
				Address:   "party4-address",
				RootCerts: [][]byte{setup.cert4},
			},
			{
				Address:   "party5-address",
				RootCerts: [][]byte{setup.cert5},
			},
		}

		t.Log("new endpoints are as expected")
		newEndpoints := cs.Endpoints()
		consistsOf(t, expectedEndpoints[1:], newEndpoints)
		endpointsNotRefreshed(t, newEndpoints)

		ep, err := cs.PartyEndpoint(5)
		assert.NoError(t, err)
		assert.Equal(t, "party5-address", ep.Address)
	})
}

func endpointsRefreshed(t *testing.T, endpoints []*orderers.Endpoint) {
	for _, ep := range endpoints {
		endpointRefreshed(t, ep)
	}
}

func endpointsNotRefreshed(t *testing.T, endpoints []*orderers.Endpoint) {
	for _, ep := range endpoints {
		endpointNotRefreshed(t, ep)
	}
}

func endpointNotRefreshed(t *testing.T, ep *orderers.Endpoint) {
	select {
	case <-ep.Refreshed:
		t.FailNow()
	default:
	}
}

func endpointRefreshed(t *testing.T, ep *orderers.Endpoint) {
	select {
	case <-ep.Refreshed:
	default:
		t.FailNow()
	}
}

func consistsOf(t *testing.T, expected, actual []*orderers.Endpoint) {
	assert.Equal(t, len(expected), len(actual))
	strippedExpected := stripEndpoints(expected)
	for _, ep := range stripEndpoints(actual) {
		assert.Contains(t, strippedExpected, ep)
	}
}

// stripEndpoints makes a comparable version of the endpoints specified.  This
// is necessary because the endpoint contains a channel which is not
// comparable.
func stripEndpoints(endpoints []*orderers.Endpoint) []orderers.SourceEndpoint {
	endpointsWithChannelStripped := make([]orderers.SourceEndpoint, len(endpoints))
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

//		It("does not include the self endpoint in random endpoint", func() { // there is a chance of failure here, but it is very small.
//			combinationMap := make(map[string]*orderers.Endpoint)
//			for i := 0; i < 10000; i++ {
//				r, _ := cs.RandomEndpoint()
//				combinationMap[r.Address] = r
//			}
//			var all []*orderers.Endpoint
//			for _, ep := range combinationMap {
//				all = append(all, ep)
//			}
//			Expect(stripEndpoints(all)).To(ConsistOf(
//				stripEndpoints(endpoints),
//			))
//		})
//
//		It("does not mark any of the endpoints as refreshed", func() {
//			for _, endpoint := range endpoints {
//				Expect(endpoint.Refreshed).NotTo(BeClosed())
//			}
//		})
//
//		When("an update does not modify the endpoint set", func() {
//			BeforeEach(func() {
//				cs.Update(nil, map[string]orderers.OrdererOrg{
//					"org1": org1,
//					"org2": org2,
//				})
//			})
//
//			It("does not update the endpoints", func() {
//				newEndpoints := cs.Endpoints()
//				Expect(newEndpoints).To(Equal(endpoints))
//			})
//
//			It("does not close any of the refresh channels", func() {
//				for _, endpoint := range endpoints {
//					Expect(endpoint.Refreshed).NotTo(BeClosed())
//				}
//			})
//		})
//
//		When("an update changes an org's TLS CA", func() {
//			BeforeEach(func() {
//				org1.RootCerts = [][]byte{cert1}
//
//				cs.Update(nil, map[string]orderers.OrdererOrg{
//					"org1": org1,
//					"org2": org2,
//				})
//			})
//
//			It("creates a new set of orderer endpoints yet skips the self-endpoint", func() {
//				newOrg1Certs := [][]byte{cert1}
//
//				newEndpoints := cs.Endpoints()
//				Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//					stripEndpoints([]*orderers.Endpoint{
//						{
//							Address:   "org1-address2",
//							RootCerts: newOrg1Certs,
//						},
//						{
//							Address:   "org2-address1",
//							RootCerts: org2Certs,
//						},
//						{
//							Address:   "org2-address2",
//							RootCerts: org2Certs,
//						},
//					}),
//				))
//			})
//
//			It("closes the refresh channel for all of the old endpoints", func() {
//				for _, endpoint := range endpoints {
//					Expect(endpoint.Refreshed).To(BeClosed())
//				}
//			})
//		})
//
//		When("an update changes an org's endpoint addresses", func() {
//			BeforeEach(func() {
//				org1.Addresses = []string{"org1-address1", "org1-address3"}
//				cs.Update(nil, map[string]orderers.OrdererOrg{
//					"org1": org1,
//					"org2": org2,
//				})
//			})
//
//			It("creates a new set of orderer endpoints, yet skips the self-endpoint", func() {
//				newEndpoints := cs.Endpoints()
//				Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//					stripEndpoints([]*orderers.Endpoint{
//						{
//							Address:   "org1-address3",
//							RootCerts: org1Certs,
//						},
//						{
//							Address:   "org2-address1",
//							RootCerts: org2Certs,
//						},
//						{
//							Address:   "org2-address2",
//							RootCerts: org2Certs,
//						},
//					}),
//				))
//			})
//
//			It("closes the refresh channel for all of the old endpoints", func() {
//				for _, endpoint := range endpoints {
//					Expect(endpoint.Refreshed).To(BeClosed())
//				}
//			})
//		})
//
//		When("an update removes an ordering organization", func() {
//			BeforeEach(func() {
//				cs.Update(nil, map[string]orderers.OrdererOrg{
//					"org2": org2,
//				})
//			})
//
//			It("creates a new set of orderer endpoints, self-endpoint matches nothing", func() {
//				newEndpoints := cs.Endpoints()
//				Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//					stripEndpoints([]*orderers.Endpoint{
//						{
//							Address:   "org2-address1",
//							RootCerts: org2Certs,
//						},
//						{
//							Address:   "org2-address2",
//							RootCerts: org2Certs,
//						},
//					}),
//				))
//			})
//
//			It("closes the refresh channel for all of the old endpoints", func() {
//				for _, endpoint := range endpoints {
//					Expect(endpoint.Refreshed).To(BeClosed())
//				}
//			})
//
//			When("the org is added back", func() {
//				BeforeEach(func() {
//					cs.Update(nil, map[string]orderers.OrdererOrg{
//						"org1": org1,
//						"org2": org2,
//					})
//				})
//
//				It("returns to the set of orderer endpoints, yet skips the self-endpoint", func() {
//					newEndpoints := cs.Endpoints()
//					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//						stripEndpoints([]*orderers.Endpoint{
//							{
//								Address:   "org1-address2",
//								RootCerts: org1Certs,
//							},
//							{
//								Address:   "org2-address1",
//								RootCerts: org2Certs,
//							},
//							{
//								Address:   "org2-address2",
//								RootCerts: org2Certs,
//							},
//						}),
//					))
//				})
//			})
//		})
//
//		When("an update modifies the global endpoints but does not affect the org endpoints", func() {
//			BeforeEach(func() {
//				cs.Update(nil, map[string]orderers.OrdererOrg{
//					"org1": org1,
//					"org2": org2,
//				})
//			})
//
//			It("does not update the endpoints", func() {
//				newEndpoints := cs.Endpoints()
//				Expect(newEndpoints).To(Equal(endpoints))
//			})
//
//			It("does not close any of the refresh channels", func() {
//				for _, endpoint := range endpoints {
//					Expect(endpoint.Refreshed).NotTo(BeClosed())
//				}
//			})
//		})
//
//		When("the configuration does not contain orderer org endpoints", func() {
//			var globalCerts [][]byte
//
//			BeforeEach(func() {
//				org1.Addresses = nil
//				org2.Addresses = nil
//
//				globalCerts = [][]byte{cert1, cert2, cert3}
//
//				cs.Update([]string{"global-addr1", "global-addr2"}, map[string]orderers.OrdererOrg{
//					"org1": org1,
//					"org2": org2,
//				})
//			})
//
//			It("creates endpoints for the global addrs", func() {
//				newEndpoints := cs.Endpoints()
//				Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//					stripEndpoints([]*orderers.Endpoint{
//						{
//							Address:   "global-addr1",
//							RootCerts: globalCerts,
//						},
//						{
//							Address:   "global-addr2",
//							RootCerts: globalCerts,
//						},
//					}),
//				))
//			})
//
//			It("closes the refresh channel for all of the old endpoints", func() {
//				for _, endpoint := range endpoints {
//					Expect(endpoint.Refreshed).To(BeClosed())
//				}
//			})
//
//			When("the global list of addresses grows", func() {
//				BeforeEach(func() {
//					cs.Update([]string{"global-addr1", "global-addr2", "global-addr3"}, map[string]orderers.OrdererOrg{
//						"org1": org1,
//						"org2": org2,
//					})
//				})
//
//				It("creates endpoints for the global addrs", func() {
//					newEndpoints := cs.Endpoints()
//					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//						stripEndpoints([]*orderers.Endpoint{
//							{
//								Address:   "global-addr1",
//								RootCerts: globalCerts,
//							},
//							{
//								Address:   "global-addr2",
//								RootCerts: globalCerts,
//							},
//							{
//								Address:   "global-addr3",
//								RootCerts: globalCerts,
//							},
//						}),
//					))
//				})
//
//				It("closes the refresh channel for all of the old endpoints", func() {
//					for _, endpoint := range endpoints {
//						Expect(endpoint.Refreshed).To(BeClosed())
//					}
//				})
//			})
//
//			When("the global set of addresses shrinks", func() {
//				BeforeEach(func() {
//					cs.Update([]string{"global-addr1"}, map[string]orderers.OrdererOrg{
//						"org1": org1,
//						"org2": org2,
//					})
//				})
//
//				It("creates endpoints for the global addrs", func() {
//					newEndpoints := cs.Endpoints()
//					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//						stripEndpoints([]*orderers.Endpoint{
//							{
//								Address:   "global-addr1",
//								RootCerts: globalCerts,
//							},
//						}),
//					))
//				})
//
//				It("closes the refresh channel for all of the old endpoints", func() {
//					for _, endpoint := range endpoints {
//						Expect(endpoint.Refreshed).To(BeClosed())
//					}
//				})
//			})
//
//			When("the global set of addresses is modified", func() {
//				BeforeEach(func() {
//					cs.Update([]string{"global-addr1", "global-addr3"}, map[string]orderers.OrdererOrg{
//						"org1": org1,
//						"org2": org2,
//					})
//				})
//
//				It("creates endpoints for the global addrs", func() {
//					newEndpoints := cs.Endpoints()
//					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//						stripEndpoints([]*orderers.Endpoint{
//							{
//								Address:   "global-addr1",
//								RootCerts: globalCerts,
//							},
//							{
//								Address:   "global-addr3",
//								RootCerts: globalCerts,
//							},
//						}),
//					))
//				})
//
//				It("closes the refresh channel for all of the old endpoints", func() {
//					for _, endpoint := range endpoints {
//						Expect(endpoint.Refreshed).To(BeClosed())
//					}
//				})
//			})
//
//			When("an update to the global addrs references an overridden org endpoint address", func() {
//				BeforeEach(func() {
//					cs.Update([]string{"global-addr1", "override-address"}, map[string]orderers.OrdererOrg{
//						"org1": org1,
//						"org2": org2,
//					},
//					)
//				})
//
//				It("creates a new set of orderer endpoints with overrides", func() {
//					newEndpoints := cs.Endpoints()
//					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//						stripEndpoints([]*orderers.Endpoint{
//							{
//								Address:   "global-addr1",
//								RootCerts: globalCerts,
//							},
//							{
//								Address:   "re-mapped-address",
//								RootCerts: overrideCerts,
//							},
//						}),
//					))
//				})
//			})
//
//			When("an orderer org adds an endpoint", func() {
//				BeforeEach(func() {
//					org1.Addresses = []string{"new-org1-address"}
//					cs.Update([]string{"global-addr1", "global-addr2"}, map[string]orderers.OrdererOrg{
//						"org1": org1,
//						"org2": org2,
//					})
//				})
//
//				It("removes the global endpoints and uses only the org level ones", func() {
//					newEndpoints := cs.Endpoints()
//					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//						stripEndpoints([]*orderers.Endpoint{
//							{
//								Address:   "new-org1-address",
//								RootCerts: org1Certs,
//							},
//						}),
//					))
//				})
//
//				It("closes the refresh channel for all of the old endpoints", func() {
//					for _, endpoint := range endpoints {
//						Expect(endpoint.Refreshed).To(BeClosed())
//					}
//				})
//			})
//		})
//
//		When("global endpoints are in effect and self-endpoint is from them", func() {
//			var globalCerts [][]byte
//
//			BeforeEach(func() {
//				cs = orderers.NewConnectionSource(flogging.MustGetLogger("peer.orderers"),
//					map[string]*orderers.Endpoint{
//						"override-address": {
//							Address:   "re-mapped-address",
//							RootCerts: overrideCerts,
//						},
//					},
//					"global-addr1") //<< self-endpoint from global endpoints
//
//				org1.Addresses = nil
//				org2.Addresses = nil
//
//				globalCerts = [][]byte{cert1, cert2, cert3}
//
//				cs.Update([]string{"global-addr1", "global-addr2"}, map[string]orderers.OrdererOrg{
//					"org1": org1,
//					"org2": org2,
//				})
//
//				endpoints = cs.Endpoints()
//			})
//
//			It("creates endpoints for the global endpoints, yet skips the self-endpoint", func() {
//				Expect(stripEndpoints(endpoints)).To(ConsistOf(
//					stripEndpoints([]*orderers.Endpoint{
//						{
//							Address:   "global-addr2",
//							RootCerts: globalCerts,
//						},
//					}),
//				))
//			})
//
//			When("the global list of addresses grows", func() {
//				BeforeEach(func() {
//					cs.Update([]string{"global-addr1", "global-addr2", "global-addr3"}, map[string]orderers.OrdererOrg{
//						"org1": org1,
//						"org2": org2,
//					})
//				})
//
//				It("creates endpoints for the global endpoints, yet skips the self-endpoint", func() {
//					newEndpoints := cs.Endpoints()
//					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//						stripEndpoints([]*orderers.Endpoint{
//							{
//								Address:   "global-addr2",
//								RootCerts: globalCerts,
//							},
//							{
//								Address:   "global-addr3",
//								RootCerts: globalCerts,
//							},
//						}),
//					))
//				})
//
//				It("closes the refresh channel for all of the old endpoints", func() {
//					for _, endpoint := range endpoints {
//						Expect(endpoint.Refreshed).To(BeClosed())
//					}
//				})
//			})
//
//			When("the global set of addresses shrinks, removing self endpoint", func() {
//				flogging.ActivateSpec("debug")
//				BeforeEach(func() {
//					cs.Update([]string{"global-addr2"}, map[string]orderers.OrdererOrg{
//						"org1": org1,
//						"org2": org2,
//					})
//				})
//
//				It("creates endpoints for the global addrs", func() {
//					newEndpoints := cs.Endpoints()
//					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//						stripEndpoints([]*orderers.Endpoint{
//							{
//								Address:   "global-addr2",
//								RootCerts: globalCerts,
//							},
//						}),
//					))
//				})
//
//				It("does not close the refresh channel for all of the old endpoints", func() {
//					for _, endpoint := range endpoints {
//						Expect(endpoint.Refreshed).NotTo(BeClosed())
//					}
//				})
//			})
//
//			When("the global set of addresses is modified", func() {
//				BeforeEach(func() {
//					cs.Update([]string{"global-addr1", "global-addr3"}, map[string]orderers.OrdererOrg{
//						"org1": org1,
//						"org2": org2,
//					})
//				})
//
//				It("creates endpoints for the global addrs", func() {
//					newEndpoints := cs.Endpoints()
//					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//						stripEndpoints([]*orderers.Endpoint{
//							{
//								Address:   "global-addr3",
//								RootCerts: globalCerts,
//							},
//						}),
//					))
//				})
//
//				It("closes the refresh channel for all of the old endpoints", func() {
//					for _, endpoint := range endpoints {
//						Expect(endpoint.Refreshed).To(BeClosed())
//					}
//				})
//			})
//
//			When("an update to the global addrs references an overridden org endpoint address", func() {
//				BeforeEach(func() {
//					cs.Update([]string{"global-addr1", "override-address"}, map[string]orderers.OrdererOrg{
//						"org1": org1,
//						"org2": org2,
//					},
//					)
//				})
//
//				It("creates a new set of orderer endpoints with overrides", func() {
//					newEndpoints := cs.Endpoints()
//					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//						stripEndpoints([]*orderers.Endpoint{
//							{
//								Address:   "re-mapped-address",
//								RootCerts: overrideCerts,
//							},
//						}),
//					))
//				})
//			})
//
//			When("an orderer org adds an endpoint", func() {
//				BeforeEach(func() {
//					org1.Addresses = []string{"new-org1-address"}
//					cs.Update([]string{"global-addr1", "global-addr2"}, map[string]orderers.OrdererOrg{
//						"org1": org1,
//						"org2": org2,
//					})
//				})
//
//				It("removes the global endpoints and uses only the org level ones", func() {
//					newEndpoints := cs.Endpoints()
//					Expect(stripEndpoints(newEndpoints)).To(ConsistOf(
//						stripEndpoints([]*orderers.Endpoint{
//							{
//								Address:   "new-org1-address",
//								RootCerts: org1Certs,
//							},
//						}),
//					))
//				})
//
//				It("closes the refresh channel for all of the old endpoints", func() {
//					for _, endpoint := range endpoints {
//						Expect(endpoint.Refreshed).To(BeClosed())
//					}
//				})
//			})
//		})
//	})
//})
