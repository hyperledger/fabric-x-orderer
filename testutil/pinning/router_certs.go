/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package pinning provides the router TLS material a test needs in order to exercise the certificate
// pinning a batcher performs: it only serves the router of its own party.
package pinning

import (
	"testing"

	"github.com/hyperledger/fabric-x-common/api/ordererpb"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm/tlsgen"
	"github.com/stretchr/testify/require"
)

const localhost = "127.0.0.1"

// CreateRouterKeyPairs creates the TLS (cert,key) pair of the router of each party, where the pair at
// index i belongs to the router of party i+1. The pairs are handed both to the batchers under test,
// which pin the certificate of the router of their own party, and to the routers themselves.
func CreateRouterKeyPairs(t *testing.T, ca tlsgen.CA, num int) []*tlsgen.CertKeyPair {
	routerKeyPairs := make([]*tlsgen.CertKeyPair, 0, num)
	for range num {
		// a server (cert,key) pair, since a router both serves clients and dials the batchers
		kp, err := ca.NewServerCertKeyPair(localhost)
		require.NoError(t, err)

		routerKeyPairs = append(routerKeyPairs, kp)
	}
	return routerKeyPairs
}

// ConfigurationWithRouters returns the minimal configuration a node of the given party needs in order
// to extract the router of its own party, where routerKeyPairs[i] is the (cert,key) pair of the router
// of party i+1, as created by CreateRouterKeyPairs.
func ConfigurationWithRouters(partyID types.PartyID, routerKeyPairs []*tlsgen.CertKeyPair) *config.Configuration {
	partiesConfig := make([]*ordererpb.PartyConfig, 0, len(routerKeyPairs))
	for i, kp := range routerKeyPairs {
		partiesConfig = append(partiesConfig, &ordererpb.PartyConfig{
			PartyID: uint32(i + 1),
			RouterConfig: &ordererpb.RouterNodeConfig{
				Host:    localhost,
				TlsCert: kp.Cert,
			},
		})
	}

	return &config.Configuration{
		LocalConfig:  &config.LocalConfig{NodeLocalConfig: &config.NodeLocalConfig{PartyID: partyID}},
		SharedConfig: &ordererpb.SharedConfig{PartiesConfig: partiesConfig},
	}
}
