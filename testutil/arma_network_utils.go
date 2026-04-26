/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"fmt"
	"net"
	"os"
	"path"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/stretchr/testify/require"
)

// GenerateNetworkConfig create a network config which collects the enpoints of nodes per party.
// the generated network configuration includes 4 parties and 2 batchers for each party.
func GenerateNetworkConfig(t *testing.T, useTLSRouter string, useTLSAssembler string) generate.Network {
	localhost := "127.0.0.1"
	allocator := SharedTestPortAllocator()

	var parties []generate.Party
	var listeners []net.Listener
	for i := 0; i < 4; i++ {
		assemblerPort, assemblerListener := allocator.Allocate(t)
		consenterPort, consenterListener := allocator.Allocate(t)
		routerPort, routerListener := allocator.Allocate(t)
		batcher1Port, batcher1Listener := allocator.Allocate(t)
		batcher2Port, batcher2Listener := allocator.Allocate(t)

		party := generate.Party{
			ID:                types.PartyID(i + 1),
			AssemblerEndpoint: net.JoinHostPort(localhost, assemblerPort),
			ConsenterEndpoint: net.JoinHostPort(localhost, consenterPort),
			RouterEndpoint:    net.JoinHostPort(localhost, routerPort),
			BatchersEndpoints: []string{net.JoinHostPort(localhost, batcher1Port), net.JoinHostPort(localhost, batcher2Port)},
		}

		parties = append(parties, party)
		listeners = append(listeners, assemblerListener, consenterListener, routerListener, batcher1Listener, batcher2Listener)
	}

	network := generate.Network{
		Parties:         parties,
		UseTLSRouter:    useTLSRouter,
		UseTLSAssembler: useTLSAssembler,
	}

	for _, ll := range listeners {
		require.NoError(t, ll.Close())
	}

	return network
}

// GetUserConfig returns the armageddon generated user config object of a given party, for testing.
func GetUserConfig(baseDir string, partyID types.PartyID) (*armageddon.UserConfig, error) {
	userConfigPath := path.Join(baseDir, "config", fmt.Sprintf("party%d", partyID), "user_config.yaml")
	f, err := os.Open(userConfigPath)
	if err != nil {
		return nil, err
	}

	return armageddon.ReadUserConfig(&f)
}
