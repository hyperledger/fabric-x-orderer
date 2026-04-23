/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package armageddon_test

import (
	"net"
	"os"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/tools/armageddon"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	genconfig "github.com/hyperledger/fabric-x-orderer/config/generate"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

func TestGenerateCryptoConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	networkConfig := prepareNetworkConfig(t)
	err = armageddon.GenerateCryptoConfig(networkConfig, dir)
	require.NoError(t, err)
}

func prepareNetworkConfig(t *testing.T) *genconfig.Network {
	localhost := "127.0.0.1"
	var parties []genconfig.Party
	var listeners []net.Listener
	for i := 0; i < 4; i++ {
		assemblerPort, lla := testutil.SharedTestPortAllocator().Allocate(t)
		consenterPort, llc := testutil.SharedTestPortAllocator().Allocate(t)
		routerPort, llr := testutil.SharedTestPortAllocator().Allocate(t)
		batcher1Port, llb1 := testutil.SharedTestPortAllocator().Allocate(t)
		batcher2Port, llb2 := testutil.SharedTestPortAllocator().Allocate(t)

		party := genconfig.Party{
			ID:                types.PartyID(i + 1),
			AssemblerEndpoint: net.JoinHostPort(localhost, assemblerPort),
			ConsenterEndpoint: net.JoinHostPort(localhost, consenterPort),
			RouterEndpoint:    net.JoinHostPort(localhost, routerPort),
			BatchersEndpoints: []string{net.JoinHostPort(localhost, batcher1Port), net.JoinHostPort(localhost, batcher2Port)},
		}

		parties = append(parties, party)
		listeners = append(listeners, lla, llc, llr, llb1, llb2)
	}

	network := genconfig.Network{
		Parties:         parties,
		UseTLSRouter:    "none",
		UseTLSAssembler: "none",
	}

	for _, ll := range listeners {
		require.NoError(t, ll.Close())
	}

	return &network
}
