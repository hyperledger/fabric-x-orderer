package testutil

import (
	"fmt"
	"net"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/decentralized-trust-research/arma/cmd/armageddon"
	"github.ibm.com/decentralized-trust-research/arma/common/types"
	"github.ibm.com/decentralized-trust-research/arma/config/generate"
)

// GenerateNetworkConfig create a network config which collects the enpoints of nodes per party.
// the generated network configuration includes 4 parties and 2 batchers for each party.
func GenerateNetworkConfig(t *testing.T, useTLSRouter string, useTLSAssembler string) generate.Network {
	var parties []generate.Party
	var listeners []net.Listener
	for i := 0; i < 4; i++ {
		assemblerPort, lla := GetAvailablePort(t)
		consenterPort, llc := GetAvailablePort(t)
		routerPort, llr := GetAvailablePort(t)
		batcher1Port, llb1 := GetAvailablePort(t)
		batcher2Port, llb2 := GetAvailablePort(t)

		party := generate.Party{
			ID:                types.PartyID(i + 1),
			AssemblerEndpoint: "127.0.0.1:" + assemblerPort,
			ConsenterEndpoint: "127.0.0.1:" + consenterPort,
			RouterEndpoint:    "127.0.0.1:" + routerPort,
			BatchersEndpoints: []string{"127.0.0.1:" + batcher1Port, "127.0.0.1:" + batcher2Port},
		}

		parties = append(parties, party)
		listeners = append(listeners, lla, llc, llr, llb1, llb2)
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
