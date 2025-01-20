package armageddon

import (
	"net"
	"os"
	"testing"

	"arma/common/types"

	"github.com/stretchr/testify/require"
)

func TestGenerateCryptoConfig(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	networkConfig := prepareNetworkConfig(t)
	err = GenerateCryptoConfig(networkConfig, dir)
	require.NoError(t, err)
}

func prepareNetworkConfig(t *testing.T) *Network {
	var parties []Party
	var listeners []net.Listener
	for i := 0; i < 4; i++ {
		assemblerPort, lla := getAvailablePort(t)
		consenterPort, llc := getAvailablePort(t)
		routerPort, llr := getAvailablePort(t)
		batcher1Port, llb1 := getAvailablePort(t)
		batcher2Port, llb2 := getAvailablePort(t)

		party := Party{
			ID:                types.PartyID(i + 1),
			AssemblerEndpoint: "127.0.0.1:" + assemblerPort,
			ConsenterEndpoint: "127.0.0.1:" + consenterPort,
			RouterEndpoint:    "127.0.0.1:" + routerPort,
			BatchersEndpoints: []string{"127.0.0.1:" + batcher1Port, "127.0.0.1:" + batcher2Port},
		}

		parties = append(parties, party)
		listeners = append(listeners, lla, llc, llr, llb1, llb2)
	}

	network := Network{
		Parties: parties,
	}

	for _, ll := range listeners {
		require.NoError(t, ll.Close())
	}

	return &network
}
