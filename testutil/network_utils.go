package testutil

import (
	"context"
	"net"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func GetAvailablePort(t *testing.T) (port string, ll net.Listener) {
	addr := "127.0.0.1:0"
	listenConfig := net.ListenConfig{Control: reusePort}

	ll, err := listenConfig.Listen(context.Background(), "tcp", addr)
	require.NoError(t, err)

	endpoint := ll.Addr().String()
	_, portS, err := net.SplitHostPort(endpoint)
	require.NoError(t, err)

	return portS, ll
}

func reusePort(network, address string, c syscall.RawConn) error {
	return c.Control(func(descriptor uintptr) {
		syscall.SetsockoptInt(int(descriptor), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	})
}
