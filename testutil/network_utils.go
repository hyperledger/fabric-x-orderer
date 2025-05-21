/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func GetAvailablePort(t *testing.T) (port string, ll net.Listener) {
	addr := "127.0.0.1:0"
	listenConfig := net.ListenConfig{}

	ll, err := listenConfig.Listen(context.Background(), "tcp", addr)
	require.NoError(t, err)

	endpoint := ll.Addr().String()
	_, portS, err := net.SplitHostPort(endpoint)
	require.NoError(t, err)

	return portS, ll
}
