/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package arma

import (
	"net/http"
	"runtime"
	"strconv"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

func TestProfilingServerServesPprof(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	server := startProfilingServer(&config.Profile{ListenAddress: "127.0.0.1"}, logger)
	t.Cleanup(func() { require.NoError(t, server.Stop()) })

	pages := []string{
		"/debug/pprof/",
		"/debug/pprof/heap",
		"/debug/pprof/goroutine?debug=1",
		"/debug/pprof/cmdline",
	}
	for _, page := range pages {
		resp, err := http.Get("http://" + server.Addr() + page)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, page)
	}
}

func TestProfilingServerSetsMutexProfileFraction(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	server := startProfilingServer(&config.Profile{ListenAddress: "127.0.0.1", MutexProfileFraction: 7}, logger)
	t.Cleanup(func() {
		require.NoError(t, server.Stop())
		runtime.SetMutexProfileFraction(0)
	})

	// A negative value only reads the current fraction.
	require.Equal(t, 7, runtime.SetMutexProfileFraction(-1))
}

func TestProfilingServerPanicsWhenPortIsTaken(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	port, listener := testutil.GetAvailablePort(t)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	takenPort, err := strconv.Atoi(port)
	require.NoError(t, err)

	require.Panics(t, func() {
		startProfilingServer(&config.Profile{ListenAddress: "127.0.0.1", ListenPort: uint32(takenPort)}, logger)
	})
}
