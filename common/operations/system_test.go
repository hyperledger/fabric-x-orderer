/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package operations_test

import (
	"net/http"
	"runtime"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/operations"
	"github.com/stretchr/testify/require"
)

func TestOperationsSystemServesPprofWhenEnabled(t *testing.T) {
	system := startOperationsSystem(t, operations.Profile{Enabled: true})

	for _, page := range []string{"/debug/pprof/", "/debug/pprof/heap"} {
		resp, err := http.Get("http://" + system.Addr() + page)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, page)
	}
}

func TestOperationsSystemDoesNotServePprofWhenDisabled(t *testing.T) {
	system := startOperationsSystem(t, operations.Profile{Enabled: false})

	resp, err := http.Get("http://" + system.Addr() + "/debug/pprof/")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestOperationsSystemSetsMutexProfileFraction(t *testing.T) {
	startOperationsSystem(t, operations.Profile{Enabled: true, MutexProfileFraction: 7})
	t.Cleanup(func() { runtime.SetMutexProfileFraction(0) })

	// A negative value only reads the current fraction.
	require.Equal(t, 7, runtime.SetMutexProfileFraction(-1))
}

func startOperationsSystem(t *testing.T, profile operations.Profile) *operations.System {
	system := operations.NewOperationsSystem(
		operations.Operations{ListenAddress: "127.0.0.1:0", Profile: profile},
		operations.Metrics{Provider: "disabled"},
	)
	require.NoError(t, system.Start())
	t.Cleanup(func() { require.NoError(t, system.Stop()) })
	return system
}
