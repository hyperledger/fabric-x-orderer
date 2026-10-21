/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package arma

import (
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strconv"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/fabhttp"
	"github.com/hyperledger/fabric-x-orderer/config"
)

// startProfilingServer sets the block and mutex profile rates and starts an HTTP
// server that serves the Go pprof endpoints under /debug/pprof/.
func startProfilingServer(profile *config.Profile, logger *flogging.FabricLogger) *fabhttp.Server {
	runtime.SetBlockProfileRate(profile.BlockProfileRate)
	runtime.SetMutexProfileFraction(profile.MutexProfileFraction)

	address := net.JoinHostPort(profile.ListenAddress, strconv.Itoa(int(profile.ListenPort)))
	server := fabhttp.NewServer(fabhttp.Options{
		Logger:        logger,
		ListenAddress: address,
	})

	// Index also serves the named profiles, e.g. /debug/pprof/heap and /debug/pprof/goroutine.
	server.RegisterHandler("/debug/pprof/", http.HandlerFunc(pprof.Index), false)
	server.RegisterHandler("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline), false)
	server.RegisterHandler("/debug/pprof/profile", http.HandlerFunc(pprof.Profile), false)
	server.RegisterHandler("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol), false)
	server.RegisterHandler("/debug/pprof/trace", http.HandlerFunc(pprof.Trace), false)

	if err := server.Start(); err != nil {
		panic(fmt.Sprintf("failed to start the pprof profiling server on %s: %s", address, err))
	}

	logger.Infof("Go pprof profiling server listening on %s, see http://%s/debug/pprof/", server.Addr(), server.Addr())
	return server
}
