/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fabxhttp

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"

	"github.com/hyperledger/fabric-x-common/common/util"
	"github.com/hyperledger/fabric-x-orderer/common/fabxhttp/middleware"
)

// Logger defines the logging interface for the HTTP server.
type Logger interface {
	Warn(args ...any)
	Warnf(template string, args ...any)
}

// Options contains configuration options for the HTTP server.
type Options struct {
	Logger        Logger
	ListenAddress string
	TLS           TLS
}

// Server represents an HTTP server with TLS support and middleware capabilities.
type Server struct {
	logger     Logger
	options    Options
	httpServer *http.Server
	mux        *http.ServeMux
	addr       string
}

// NewServer creates a new HTTP server with the provided options.
func NewServer(o Options) *Server {
	logger := o.Logger
	if logger == nil {
		logger = flogging.MustGetLogger("fabhttp")
	}

	server := &Server{
		logger:  logger,
		options: o,
	}

	server.initializeServer()

	return server
}

// Run starts the server and blocks until a signal is received, then stops the server.
func (s *Server) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	err := s.Start()
	if err != nil {
		return err
	}

	close(ready)

	<-signals
	return s.Stop()
}

// Start begins listening and serving HTTP requests in a goroutine.
func (s *Server) Start() error {
	listener, err := s.Listen()
	if err != nil {
		return err
	}
	s.addr = listener.Addr().String()

	go func() {
		if err := s.httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			s.logger.Warnf("HTTP server stopped with error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the server with a timeout.
func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.httpServer.Shutdown(ctx)
}

func (s *Server) initializeServer() {
	s.mux = http.NewServeMux()
	s.httpServer = &http.Server{
		Addr:         s.options.ListenAddress,
		Handler:      s.mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 2 * time.Minute,
	}
}

// SecureHandlerChain wraps the provided handler with middleware including certificate requirement.
func (*Server) SecureHandlerChain(h http.Handler) http.Handler {
	return middleware.NewChain(middleware.RequireCert(), middleware.WithRequestID(util.GenerateUUID)).Handler(h)
}

// InsecureHandlerChain wraps the provided handler with basic middleware without certificate requirement.
func (*Server) InsecureHandlerChain(h http.Handler) http.Handler {
	return middleware.NewChain(middleware.WithRequestID(util.GenerateUUID)).Handler(h)
}

// RegisterHandler registers into the ServeMux a handler chain that borrows
// its security properties from the fabhttp.Server. This method is thread
// safe because ServeMux.Handle() is thread safe, and options are immutable.
// This method can be called either before or after Server.Start(). If the
// pattern exists the method panics.
//
//nolint:revive // secure parameter is part of the public API
func (s *Server) RegisterHandler(pattern string, handler http.Handler, secure bool) {
	var h http.Handler
	if secure {
		h = s.SecureHandlerChain(handler)
	} else {
		h = s.InsecureHandlerChain(handler)
	}
	s.mux.Handle(pattern, h)
}

// Listen creates a network listener with optional TLS configuration.
func (s *Server) Listen() (net.Listener, error) {
	listener, err := net.Listen("tcp", s.options.ListenAddress)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := s.options.TLS.Config()
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		listener = tls.NewListener(listener, tlsConfig)
	}
	return listener, nil
}

// Addr returns the server's listening address.
func (s *Server) Addr() string {
	return s.addr
}

// Log logs a warning message with the provided key-value pairs.
func (s *Server) Log(keyvals ...any) error {
	s.logger.Warn(keyvals...)
	return nil
}
