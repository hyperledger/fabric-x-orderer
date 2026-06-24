/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package operations

import (
	"net/url"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-lib-go/healthz"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/hyperledger/fabric-x-orderer/common/fabhttp"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/metadata"
)

// Logger defines the logging interface for the operations system.
type Logger interface {
	Warn(args ...any)
	Warnf(template string, args ...any)
}

// System represents the operations system with HTTP server, metrics, and health check capabilities.
type System struct {
	*fabhttp.Server
	metrics.Provider

	logger        Logger
	healthHandler *healthz.HealthHandler
	options       Options
}

// TLS contains configuration for TLS connections.
type TLS struct {
	Enabled               bool
	PrivateKey            string
	Certificate           string
	RootCAs               []string
	ClientAuthRequired    bool
	ClientRootCAs         []string
	TLSHandshakeTimeShift time.Duration
}

// Operations configures the operations endpoint for the orderer.
type Operations struct {
	// ListenAddress is the host and port for the operations server to listen on. It should be in the format of "host:port".
	ListenAddress string
	TLS           TLS
}

// Metrics configures the metrics provider for the orderer.
type Metrics struct {
	Provider           string
	MetricsLogInterval time.Duration
}

// MetricsOptions contains configuration for the metrics provider.
type MetricsOptions struct {
	Provider string
}

// Options contains all configuration options for the operations system.
type Options struct {
	fabhttp.Options
	Metrics MetricsOptions
	Version string
}

func operationServiceURL(scheme string, operationSubPath string, address string, logger *flogging.FabricLogger) string {
	uRL, err := url.JoinPath(scheme, address, operationSubPath)
	if err != nil {
		logger.Panicf("failed to construct metrics URL: %s", err)
	}
	return uRL
}

func PrometheusMetricsServiceURL(system *System, logger *flogging.FabricLogger) string {
	if system.options.TLS.Enabled {
		return operationServiceURL("https://", "metrics", system.Addr(), logger)
	}
	return operationServiceURL("http://", "metrics", system.Addr(), logger)
}

func HealthCheckServiceURL(system *System, logger *flogging.FabricLogger) string {
	return operationServiceURL("healthz", system.Addr(), logger)
}

// NewOperationsSystem creates a new operations system with the provided configuration.
func NewOperationsSystem(ops Operations, metricsConfig Metrics) *System {
	o := Options{
		Options: fabhttp.Options{
			Logger:        flogging.MustGetLogger("orderer.operations"),
			ListenAddress: ops.ListenAddress,
			TLS: fabhttp.TLS{
				Enabled:            ops.TLS.Enabled,
				CertFile:           ops.TLS.Certificate,
				KeyFile:            ops.TLS.PrivateKey,
				ClientCertRequired: ops.TLS.ClientAuthRequired,
				ClientCACertFiles:  ops.TLS.ClientRootCAs,
			},
		},
		Metrics: MetricsOptions{
			Provider: metricsConfig.Provider,
		},
		Version: metadata.Version,
	}

	logger := o.Logger
	if logger == nil {
		logger = flogging.MustGetLogger("operations.runner")
	}

	s := fabhttp.NewServer(o.Options)

	system := &System{
		Server:  s,
		logger:  logger,
		options: o,
	}

	system.initializeMetricsProvider()
	system.initializeHealthCheckHandler()

	return system
}

// Start begins the operations system server.
func (s *System) Start() error {
	return s.Server.Start()
}

// Stop gracefully shuts down the operations system server.
func (s *System) Stop() error {
	return s.Server.Stop()
}

// RegisterChecker registers a health checker for the specified component.
func (s *System) RegisterChecker(component string, checker healthz.HealthChecker) error {
	return s.healthHandler.RegisterChecker(component, checker)
}

func (s *System) initializeMetricsProvider() {
	// case "prometheus":
	// 	// s.Provider = provider
	// 	s.versionGauge = versionGauge(s.Provider)
	// 	// swagger:operation GET /metrics operations metrics
	// 	// ---
	// 	// responses:
	// 	//     '200':
	// 	//        description: Ok.
	s.RegisterHandler("/metrics", promhttp.Handler(), s.options.TLS.Enabled)
}

func (s *System) initializeHealthCheckHandler() {
	s.healthHandler = healthz.NewHealthHandler()
	// swagger:operation GET /healthz operations healthz
	// ---
	// summary: Retrieves all registered health checkers for the process.
	// responses:
	//     '200':
	//        description: Ok.
	//     '503':
	//        description: Service unavailable.
	s.RegisterHandler("/healthz", s.healthHandler, false)
}
