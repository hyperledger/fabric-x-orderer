/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package operations

import (
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/flogging/httpadmin"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-lib-go/healthz"
	"github.com/hyperledger/fabric/common/fabhttp"
	"github.com/hyperledger/fabric/common/metadata"
	"github.com/hyperledger/fabric/core/operations"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type System struct {
	*fabhttp.Server
	metrics.Provider

	logger        operations.Logger
	healthHandler *healthz.HealthHandler
	options       operations.Options
	// versionGauge  metrics.Gauge
}

func NewOperationsSystem(ops localconfig.Operations, metrics localconfig.Metrics) *System {
	o := operations.Options{
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
		Metrics: operations.MetricsOptions{
			Provider: metrics.Provider,
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

	system.initializeHealthCheckHandler()
	system.initializeLoggingHandler()
	system.initializeMetricsProvider()
	system.initializeVersionInfoHandler()

	return system
}

func (s *System) Start() error {
	return s.Server.Start()
}

func (s *System) Stop() error {
	return s.Server.Stop()
}

func (s *System) RegisterChecker(component string, checker healthz.HealthChecker) error {
	return s.healthHandler.RegisterChecker(component, checker)
}

func (s *System) initializeMetricsProvider() error {
	// m := s.options.Metrics
	// providerType := m.Provider
	// switch providerType {

	// case "prometheus":
	// 	// s.Provider = provider
	// 	s.versionGauge = versionGauge(s.Provider)
	// 	// swagger:operation GET /metrics operations metrics
	// 	// ---
	// 	// responses:
	// 	//     '200':
	// 	//        description: Ok.
	s.RegisterHandler("/metrics", promhttp.Handler(), s.options.TLS.Enabled)
	return nil

	// default:
	// 	if providerType != "disabled" {
	// 		s.logger.Warnf("Unknown provider type: %s; metrics disabled", providerType)
	// 	}

	// 	s.Provider = &disabled.Provider{}
	// 	s.versionGauge = versionGauge(s.Provider)
	// 	return nil
	// }
}

func (s *System) initializeLoggingHandler() {
	// swagger:operation GET /logspec operations logspecget
	// ---
	// summary: Retrieves the active logging spec for a peer or orderer.
	// responses:
	//     '200':
	//        description: Ok.

	// swagger:operation PUT /logspec operations logspecput
	// ---
	// summary: Updates the active logging spec for a peer or orderer.
	//
	// parameters:
	// - name: payload
	//   in: formData
	//   type: string
	//   description: The payload must consist of a single attribute named spec.
	//   required: true
	// responses:
	//     '204':
	//        description: No content.
	//     '400':
	//        description: Bad request.
	// consumes:
	//   - multipart/form-data
	s.RegisterHandler("/logspec", httpadmin.NewSpecHandler(), s.options.TLS.Enabled)
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

func (s *System) initializeVersionInfoHandler() {
	versionInfo := &operations.VersionInfoHandler{
		CommitSHA: metadata.CommitSHA,
		Version:   metadata.Version,
	}
	// swagger:operation GET /version operations version
	// ---
	// summary: Returns the orderer or peer version and the commit SHA on which the release was created.
	// responses:
	//     '200':
	//        description: Ok.
	s.RegisterHandler("/version", versionInfo, false)
}
