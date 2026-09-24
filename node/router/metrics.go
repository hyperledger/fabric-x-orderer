/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/metadata"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	arrivedTxs = metrics.CounterOpts{
		Namespace:  "router",
		Name:       "requests_arrived",
		Help:       "The number of requests that arrived at the router.",
		LabelNames: []string{"party_id"},
	}

	rejectedTxs = metrics.CounterOpts{
		Namespace:  "router",
		Name:       "requests_rejected",
		Help:       "The number of incomming requests that have been rejected.",
		LabelNames: []string{"code", "party_id"},
	}

	throttledTxs = metrics.CounterOpts{
		Namespace:  "router",
		Name:       "requests_throttled",
		Help:       "The number of incoming requests rejected by the rate limiter.",
		LabelNames: []string{"party_id"},
	}

	forwardedTxs = metrics.CounterOpts{
		Namespace:  "router",
		Name:       "requests_forwarded",
		Help:       "The number of requests that the router forwarded to a batcher or to the consenter.",
		LabelNames: []string{"party_id"},
	}
)

type RouterMetrics struct {
	arrivedTxs             metrics.Counter
	rejectedTxsWithCode400 metrics.Counter
	rejectedTxsWithCode500 metrics.Counter
	throttledTxs           metrics.Counter
	forwardedTxs           metrics.Counter
	arrivedTxsLastValue    uint64
	logger                 *flogging.FabricLogger
	interval               time.Duration
	stopChan               chan struct{}
	stopOnce               sync.Once
	startOnce              sync.Once
	partyID                arma_types.PartyID
}

// NewRouterMetrics creates the Metrics
func NewRouterMetrics(routerNodeConfig *config.RouterNodeConfig, logger *flogging.FabricLogger) *RouterMetrics {
	partyID := fmt.Sprintf("%d", routerNodeConfig.PartyID)
	provider := monitoring.NewProvider(routerNodeConfig.Metrics.Provider, logger)

	rejectedTxs := provider.NewCounter(rejectedTxs)
	versionGauge := monitoring.VersionGauge(provider)
	versionGauge.With(metadata.Version).Set(1)

	return &RouterMetrics{
		interval:               routerNodeConfig.Metrics.MetricsLogInterval,
		logger:                 logger,
		stopChan:               make(chan struct{}),
		arrivedTxs:             provider.NewCounter(arrivedTxs).With([]string{partyID}...),
		rejectedTxsWithCode400: rejectedTxs.With([]string{"400", partyID}...),
		rejectedTxsWithCode500: rejectedTxs.With([]string{"500", partyID}...),
		throttledTxs:           provider.NewCounter(throttledTxs).With([]string{partyID}...),
		forwardedTxs:           provider.NewCounter(forwardedTxs).With([]string{partyID}...),
		partyID:                routerNodeConfig.PartyID,
	}
}

func (m *RouterMetrics) StopMetricsTracker() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.logger.Infof("Reporting routine is stopping")
		m.reportMetrics()
	})
}

func (m *RouterMetrics) StartMetricsTracker() {
	m.startOnce.Do(func() {
		if m.interval > 0 {
			go m.trackMetrics()
		}
	})
}

func (m *RouterMetrics) trackMetrics() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	m.logger.Infof("Reporting routine is starting")

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			m.reportMetrics()
		}
	}
}

func (m *RouterMetrics) reportMetrics() {
	txCount := monitoring.GetMetricValue(m.arrivedTxs.(prometheus.Metric), m.logger)
	txRejected400 := monitoring.GetMetricValue(m.rejectedTxsWithCode400.(prometheus.Metric), m.logger)
	txRejected500 := monitoring.GetMetricValue(m.rejectedTxsWithCode500.(prometheus.Metric), m.logger)
	txThrottled := monitoring.GetMetricValue(m.throttledTxs.(prometheus.Metric), m.logger)
	txForwarded := monitoring.GetMetricValue(m.forwardedTxs.(prometheus.Metric), m.logger)
	arrivedTxsLastValue := atomic.LoadUint64(&m.arrivedTxsLastValue)
	m.logger.Infof("ROUTER_METRICS: party_id=%d, transactions_arrived=%d, transactions_arrived_per_second=%.f, transactions_rejected_with_code_400=%d, transactions_rejected_with_code_500=%d, transactions_throttled=%d, transactions_forwarded=%d",
		m.partyID, int(txCount), float64(txCount-float64(arrivedTxsLastValue))/m.interval.Seconds(), int(txRejected400), int(txRejected500), int(txThrottled), int(txForwarded))

	atomic.StoreUint64(&m.arrivedTxsLastValue, uint64(txCount))
}

func (m *RouterMetrics) increaseErrorCount(err error) {
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), "request verification error") ||
		strings.Contains(err.Error(), "request structure verification error") ||
		strings.Contains(err.Error(), "bad request") {
		m.rejectedTxsWithCode400.Add(1)
	}
	if strings.Contains(err.Error(), "server error") {
		m.rejectedTxsWithCode500.Add(1)
	}
}
