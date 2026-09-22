/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"crypto/tls"
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
)

var (
	incomingTxs = metrics.CounterOpts{
		Namespace:  "router",
		Name:       "requests_completed",
		Help:       "The number of incomming requests that have been completed.",
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
)

type RouterMetrics struct {
	incomingTxs            metrics.Counter
	rejectedTxsWithCode400 metrics.Counter
	rejectedTxsWithCode500 metrics.Counter
	throttledTxs           metrics.Counter
	incomingTxsLastValue   uint64
	logger                 *flogging.FabricLogger
	interval               time.Duration
	stopChan               chan struct{}
	stopOnce               sync.Once
	startOnce              sync.Once
	partyID                arma_types.PartyID
	promAddress            string
	promTLS                *tls.Config
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
		incomingTxs:            provider.NewCounter(incomingTxs).With([]string{partyID}...),
		rejectedTxsWithCode400: rejectedTxs.With([]string{"400", partyID}...),
		rejectedTxsWithCode500: rejectedTxs.With([]string{"500", partyID}...),
		throttledTxs:           provider.NewCounter(throttledTxs).With([]string{partyID}...),
		partyID:                routerNodeConfig.PartyID,
		promAddress:            routerNodeConfig.Metrics.PrometheusAddress,
		promTLS:                routerNodeConfig.Metrics.PrometheusTLS,
	}
}

func (m *RouterMetrics) StopMetricsTracker() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.logger.Infof("Reporting routine is stopping")
		m.reportMetrics(monitoring.NewReader(m.promAddress, m.interval, m.promTLS))
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

	reader := monitoring.NewReader(m.promAddress, m.interval, m.promTLS)

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			m.reportMetrics(reader)
		}
	}
}

func (m *RouterMetrics) reportMetrics(reader *monitoring.Reader) {
	partyID := fmt.Sprintf("%d", m.partyID)

	incomingTxsLastValue := atomic.LoadUint64(&m.incomingTxsLastValue)

	txCount, err := reader.Total(incomingTxs, partyID)
	if err != nil {
		m.logger.Warnf("Failed to read incoming transactions: %s", err)
		txCount = incomingTxsLastValue
	}

	txRejected400, err := reader.Total(rejectedTxs, "400", partyID)
	if err != nil {
		m.logger.Warnf("Failed to read rejected transactions with code 400: %s", err)
	}

	txRejected500, err := reader.Total(rejectedTxs, "500", partyID)
	if err != nil {
		m.logger.Warnf("Failed to read rejected transactions with code 500: %s", err)
	}

	txThrottled, err := reader.Total(throttledTxs, partyID)
	if err != nil {
		m.logger.Warnf("Failed to read throttled transactions: %s", err)
	}

	m.logger.Infof("ROUTER_METRICS: party_id=%d, transactions_received=%d, transactions_received_per_second=%.f, transactions_rejected_with_code_400=%d, transactions_rejected_with_code_500=%d, transactions_throttled=%d",
		m.partyID,
		txCount,
		(float64(txCount)-float64(incomingTxsLastValue))/m.interval.Seconds(),
		txRejected400,
		txRejected500,
		int(txThrottled))

	atomic.StoreUint64(&m.incomingTxsLastValue, txCount)
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
