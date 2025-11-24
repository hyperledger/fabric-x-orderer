/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/prometheus/client_golang/prometheus"
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
)

type RouterMetrics struct {
	incomingTxs            metrics.Counter
	rejectedTxsWithCode400 metrics.Counter
	rejectedTxsWithCode500 metrics.Counter
	logger                 arma_types.Logger
	interval               time.Duration
	stopChan               chan struct{}
	stopOnce               sync.Once
	startOnce              sync.Once
	monitor                *monitoring.Monitor
}

// NewRouterMetrics creates the Metrics
func NewRouterMetrics(routerNodeConfig *config.RouterNodeConfig, logger arma_types.Logger, interval time.Duration) *RouterMetrics {
	host, port, err := net.SplitHostPort(routerNodeConfig.MonitoringListenAddress)
	if err != nil {
		logger.Panicf("failed to get hostname: %v", err)
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		logger.Panicf("failed to convert port to int: %v", err)
	}

	monitor := monitoring.NewMonitor(monitoring.Endpoint{Host: host, Port: portInt}, "router")
	p := monitor.Provider
	partyID := fmt.Sprintf("%d", routerNodeConfig.PartyID)

	rejectedTxs := p.NewCounter(rejectedTxs)

	return &RouterMetrics{
		interval:               interval,
		logger:                 logger,
		stopChan:               make(chan struct{}),
		monitor:                monitor,
		incomingTxs:            p.NewCounter(incomingTxs).With([]string{partyID}...),
		rejectedTxsWithCode400: rejectedTxs.With([]string{"400", partyID}...),
		rejectedTxsWithCode500: rejectedTxs.With([]string{"500", partyID}...),
	}
}

func (m *RouterMetrics) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.logger.Infof("Reporting routine is stopping")
		if m.monitor != nil {
			m.monitor.Stop()
			m.monitor = nil
		}
	})
}

func (m *RouterMetrics) Start() {
	m.startOnce.Do(func() {
		m.monitor.Start()
		if m.interval > 0 {
			go m.trackMetrics()
		}
	})
}

func (m *RouterMetrics) trackMetrics() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	var incomingTxsLastValue float64
	m.logger.Infof("Reporting routine is starting")

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			txCount := monitoring.GetMetricValue(m.incomingTxs.(prometheus.Metric), m.logger)
			m.logger.Infof("Received %.f transactions per second", float64(txCount-incomingTxsLastValue)/m.interval.Seconds())
			incomingTxsLastValue = txCount
		}
	}
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
