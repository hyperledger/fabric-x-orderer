/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
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
)

type RouterMetrics struct {
	IncomingTxs            metrics.Counter
	RejectedTxs            metrics.Counter
	rejectedTxsWithCode400 atomic.Uint64
	rejectedTxsWithCode500 atomic.Uint64
	incoming               atomic.Uint64
	logger                 arma_types.Logger
	interval               time.Duration
	stopChan               chan struct{}
	stopOnce               sync.Once
	startOnce              sync.Once
	monitor                *monitoring.Monitor
	partyID                arma_types.PartyID
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

	return &RouterMetrics{
		interval:    interval,
		logger:      logger,
		stopChan:    make(chan struct{}),
		partyID:     routerNodeConfig.PartyID,
		monitor:     monitor,
		IncomingTxs: p.NewCounter(incomingTxs),
		RejectedTxs: p.NewCounter(rejectedTxs),
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
	var incomingTxsLastValue, rejectedTxsWithCode400LastValue, rejectedTxsWithCode500LastValue uint64
	partyID := fmt.Sprintf("%d", m.partyID)
	m.logger.Infof("Reporting routine is starting")

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			txCount := m.incoming.Load()
			rejectedTxsWithCode400Value := m.rejectedTxsWithCode400.Load()
			rejectedTxsWithCode500Value := m.rejectedTxsWithCode500.Load()

			if m.monitor != nil {
				m.IncomingTxs.With([]string{partyID}...).Add(float64(txCount - incomingTxsLastValue))
				m.RejectedTxs.With([]string{"400", partyID}...).Add(float64(rejectedTxsWithCode400Value - rejectedTxsWithCode400LastValue))
				m.RejectedTxs.With([]string{"500", partyID}...).Add(float64(rejectedTxsWithCode500Value - rejectedTxsWithCode500LastValue))
			}

			m.logger.Infof("Received %.f transactions per second", float64(txCount-incomingTxsLastValue)/m.interval.Seconds())
			incomingTxsLastValue = txCount
			rejectedTxsWithCode400LastValue = rejectedTxsWithCode400Value
			rejectedTxsWithCode500LastValue = rejectedTxsWithCode500Value
		}
	}
}
