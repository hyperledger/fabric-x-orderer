/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	currentRoleOpts = metrics.GaugeOpts{
		Namespace:  "batcher",
		Name:       "current_role",
		Help:       "The current role of the batcher: 1=primary, 2=secondary.",
		LabelNames: []string{"party_id", "shard_id"},
	}

	memPoolSizeOOpts = metrics.GaugeOpts{
		Namespace:  "batcher",
		Name:       "mempool_size",
		Help:       "The current size of the mempool.",
		LabelNames: []string{"party_id", "shard_id"},
	}

	roleChangesTotalOpts = metrics.CounterOpts{
		Namespace:  "batcher",
		Name:       "role_changes_total",
		Help:       "The total number of role changes.",
		LabelNames: []string{"party_id", "shard_id"},
	}

	batchesCreatedTotalOpts = metrics.CounterOpts{
		Namespace:  "batcher",
		Name:       "batches_created_total",
		Help:       "The total number of batches created.",
		LabelNames: []string{"party_id", "shard_id"},
	}

	batchesPulledTotalOpts = metrics.CounterOpts{
		Namespace:  "batcher",
		Name:       "batches_pulled_total",
		Help:       "The total number of batches pulled.",
		LabelNames: []string{"party_id", "shard_id"},
	}

	batchedTxsTotalOpts = metrics.CounterOpts{
		Namespace:  "batcher",
		Name:       "batched_txs_total",
		Help:       "The total number of transactions batched.",
		LabelNames: []string{"party_id", "shard_id"},
	}

	routerTxsTotalOpts = metrics.CounterOpts{
		Namespace:  "batcher",
		Name:       "router_txs_total",
		Help:       "The total number of transactions received from the router.",
		LabelNames: []string{"party_id", "shard_id"},
	}

	complaintsTotalOpts = metrics.CounterOpts{
		Namespace:  "batcher",
		Name:       "complaints_total",
		Help:       "The total number of complaints sent.",
		LabelNames: []string{"party_id", "shard_id"},
	}

	firstResendsTotalOpts = metrics.CounterOpts{
		Namespace:  "batcher",
		Name:       "first_resends_total",
		Help:       "The total number of first resends performed.",
		LabelNames: []string{"party_id", "shard_id"},
	}
)

type BatcherMetrics struct {
	partyID   arma_types.PartyID
	shardID   arma_types.ShardID
	logger    *flogging.FabricLogger
	interval  time.Duration
	stopChan  chan struct{}
	stopOnce  sync.Once
	startOnce sync.Once

	// metrics
	currentRole         metrics.Gauge // 1=primary, 2=secondary
	roleChangesTotal    metrics.Counter
	batchesCreatedTotal metrics.Counter
	batchesPulledTotal  metrics.Counter
	batchedTxsTotal     metrics.Counter
	routerTxsTotal      metrics.Counter
	complaintsTotal     metrics.Counter
	memPoolSize         metrics.Gauge
	firstResendsTotal   metrics.Counter

	// monitor
	monitor *monitoring.Monitor
}

func NewBatcherMetrics(batcherNodeConfig *config.BatcherNodeConfig, batchersInfo []config.BatcherInfo, ledger BatchLedger, logger *flogging.FabricLogger) *BatcherMetrics {
	host, port, err := net.SplitHostPort(batcherNodeConfig.MonitoringListenAddress)
	if err != nil {
		logger.Panicf("failed to get hostname: %v", err)
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		logger.Panicf("failed to convert port to int: %v", err)
	}
	partyID := fmt.Sprintf("%d", batcherNodeConfig.PartyId)
	shardID := fmt.Sprintf("%d", batcherNodeConfig.ShardId)

	monitor := monitoring.NewMonitor(monitoring.Endpoint{Host: host, Port: portInt}, fmt.Sprintf("batcher_%s_%s", partyID, shardID))
	p := monitor.Provider

	// initialize metrics from ledger
	var batches, pulled uint64
	for _, b := range batchersInfo {
		h := ledger.Height(b.PartyID)
		if batcherNodeConfig.PartyId != b.PartyID {
			pulled += h
		}
		batches += h
	}

	batchesPulledTotal := p.NewCounter(batchesPulledTotalOpts).With([]string{partyID, shardID}...)
	batchesPulledTotal.Add(float64(pulled))

	batchesCreatedTotal := p.NewCounter(batchesCreatedTotalOpts).With([]string{partyID, shardID}...)
	batchesCreatedTotal.Add(float64(batches))

	return &BatcherMetrics{
		interval: batcherNodeConfig.MetricsLogInterval,
		partyID:  batcherNodeConfig.PartyId,
		shardID:  batcherNodeConfig.ShardId,
		logger:   logger,
		stopChan: make(chan struct{}),
		monitor:  monitor,

		currentRole:         p.NewGauge(currentRoleOpts).With([]string{partyID, shardID}...),
		roleChangesTotal:    p.NewCounter(roleChangesTotalOpts).With([]string{partyID, shardID}...),
		batchesCreatedTotal: batchesCreatedTotal,
		batchesPulledTotal:  batchesPulledTotal,
		batchedTxsTotal:     p.NewCounter(batchedTxsTotalOpts).With([]string{partyID, shardID}...),
		routerTxsTotal:      p.NewCounter(routerTxsTotalOpts).With([]string{partyID, shardID}...),
		complaintsTotal:     p.NewCounter(complaintsTotalOpts).With([]string{partyID, shardID}...),
		memPoolSize:         p.NewGauge(memPoolSizeOOpts).With([]string{partyID, shardID}...),
		firstResendsTotal:   p.NewCounter(firstResendsTotalOpts).With([]string{partyID, shardID}...),
	}
}

func (m *BatcherMetrics) Start() {
	m.startOnce.Do(func() {
		m.monitor.Start()
		if m.interval > 0 {
			go m.trackMetrics()
		}
	})
}

func (m *BatcherMetrics) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.logger.Infof("Reporting routine is stopping")
		if m.monitor != nil {
			m.monitor.Stop()
			m.monitor = nil
		}
		m.logger.Infof("BATCHER_METRICS party_id=%d, shard_id=%d, role=%s, batches_created_total=%d, batches_pulled_total=%d, first_resends_total=%d, txs_total=%d, mempool_size=%d, router_txs_total=%d, role_changes_total=%d, complaints_total=%d",
			m.partyID,
			m.shardID,
			m.role(),
			uint64(monitoring.GetMetricValue(m.batchesCreatedTotal.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.batchesPulledTotal.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.firstResendsTotal.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.batchedTxsTotal.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.memPoolSize.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.routerTxsTotal.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.roleChangesTotal.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.complaintsTotal.(prometheus.Metric), m.logger)),
		)
	})
}

func (m *BatcherMetrics) trackMetrics() {
	prevC, prevP, prevR := float64(0), float64(0), float64(0)
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			created := monitoring.GetMetricValue(m.batchesCreatedTotal.(prometheus.Metric), m.logger)
			pulled := monitoring.GetMetricValue(m.batchesPulledTotal.(prometheus.Metric), m.logger)
			resends := monitoring.GetMetricValue(m.firstResendsTotal.(prometheus.Metric), m.logger)

			m.logger.Infof("BATCHER_METRICS party_id=%d, shard_id=%d, role=%s, interval_s=%.2f, batches_created_interval=%d, batches_created_rate=%.4f, batches_created_total=%d, batches_pulled_interval=%d, batches_pulled_rate=%.4f, batches_pulled_total=%d, first_resends_interval=%d, first_resend_rate=%.4f, first_resends_total=%d, txs_total=%d, mempool_size=%d, router_txs_total=%d, role_changes_total=%d, complaints_total=%d",
				m.partyID,
				m.shardID,
				m.role(),
				sec,
				uint64(created-prevC), created-prevC/sec, uint64(created),
				uint64(pulled-prevP), pulled-prevP/sec, uint64(pulled),
				uint64(resends-prevR), resends-prevR/sec, uint64(resends),
				uint64(monitoring.GetMetricValue(m.batchedTxsTotal.(prometheus.Metric), m.logger)),
				uint64(monitoring.GetMetricValue(m.memPoolSize.(prometheus.Metric), m.logger)),
				uint64(monitoring.GetMetricValue(m.routerTxsTotal.(prometheus.Metric), m.logger)),
				uint64(monitoring.GetMetricValue(m.roleChangesTotal.(prometheus.Metric), m.logger)),
				uint64(monitoring.GetMetricValue(m.complaintsTotal.(prometheus.Metric), m.logger)),
			)
			prevC, prevP, prevR = created, pulled, resends

		case <-m.stopChan:
			return
		}
	}
}

func (m *BatcherMetrics) role() string {
	currentRole := int(monitoring.GetMetricValue(m.currentRole.(prometheus.Metric), m.logger))
	if currentRole == 1 {
		return "primary"
	}
	return "secondary"
}
