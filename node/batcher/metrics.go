/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/metadata"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
)

var (
	currentRoleOpts = metrics.GaugeOpts{
		Namespace:  "batcher",
		Name:       "current_role",
		Help:       "The current role of the batcher: 1=primary, 2=secondary.",
		LabelNames: []string{"party_id", "shard_id"},
	}

	memPoolSizeOpts = metrics.GaugeOpts{
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

	batchMempoolNextRequestsLatencyOpts = metrics.HistogramOpts{
		Namespace:  "batcher",
		Name:       "batch_mempool_next_requests_latency_seconds",
		Help:       "The latency for the primary to retrieve the next batch from the mempool.",
		LabelNames: []string{"party_id", "shard_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}

	batchVerifyLatencyOpts = metrics.HistogramOpts{
		Namespace:  "batcher",
		Name:       "batch_verify_latency_seconds",
		Help:       "The latency from receiving a batch on the secondary until it is verified.",
		LabelNames: []string{"party_id", "shard_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}

	batchHashingLatencyOpts = metrics.HistogramOpts{
		Namespace:  "batcher",
		Name:       "batch_hashing_latency_seconds",
		Help:       "The latency to compute the batch requests digest.",
		LabelNames: []string{"party_id", "shard_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}
)

type BatcherMetrics struct {
	partyID     arma_types.PartyID
	shardID     arma_types.ShardID
	logger      *flogging.FabricLogger
	interval    time.Duration
	stopChan    chan struct{}
	stopOnce    sync.Once
	startOnce   sync.Once
	promAddress string

	ledgerMetrics *ledger.BatchLedgerMetrics

	// metrics
	currentRole                     metrics.Gauge // 1=primary, 2=secondary
	roleChangesTotal                metrics.Counter
	batchesCreatedTotal             metrics.Counter
	batchesPulledTotal              metrics.Counter
	batchedTxsTotal                 metrics.Counter
	routerTxsTotal                  metrics.Counter
	complaintsTotal                 metrics.Counter
	memPoolSize                     metrics.Gauge
	firstResendsTotal               metrics.Counter
	batchMempoolNextRequestsLatency metrics.Histogram
	batchVerifyLatency              metrics.Histogram
	batchHashingLatency             metrics.Histogram
}

func NewBatcherMetrics(batcherNodeConfig *config.BatcherNodeConfig, batchersInfo []config.BatcherInfo, ledgerMetrics *ledger.BatchLedgerMetrics, ledger BatchLedger, logger *flogging.FabricLogger) *BatcherMetrics {
	partyID := fmt.Sprintf("%d", batcherNodeConfig.PartyId)
	shardID := fmt.Sprintf("%d", batcherNodeConfig.ShardId)

	provider := monitoring.NewProvider(batcherNodeConfig.Metrics.Provider, logger)

	versionGauge := monitoring.VersionGauge(provider)
	versionGauge.With(metadata.Version).Set(1)

	ledgerMetrics.NewBatchLedgerMetrics(provider, partyID, shardID)

	// initialize metrics from ledger
	var batches, pulled uint64
	for _, b := range batchersInfo {
		h := ledger.Height(b.PartyID)
		if batcherNodeConfig.PartyId != b.PartyID {
			pulled += h
		}
		batches += h
	}

	batchesPulledTotal := provider.NewCounter(batchesPulledTotalOpts).With([]string{partyID, shardID}...)
	batchesPulledTotal.Add(float64(pulled))

	batchesCreatedTotal := provider.NewCounter(batchesCreatedTotalOpts).With([]string{partyID, shardID}...)
	batchesCreatedTotal.Add(float64(batches))

	return &BatcherMetrics{
		interval:      batcherNodeConfig.Metrics.MetricsLogInterval,
		promAddress:   batcherNodeConfig.Metrics.PrometheusAddress,
		partyID:       batcherNodeConfig.PartyId,
		shardID:       batcherNodeConfig.ShardId,
		logger:        logger,
		stopChan:      make(chan struct{}),
		ledgerMetrics: ledgerMetrics,

		currentRole:                     provider.NewGauge(currentRoleOpts).With([]string{partyID, shardID}...),
		roleChangesTotal:                provider.NewCounter(roleChangesTotalOpts).With([]string{partyID, shardID}...),
		batchesCreatedTotal:             batchesCreatedTotal,
		batchesPulledTotal:              batchesPulledTotal,
		batchedTxsTotal:                 provider.NewCounter(batchedTxsTotalOpts).With([]string{partyID, shardID}...),
		routerTxsTotal:                  provider.NewCounter(routerTxsTotalOpts).With([]string{partyID, shardID}...),
		complaintsTotal:                 provider.NewCounter(complaintsTotalOpts).With([]string{partyID, shardID}...),
		memPoolSize:                     provider.NewGauge(memPoolSizeOpts).With([]string{partyID, shardID}...),
		firstResendsTotal:               provider.NewCounter(firstResendsTotalOpts).With([]string{partyID, shardID}...),
		batchMempoolNextRequestsLatency: provider.NewHistogram(batchMempoolNextRequestsLatencyOpts).With([]string{partyID, shardID}...),
		batchVerifyLatency:              provider.NewHistogram(batchVerifyLatencyOpts).With([]string{partyID, shardID}...),
		batchHashingLatency:             provider.NewHistogram(batchHashingLatencyOpts).With([]string{partyID, shardID}...),
	}
}

func (m *BatcherMetrics) StartMetricsTracker() {
	m.startOnce.Do(func() {
		if m.interval > 0 {
			go m.trackMetrics()
		}
	})
}

func (m *BatcherMetrics) StopMetricsTracker() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.logger.Infof("Reporting routine is stopping")

		labels := m.labels()
		reader := monitoring.NewReader(m.promAddress, m.interval)

		role := reader.Gauge(currentRoleOpts, labels...)
		created := reader.Total(batchesCreatedTotalOpts, labels...)
		pulled := reader.Total(batchesPulledTotalOpts, labels...)
		resends := reader.Total(firstResendsTotalOpts, labels...)
		batchedTxs := reader.Total(batchedTxsTotalOpts, labels...)
		memPool := reader.Gauge(memPoolSizeOpts, labels...)
		routerTxs := reader.Total(routerTxsTotalOpts, labels...)
		roleChanges := reader.Total(roleChangesTotalOpts, labels...)
		complaints := reader.Total(complaintsTotalOpts, labels...)
		mempoolNextLatency := reader.HistogramAverage(batchMempoolNextRequestsLatencyOpts, labels...)
		verifyLatency := reader.HistogramAverage(batchVerifyLatencyOpts, labels...)
		hashingLatency := reader.HistogramAverage(batchHashingLatencyOpts, labels...)
		ledgerHashingLatency := reader.HistogramAverage(ledger.HeaderHashingLatencyOpts, labels...)
		ledgerAppendLatency := reader.HistogramAverage(ledger.AppendLatencyOpts, labels...)

		if err := reader.Err(); err != nil {
			m.logger.Warnf("Failed to read final metrics: %s", err)
			return
		}

		m.logger.Infof(
			"BATCHER_METRICS party_id=%d, shard_id=%d, role=%s, batches_created_total=%d, batches_pulled_total=%d, first_resends_total=%d, txs_total=%d, mempool_size=%d, router_txs_total=%d, role_changes_total=%d, complaints_total=%d, batch_mempool_next_requests_latency_avg_seconds=%.6f, batch_verify_latency_avg_seconds=%.6f, batch_hashing_latency_avg_seconds=%.6f, batch_ledger_header_hashing_latency_avg_seconds=%.6f, batch_ledger_append_latency_avg_seconds=%.6f",
			m.partyID,
			m.shardID,
			roleName(role),
			created,
			pulled,
			resends,
			batchedTxs,
			uint64(memPool),
			routerTxs,
			roleChanges,
			complaints,
			mempoolNextLatency,
			verifyLatency,
			hashingLatency,
			ledgerHashingLatency,
			ledgerAppendLatency,
		)
	})
}

func (m *BatcherMetrics) trackMetrics() {
	sec := m.interval.Seconds()
	labels := m.labels()

	reader := monitoring.NewReader(m.promAddress, m.interval)
	prevC := reader.Total(batchesCreatedTotalOpts, labels...)
	prevP := reader.Total(batchesPulledTotalOpts, labels...)
	prevR := uint64(0)
	if err := reader.Err(); err != nil {
		m.logger.Warnf("Failed to read initial metrics: %s", err)
		prevC, prevP, prevR = 0, 0, 0
	}

	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			reader := monitoring.NewReader(m.promAddress, m.interval)

			role := reader.Gauge(currentRoleOpts, labels...)
			created := reader.Total(batchesCreatedTotalOpts, labels...)
			pulled := reader.Total(batchesPulledTotalOpts, labels...)
			resends := reader.Total(firstResendsTotalOpts, labels...)
			batchedTxs := reader.Total(batchedTxsTotalOpts, labels...)
			memPool := reader.Gauge(memPoolSizeOpts, labels...)
			routerTxs := reader.Total(routerTxsTotalOpts, labels...)
			roleChanges := reader.Total(roleChangesTotalOpts, labels...)
			complaints := reader.Total(complaintsTotalOpts, labels...)
			mempoolNextLatency := reader.HistogramIntervalAverage(batchMempoolNextRequestsLatencyOpts, labels...)
			verifyLatency := reader.HistogramIntervalAverage(batchVerifyLatencyOpts, labels...)
			hashingLatency := reader.HistogramIntervalAverage(batchHashingLatencyOpts, labels...)
			ledgerHashingLatency := reader.HistogramIntervalAverage(ledger.HeaderHashingLatencyOpts, labels...)
			ledgerAppendLatency := reader.HistogramIntervalAverage(ledger.AppendLatencyOpts, labels...)

			if err := reader.Err(); err != nil {
				m.logger.Warnf("Skipping metrics report: %s", err)
				continue
			}

			m.logger.Infof(
				"BATCHER_METRICS party_id=%d, shard_id=%d, role=%s, interval_s=%.2f, batches_created_interval=%d, batches_created_rate=%.4f, batches_created_total=%d, batches_pulled_interval=%d, batches_pulled_rate=%.4f, batches_pulled_total=%d, first_resends_interval=%d, first_resends_rate=%.4f, first_resends_total=%d, txs_total=%d, mempool_size=%d, router_txs_total=%d, role_changes_total=%d, complaints_total=%d, batch_mempool_next_requests_latency_avg_seconds=%.6f, batch_verify_latency_avg_seconds=%.6f, batch_hashing_latency_avg_seconds=%.6f, batch_ledger_header_hashing_latency_avg_seconds=%.6f, batch_ledger_append_latency_avg_seconds=%.6f",
				m.partyID,
				m.shardID,
				roleName(role),
				sec,
				created-prevC, float64(created-prevC)/sec, created,
				pulled-prevP, float64(pulled-prevP)/sec, pulled,
				resends-prevR, float64(resends-prevR)/sec, resends,
				batchedTxs,
				uint64(memPool),
				routerTxs,
				roleChanges,
				complaints,
				mempoolNextLatency,
				verifyLatency,
				hashingLatency,
				ledgerHashingLatency,
				ledgerAppendLatency,
			)
			prevC, prevP, prevR = created, pulled, resends

		case <-m.stopChan:
			return
		}
	}
}

func (m *BatcherMetrics) labels() []string {
	return []string{fmt.Sprintf("%d", m.partyID), fmt.Sprintf("%d", m.shardID)}
}

func roleName(currentRole float64) string {
	if int(currentRole) == 1 {
		return "primary"
	}
	return "secondary"
}
