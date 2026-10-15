/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	"github.com/hyperledger/fabric-x-orderer/common/metadata"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	batchUnaryFetchLatencyOpts = metrics.HistogramOpts{
		Namespace:  "assembler",
		Name:       "batch_unary_fetch_latency_seconds",
		Help:       "The latency to unary fetch a requested batch from the batchers in the shard.",
		LabelNames: []string{"party_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1, 3, 5, 10, 30, 50, 100},
	}

	attestationToBatchCollationLatencyOpts = metrics.HistogramOpts{
		Namespace:  "assembler",
		Name:       "attestation_to_batch_collation_latency_seconds",
		Help:       "The latency from receiving a batch attestation until the matching batch is available.",
		LabelNames: []string{"party_id"},
		Buckets:    []float64{.00001, .00005, .0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1, 3, 5, 10, 30, 50, 100},
	}

	batchLedgerAppendLatencyOpts = metrics.HistogramOpts{
		Namespace:  "assembler",
		Name:       "batch_ledger_append_latency_seconds",
		Help:       "The latency to append a batch to the ledger.",
		LabelNames: []string{"party_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}

	prefetchIndexSizeOpts = metrics.GaugeOpts{
		Namespace:  "assembler",
		Name:       "prefetch_index_size_bytes",
		Help:       "The current size of the assembler prefetch index for a shard in bytes.",
		LabelNames: []string{"party_id", "shard_id"},
	}

	prefetchIndexCacheEvictionsTotalOpts = metrics.CounterOpts{
		Namespace:  "assembler",
		Name:       "prefetch_index_cache_evictions_total",
		Help:       "The total number of evictions from the assembler prefetch index cache.",
		LabelNames: []string{"party_id"},
	}
)

type Metrics struct {
	ledgerMetrics                      *node_ledger.AssemblerLedgerMetrics
	deliverMetrics                     *deliver.Metrics
	batchUnaryFetchLatency             metrics.Histogram
	attestationToBatchCollationLatency metrics.Histogram
	batchLedgerAppendLatency           metrics.Histogram
	prefetchIndexSize                  metrics.Gauge
	prefetchIndexCacheEvictionsTotal   metrics.Counter

	logger    *flogging.FabricLogger
	interval  time.Duration
	stopChan  chan struct{}
	stopOnce  sync.Once
	startOnce sync.Once
	wg        sync.WaitGroup
	partyID   arma_types.PartyID
}

func NewMetrics(assemblerNodeConfig *config.AssemblerNodeConfig, ledgerMetrics *node_ledger.AssemblerLedgerMetrics, logger *flogging.FabricLogger) *Metrics {
	partyID := fmt.Sprintf("%d", assemblerNodeConfig.PartyId)

	provider := monitoring.NewProvider(assemblerNodeConfig.Metrics.Provider, logger)

	versionGauge := monitoring.VersionGauge(provider)
	versionGauge.With(metadata.Version).Set(1)

	ledgerMetrics.NewAssemblerLedgerMetrics(provider, partyID)
	deliverMetrics := deliver.NewMetrics(provider)

	batchUnaryFetchLatency := provider.NewHistogram(batchUnaryFetchLatencyOpts).With([]string{partyID}...)
	attestationToBatchCollationLatency := provider.NewHistogram(attestationToBatchCollationLatencyOpts).With([]string{partyID}...)
	batchLedgerAppendLatency := provider.NewHistogram(batchLedgerAppendLatencyOpts).With([]string{partyID}...)

	prefetchIndexSize := provider.NewGauge(prefetchIndexSizeOpts)
	prefetchIndexCacheEvictionsTotal := provider.NewCounter(prefetchIndexCacheEvictionsTotalOpts).With([]string{partyID}...)

	return &Metrics{
		ledgerMetrics:                      ledgerMetrics,
		deliverMetrics:                     deliverMetrics,
		interval:                           assemblerNodeConfig.Metrics.MetricsLogInterval,
		logger:                             logger,
		stopChan:                           make(chan struct{}),
		partyID:                            assemblerNodeConfig.PartyId,
		batchUnaryFetchLatency:             batchUnaryFetchLatency,
		attestationToBatchCollationLatency: attestationToBatchCollationLatency,
		batchLedgerAppendLatency:           batchLedgerAppendLatency,
		prefetchIndexSize:                  prefetchIndexSize,
		prefetchIndexCacheEvictionsTotal:   prefetchIndexCacheEvictionsTotal,
	}
}

func (m *Metrics) StartMetricsTracker() {
	m.startOnce.Do(func() {
		if m.interval > 0 {
			m.wg.Go(m.trackMetrics)
		}
	})
}

func (m *Metrics) StopMetricsTracker() {
	m.stopOnce.Do(func() {
		m.logger.Infof("Reporting routine is stopping")
		close(m.stopChan)
		m.wg.Wait()

		txCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
		blocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
		blocksSizeCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksSize.(prometheus.Counter), m.logger))
		prefetchIndexCacheEvictions := uint64(monitoring.GetMetricValue(m.prefetchIndexCacheEvictionsTotal.(prometheus.Counter), m.logger))

		batchUnaryFetchLatencyAvg := monitoring.GetHistogramAverage(m.batchUnaryFetchLatency.(prometheus.Metric), m.logger)
		attestationToBatchCollationLatencyAvg := monitoring.GetHistogramAverage(m.attestationToBatchCollationLatency.(prometheus.Metric), m.logger)
		batchLedgerAppendLatencyAvg := monitoring.GetHistogramAverage(m.batchLedgerAppendLatency.(prometheus.Metric), m.logger)

		m.logger.Infof("ASSEMBLER_METRICS: party_id=%d, txs_total=%d, blocks_total=%d, estimated_block_size=%d, prefetch_index_cache_evictions=%d, batch_unary_fetch_latency_avg_seconds=%.6f, attestation_to_batch_collation_latency_avg_seconds=%.6f, batch_ledger_append_latency_avg_seconds=%.6f", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted, prefetchIndexCacheEvictions, batchUnaryFetchLatencyAvg, attestationToBatchCollationLatencyAvg, batchLedgerAppendLatencyAvg)
	})
}

func (m *Metrics) trackMetrics() {
	lastTxCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
	lastBlocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
	lastFetchSum, lastFetchCount := monitoring.GetHistogramSumAndCount(m.batchUnaryFetchLatency.(prometheus.Metric), m.logger)
	lastCollationSum, lastCollationCount := monitoring.GetHistogramSumAndCount(m.attestationToBatchCollationLatency.(prometheus.Metric), m.logger)
	lastAppendSum, lastAppendCount := monitoring.GetHistogramSumAndCount(m.batchLedgerAppendLatency.(prometheus.Metric), m.logger)
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			txCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
			blocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
			blocksSizeCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksSize.(prometheus.Counter), m.logger))
			prefetchIndexCacheEvictions := uint64(monitoring.GetMetricValue(m.prefetchIndexCacheEvictionsTotal.(prometheus.Counter), m.logger))

			newBlocks := uint64(0)
			if blocksCommitted > lastBlocksCommitted {
				newBlocks = blocksCommitted - lastBlocksCommitted
			}

			newTXs := uint64(0)
			if txCommitted > lastTxCommitted {
				newTXs = txCommitted - lastTxCommitted
			}

			fetchSum, fetchCount := monitoring.GetHistogramSumAndCount(m.batchUnaryFetchLatency.(prometheus.Metric), m.logger)
			collationSum, collationCount := monitoring.GetHistogramSumAndCount(m.attestationToBatchCollationLatency.(prometheus.Metric), m.logger)
			appendSum, appendCount := monitoring.GetHistogramSumAndCount(m.batchLedgerAppendLatency.(prometheus.Metric), m.logger)
			batchUnaryFetchLatencyAvg := monitoring.Average(fetchSum-lastFetchSum, fetchCount-lastFetchCount)
			attestationToBatchCollationLatencyAvg := monitoring.Average(collationSum-lastCollationSum, collationCount-lastCollationCount)
			batchLedgerAppendLatencyAvg := monitoring.Average(appendSum-lastAppendSum, appendCount-lastAppendCount)

			m.logger.Infof("ASSEMBLER_METRICS: party_id=%d, interval_s=%.2f, txs_total=%d, blocks_total=%d, estimated_block_size=%d, prefetch_index_cache_evictions=%d, txs_interval=%d, blocks_interval=%d, batch_unary_fetch_latency_interval_avg_seconds=%.6f, attestation_to_batch_collation_latency_interval_avg_seconds=%.6f, batch_ledger_append_latency_interval_avg_seconds=%.6f", m.partyID, sec, txCommitted, blocksCommitted, blocksSizeCommitted, prefetchIndexCacheEvictions, newTXs, newBlocks, batchUnaryFetchLatencyAvg, attestationToBatchCollationLatencyAvg, batchLedgerAppendLatencyAvg)
			lastTxCommitted, lastBlocksCommitted = txCommitted, blocksCommitted
			lastFetchSum, lastFetchCount = fetchSum, fetchCount
			lastCollationSum, lastCollationCount = collationSum, collationCount
			lastAppendSum, lastAppendCount = appendSum, appendCount
		case <-m.stopChan:
			return
		}
	}
}

func (m *Metrics) updatePrefetchIndexSize(shardID arma_types.ShardID, deltaBytes int) {
	m.prefetchIndexSize.With(fmt.Sprintf("%d", m.partyID), fmt.Sprintf("%d", shardID)).Add(float64(deltaBytes))
}

func (m *Metrics) resetPrefetchIndexSize(shardID arma_types.ShardID) {
	m.prefetchIndexSize.With(fmt.Sprintf("%d", m.partyID), fmt.Sprintf("%d", shardID)).Set(0)
}
