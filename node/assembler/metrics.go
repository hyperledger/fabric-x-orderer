/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/deliver"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/metadata"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
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

	logger      *flogging.FabricLogger
	interval    time.Duration
	stopChan    chan struct{}
	stopOnce    sync.Once
	startOnce   sync.Once
	partyID     arma_types.PartyID
	promAddress string
	promTLS     *tls.Config
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
		promAddress:                        assemblerNodeConfig.Metrics.PrometheusAddress,
		promTLS:                            assemblerNodeConfig.Metrics.PrometheusTLS,
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
			go m.trackMetrics()
		}
	})
}

func (m *Metrics) StopMetricsTracker() {
	m.stopOnce.Do(func() {
		m.logger.Infof("Reporting routine is stopping")
		close(m.stopChan)

		partyID := fmt.Sprintf("%d", m.partyID)
		reader := monitoring.NewReader(m.promAddress, m.interval, m.promTLS)

		txCommitted, err := reader.Total(node_ledger.TransactionCountOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read committed transactions count: %s", err)
		}

		blocksCommitted, err := reader.Total(node_ledger.BlocksCountOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read committed blocks count: %s", err)
		}

		blocksSizeCommitted, err := reader.Total(node_ledger.BlocksSizeOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read committed blocks size: %s", err)
		}

		prefetchIndexCacheEvictions, err := reader.Total(prefetchIndexCacheEvictionsTotalOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read prefetch index cache evictions: %s", err)
		}

		batchUnaryFetchLatencyAvg, err := reader.HistogramAverage(batchUnaryFetchLatencyOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read batch unary fetch latency: %s", err)
		}

		attestationToBatchCollationLatencyAvg, err := reader.HistogramAverage(attestationToBatchCollationLatencyOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read attestation to batch collation latency: %s", err)
		}

		batchLedgerAppendLatencyAvg, err := reader.HistogramAverage(batchLedgerAppendLatencyOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read batch ledger append latency: %s", err)
		}

		m.logger.Infof("ASSEMBLER_METRICS: party_id=%d, total: TXs=%d, blocks=%d, estimated_block_size=%d, batch_unary_fetch_latency_avg_seconds=%.6f, attestation_to_batch_collation_latency_avg_seconds=%.6f, batch_ledger_append_latency_avg_seconds=%.6f, prefetch_index_cache_evictions=%d", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted, batchUnaryFetchLatencyAvg, attestationToBatchCollationLatencyAvg, batchLedgerAppendLatencyAvg, prefetchIndexCacheEvictions)
	})
}

func (m *Metrics) trackMetrics() {
	sec := m.interval.Seconds()
	partyID := fmt.Sprintf("%d", m.partyID)

	reader := monitoring.NewReader(m.promAddress, m.interval, m.promTLS)
	lastTxCommitted, err := reader.Total(node_ledger.TransactionCountOpts, partyID)
	if err != nil {
		m.logger.Warnf("Failed to read initial committed transactions count: %s", err)
	}

	lastBlocksCommitted, err := reader.Total(node_ledger.BlocksCountOpts, partyID)
	if err != nil {
		m.logger.Warnf("Failed to read initial committed blocks count: %s", err)
	}

	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			txCommitted, err := reader.Total(node_ledger.TransactionCountOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read committed transactions count: %s", err)
				txCommitted = lastTxCommitted
			}

			blocksCommitted, err := reader.Total(node_ledger.BlocksCountOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read committed blocks count: %s", err)
				blocksCommitted = lastBlocksCommitted
			}

			blocksSizeCommitted, err := reader.Total(node_ledger.BlocksSizeOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read committed blocks size: %s", err)
			}

			prefetchIndexCacheEvictions, err := reader.Total(prefetchIndexCacheEvictionsTotalOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read prefetch index cache evictions: %s", err)
			}

			batchUnaryFetchLatencyAvg, err := reader.HistogramIntervalAverage(batchUnaryFetchLatencyOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read batch unary fetch latency: %s", err)
			}

			attestationToBatchCollationLatencyAvg, err := reader.HistogramIntervalAverage(attestationToBatchCollationLatencyOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read attestation to batch collation latency: %s", err)
			}

			batchLedgerAppendLatencyAvg, err := reader.HistogramIntervalAverage(batchLedgerAppendLatencyOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read batch ledger append latency: %s", err)
			}

			newBlocks := uint64(0)
			if blocksCommitted > lastBlocksCommitted {
				newBlocks = blocksCommitted - lastBlocksCommitted
			}

			newTXs := uint64(0)
			if txCommitted > lastTxCommitted {
				newTXs = txCommitted - lastTxCommitted
			}

			m.logger.Infof("ASSEMBLER_METRICS: total: party_id=%d, TXs=%d, blocks=%d, estimated_block_size=%d, batch_unary_fetch_latency_avg_seconds=%.6f, attestation_to_batch_collation_latency_avg_seconds=%.6f, batch_ledger_append_latency_avg_seconds=%.6f, prefetch_index_cache_evictions=%d, in the last %.2f seconds: TXs=%d, blocks=%d", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted, batchUnaryFetchLatencyAvg, attestationToBatchCollationLatencyAvg, batchLedgerAppendLatencyAvg, prefetchIndexCacheEvictions, sec, newTXs, newBlocks)
			lastTxCommitted, lastBlocksCommitted = txCommitted, blocksCommitted
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
