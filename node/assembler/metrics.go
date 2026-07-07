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
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/metadata"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	batchFetchLatencyOpts = metrics.HistogramOpts{
		Namespace:  "assembler",
		Name:       "batch_fetch_latency_seconds",
		Help:       "The latency to fetch a requested batch from the batcher.",
		LabelNames: []string{"party_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}

	baToBatchLatencyOpts = metrics.HistogramOpts{
		Namespace:  "assembler",
		Name:       "ba_to_batch_latency_seconds",
		Help:       "The latency from receiving a batch attestation until the matching batch is available.",
		LabelNames: []string{"party_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}

	batchLedgerAppendLatencyOpts = metrics.HistogramOpts{
		Namespace:  "assembler",
		Name:       "batch_ledger_append_latency_seconds",
		Help:       "The latency to append a batch to the ledger.",
		LabelNames: []string{"party_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}
)

type Metrics struct {
	ledgerMetrics            *node_ledger.AssemblerLedgerMetrics
	deliverMetrics           *deliver.Metrics
	batchFetchLatency        metrics.Histogram
	baToBatchLatency         metrics.Histogram
	batchLedgerAppendLatency metrics.Histogram
	logger                   *flogging.FabricLogger
	interval                 time.Duration
	stopChan                 chan struct{}
	stopOnce                 sync.Once
	startOnce                sync.Once
	partyID                  arma_types.PartyID
}

func NewMetrics(assemblerNodeConfig *config.AssemblerNodeConfig, ledgerMetrics *node_ledger.AssemblerLedgerMetrics, logger *flogging.FabricLogger) *Metrics {
	partyID := fmt.Sprintf("%d", assemblerNodeConfig.PartyId)

	provider := monitoring.NewProvider(assemblerNodeConfig.Metrics.Provider, logger)

	versionGauge := monitoring.VersionGauge(provider)
	versionGauge.With(metadata.Version).Set(1)

	ledgerMetrics.NewAssemblerLedgerMetrics(provider, partyID, logger)
	deliverMetrics := deliver.NewMetrics(provider)

	batchFetchLatency := provider.NewHistogram(batchFetchLatencyOpts).With([]string{partyID}...)
	baToBatchLatency := provider.NewHistogram(baToBatchLatencyOpts).With([]string{partyID}...)
	batchLedgerAppendLatency := provider.NewHistogram(batchLedgerAppendLatencyOpts).With([]string{partyID}...)

	return &Metrics{
		ledgerMetrics:            ledgerMetrics,
		deliverMetrics:           deliverMetrics,
		interval:                 assemblerNodeConfig.Metrics.MetricsLogInterval,
		logger:                   logger,
		stopChan:                 make(chan struct{}),
		partyID:                  assemblerNodeConfig.PartyId,
		batchFetchLatency:        batchFetchLatency,
		baToBatchLatency:         baToBatchLatency,
		batchLedgerAppendLatency: batchLedgerAppendLatency,
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

		txCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
		blocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
		blocksSizeCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksSize.(prometheus.Counter), m.logger))

		batchFetchLatencyAvg := monitoring.GetHistogramAverage(m.batchFetchLatency.(prometheus.Metric), m.logger)
		baToBatchLatencyAvg := monitoring.GetHistogramAverage(m.baToBatchLatency.(prometheus.Metric), m.logger)
		batchLedgerAppendLatencyAvg := monitoring.GetHistogramAverage(m.batchLedgerAppendLatency.(prometheus.Metric), m.logger)

		m.logger.Infof("ASSEMBLER_METRICS: party_id=%d, total: TXs=%d, blocks=%d, estimated_block_size=%d, batch_fetch_latency_avg_seconds=%.6f, ba_to_batch_latency_avg_seconds=%.6f, batch_ledger_append_latency_avg_seconds=%.6f", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted, batchFetchLatencyAvg, baToBatchLatencyAvg, batchLedgerAppendLatencyAvg)
	})
}

func (m *Metrics) trackMetrics() {
	lastTxCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
	lastBlocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			txCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
			blocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
			blocksSizeCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksSize.(prometheus.Counter), m.logger))

			newBlocks := uint64(0)
			if blocksCommitted > lastBlocksCommitted {
				newBlocks = blocksCommitted - lastBlocksCommitted
			}

			newTXs := uint64(0)
			if txCommitted > lastTxCommitted {
				newTXs = txCommitted - lastTxCommitted
			}

			batchFetchLatencyAvg := monitoring.GetHistogramAverage(m.batchFetchLatency.(prometheus.Metric), m.logger)
			baToBatchLatencyAvg := monitoring.GetHistogramAverage(m.baToBatchLatency.(prometheus.Metric), m.logger)
			batchLedgerAppendLatencyAvg := monitoring.GetHistogramAverage(m.batchLedgerAppendLatency.(prometheus.Metric), m.logger)

			m.logger.Infof("ASSEMBLER_METRICS: total: party_id=%d, TXs=%d, blocks=%d, estimated_block_size=%d, batch_fetch_latency_avg_seconds=%.6f, ba_to_batch_latency_avg_seconds=%.6f, batch_ledger_append_latency_avg_seconds=%.6f, in the last %.2f seconds: TXs=%d, blocks=%d", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted, batchFetchLatencyAvg, baToBatchLatencyAvg, batchLedgerAppendLatencyAvg, sec, newTXs, newBlocks)
			lastTxCommitted, lastBlocksCommitted = txCommitted, blocksCommitted
		case <-m.stopChan:
			return
		}
	}
}
