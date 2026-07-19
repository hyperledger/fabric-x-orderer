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
)

type Metrics struct {
	ledgerMetrics                      *node_ledger.AssemblerLedgerMetrics
	deliverMetrics                     *deliver.Metrics
	batchUnaryFetchLatency             metrics.Histogram
	attestationToBatchCollationLatency metrics.Histogram
	batchLedgerAppendLatency           metrics.Histogram
	logger                             *flogging.FabricLogger
	interval                           time.Duration
	stopChan                           chan struct{}
	stopOnce                           sync.Once
	startOnce                          sync.Once
	partyID                            arma_types.PartyID
}

func NewMetrics(assemblerNodeConfig *config.AssemblerNodeConfig, ledgerMetrics *node_ledger.AssemblerLedgerMetrics, logger *flogging.FabricLogger) *Metrics {
	partyID := fmt.Sprintf("%d", assemblerNodeConfig.PartyId)

	provider := monitoring.NewProvider(assemblerNodeConfig.Metrics.Provider, logger)

	versionGauge := monitoring.VersionGauge(provider)
	versionGauge.With(metadata.Version).Set(1)

	ledgerMetrics.NewAssemblerLedgerMetrics(provider, partyID, logger)
	deliverMetrics := deliver.NewMetrics(provider)

	batchUnaryFetchLatency := provider.NewHistogram(batchUnaryFetchLatencyOpts).With([]string{partyID}...)
	attestationToBatchCollationLatency := provider.NewHistogram(attestationToBatchCollationLatencyOpts).With([]string{partyID}...)
	batchLedgerAppendLatency := provider.NewHistogram(batchLedgerAppendLatencyOpts).With([]string{partyID}...)

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

		batchUnaryFetchLatencyAvg := monitoring.GetHistogramAverage(m.batchUnaryFetchLatency.(prometheus.Metric), m.logger)
		attestationToBatchCollationLatencyAvg := monitoring.GetHistogramAverage(m.attestationToBatchCollationLatency.(prometheus.Metric), m.logger)
		batchLedgerAppendLatencyAvg := monitoring.GetHistogramAverage(m.batchLedgerAppendLatency.(prometheus.Metric), m.logger)

		m.logger.Infof("ASSEMBLER_METRICS: party_id=%d, total: TXs=%d, blocks=%d, estimated_block_size=%d, batch_unary_fetch_latency_avg_seconds=%.6f, attestation_to_batch_collation_latency_avg_seconds=%.6f, batch_ledger_append_latency_avg_seconds=%.6f", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted, batchUnaryFetchLatencyAvg, attestationToBatchCollationLatencyAvg, batchLedgerAppendLatencyAvg)
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

			batchUnaryFetchLatencyAvg := monitoring.GetHistogramAverage(m.batchUnaryFetchLatency.(prometheus.Metric), m.logger)
			attestationToBatchCollationLatencyAvg := monitoring.GetHistogramAverage(m.attestationToBatchCollationLatency.(prometheus.Metric), m.logger)
			batchLedgerAppendLatencyAvg := monitoring.GetHistogramAverage(m.batchLedgerAppendLatency.(prometheus.Metric), m.logger)

			m.logger.Infof("ASSEMBLER_METRICS: total: party_id=%d, TXs=%d, blocks=%d, estimated_block_size=%d, batch_unary_fetch_latency_avg_seconds=%.6f, attestation_to_batch_collation_latency_avg_seconds=%.6f, batch_ledger_append_latency_avg_seconds=%.6f, in the last %.2f seconds: TXs=%d, blocks=%d", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted, batchUnaryFetchLatencyAvg, attestationToBatchCollationLatencyAvg, batchLedgerAppendLatencyAvg, sec, newTXs, newBlocks)
			lastTxCommitted, lastBlocksCommitted = txCommitted, blocksCommitted
		case <-m.stopChan:
			return
		}
	}
}
