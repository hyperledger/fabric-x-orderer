/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

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
	"github.com/prometheus/client_golang/prometheus"
)

var (
	decisionsCountOpts = metrics.CounterOpts{
		Namespace:  "consensus",
		Name:       "decisions_count",
		Help:       "Total number of decisions made by the consenter.",
		LabelNames: []string{"party_id"},
	}

	blocksCountOpts = metrics.CounterOpts{
		Namespace:  "consensus",
		Name:       "blocks_count",
		Help:       "Total number of blocks ordered by the consenter.",
		LabelNames: []string{"party_id"},
	}

	bafsCountOpts = metrics.CounterOpts{
		Namespace:  "consensus",
		Name:       "bafs_count",
		Help:       "Total number of batch attestation fragments received by the consenter.",
		LabelNames: []string{"party_id"},
	}

	complaintsCountOpts = metrics.CounterOpts{
		Namespace:  "consensus",
		Name:       "complaints_count",
		Help:       "Total number of complaints received by the consenter.",
		LabelNames: []string{"party_id"},
	}

	txsCountOpts = metrics.CounterOpts{
		Namespace:  "consensus",
		Name:       "txs_count",
		Help:       "Total number of transactions ordered by the consenter.",
		LabelNames: []string{"party_id"},
	}
)

type ConsensusMetrics struct {
	partyID   arma_types.PartyID
	logger    *flogging.FabricLogger
	interval  time.Duration
	stopChan  chan struct{}
	stopOnce  sync.Once
	startOnce sync.Once

	// metrics
	decisionsCount  metrics.Counter
	blocksCount     metrics.Counter
	bafsCount       metrics.Counter
	complaintsCount metrics.Counter
	txsCount        metrics.Counter
}

func NewConsensusMetrics(consenterNodeConfig *config.ConsenterNodeConfig, decisions uint64, txCount uint64, logger *flogging.FabricLogger) *ConsensusMetrics {
	partyID := fmt.Sprintf("%d", consenterNodeConfig.PartyId)

	provider := monitoring.NewProvider(consenterNodeConfig.Metrics.Provider, logger)

	versionGauge := monitoring.VersionGauge(provider)
	versionGauge.With(metadata.Version).Set(1)

	decisionsCount := provider.NewCounter(metrics.CounterOpts(decisionsCountOpts)).With([]string{partyID}...)
	decisionsCount.Add(float64(decisions))

	txsCount := provider.NewCounter(metrics.CounterOpts(txsCountOpts)).With([]string{partyID}...)
	txsCount.Add(float64(txCount))

	return &ConsensusMetrics{
		interval: consenterNodeConfig.Metrics.MetricsLogInterval,
		partyID:  consenterNodeConfig.PartyId,
		logger:   logger,
		stopChan: make(chan struct{}),

		decisionsCount:  decisionsCount,
		blocksCount:     provider.NewCounter(metrics.CounterOpts(blocksCountOpts)).With([]string{partyID}...),
		bafsCount:       provider.NewCounter(metrics.CounterOpts(bafsCountOpts)).With([]string{partyID}...),
		complaintsCount: provider.NewCounter(metrics.CounterOpts(complaintsCountOpts)).With([]string{partyID}...),
		txsCount:        txsCount,
	}
}

func (m *ConsensusMetrics) StartMetricsTracker() {
	m.startOnce.Do(func() {
		if m.interval > 0 {
			go m.trackMetrics()
		}
	})
}

func (m *ConsensusMetrics) StopMetricsTracker() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.logger.Infof("Reporting routine is stopping")
		m.logger.Infof("CONSENSUS_METRICS party_id=%d: decisions: total=%d, blocks: total=%d, bafs: total=%d, complaints: total=%d, txs: total=%d", m.partyID,
			uint64(monitoring.GetMetricValue(m.decisionsCount.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.blocksCount.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.bafsCount.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.complaintsCount.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.txsCount.(prometheus.Metric), m.logger)))
	})
}

func (m *ConsensusMetrics) trackMetrics() {
	prevDec, prevBlk := uint64(0), uint64(0)
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			dec := uint64(monitoring.GetMetricValue(m.decisionsCount.(prometheus.Metric), m.logger))
			blk := uint64(monitoring.GetMetricValue(m.blocksCount.(prometheus.Metric), m.logger))

			m.logger.Infof("CONSENSUS_METRICS party_id=%d: interval=%.2f sec, decisions: interval=%d, rate=%.4f, total=%d, blocks: interval=%d, rate=%.4f, total=%d, bafs: total=%d, complaints: total=%d",
				m.partyID,
				sec,
				dec-prevDec, float64(dec-prevDec)/sec,
				dec,
				blk-prevBlk, float64(blk-prevBlk)/sec,
				blk,
				uint64(monitoring.GetMetricValue(m.bafsCount.(prometheus.Metric), m.logger)),
				uint64(monitoring.GetMetricValue(m.complaintsCount.(prometheus.Metric), m.logger)),
			)

			prevDec, prevBlk = dec, blk

		case <-m.stopChan:
			return
		}
	}
}
