/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/internal/cryptogen/metadata"
	"github.com/hyperledger/fabric-x-orderer/node/config"
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
	partyID     arma_types.PartyID
	logger      *flogging.FabricLogger
	interval    time.Duration
	stopChan    chan struct{}
	stopOnce    sync.Once
	startOnce   sync.Once
	promAddress string
	promTLS     *tls.Config

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
		interval:    consenterNodeConfig.Metrics.MetricsLogInterval,
		partyID:     consenterNodeConfig.PartyId,
		logger:      logger,
		stopChan:    make(chan struct{}),
		promAddress: consenterNodeConfig.Metrics.PrometheusAddress,
		promTLS:     consenterNodeConfig.Metrics.PrometheusTLS,

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

		partyID := fmt.Sprintf("%d", m.partyID)
		reader := monitoring.NewReader(m.promAddress, m.interval, m.promTLS)

		decisions, err := reader.Total(decisionsCountOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read decisions count: %s", err)
		}

		blocks, err := reader.Total(blocksCountOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read blocks count: %s", err)
		}

		bafs, err := reader.Total(bafsCountOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read BAFs count: %s", err)
		}

		complaints, err := reader.Total(complaintsCountOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read complaints count: %s", err)
		}

		txs, err := reader.Total(txsCountOpts, partyID)
		if err != nil {
			m.logger.Warnf("Failed to read transactions count: %s", err)
		}

		m.logger.Infof(
			"CONSENSUS_METRICS party_id=%d: decisions: total=%d, blocks: total=%d, bafs: total=%d, complaints: total=%d, txs: total=%d", m.partyID,
			decisions,
			blocks,
			bafs,
			complaints,
			txs,
		)
	})
}

func (m *ConsensusMetrics) trackMetrics() {
	prevDec, prevBlk := uint64(0), uint64(0)
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	partyID := fmt.Sprintf("%d", m.partyID)
	reader := monitoring.NewReader(m.promAddress, m.interval, m.promTLS)

	for {
		select {
		case <-t.C:
			dec, err := reader.Total(decisionsCountOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read decisions count: %s", err)
				dec = prevDec
			}

			blk, err := reader.Total(blocksCountOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read blocks count: %s", err)
				blk = prevBlk
			}

			bafs, err := reader.Total(bafsCountOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read BAFs count: %s", err)
			}

			complaints, err := reader.Total(complaintsCountOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read complaints count: %s", err)
			}

			txs, err := reader.Total(txsCountOpts, partyID)
			if err != nil {
				m.logger.Warnf("Failed to read transactions count: %s", err)
			}

			m.logger.Infof(
				"CONSENSUS_METRICS party_id=%d: interval=%.2f sec, decisions: interval=%d, rate=%.4f, total=%d, blocks: interval=%d, rate=%.4f, total=%d, bafs: total=%d, complaints: total=%d, txs: total=%d",
				m.partyID,
				sec,
				dec-prevDec,
				float64(dec-prevDec)/sec,
				dec,
				blk-prevBlk,
				float64(blk-prevBlk)/sec,
				blk,
				bafs,
				complaints,
				txs,
			)

			prevDec, prevBlk = dec, blk

		case <-m.stopChan:
			return
		}
	}
}
