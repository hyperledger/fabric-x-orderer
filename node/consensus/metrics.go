/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"github.com/hyperledger/fabric-x-orderer/node/ledger"
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
)

type ConsensusMetrics struct {
	partyID   arma_types.PartyID
	logger    arma_types.Logger
	interval  time.Duration
	stopChan  chan struct{}
	stopOnce  sync.Once
	startOnce sync.Once
	monitor   *monitoring.Monitor

	// metrics
	decisionsCount  metrics.Counter
	blocksCount     metrics.Counter
	bafsCount       metrics.Counter
	complaintsCount metrics.Counter
}

func NewConsensusMetrics(consenterNodeConfig *config.ConsenterNodeConfig, logger arma_types.Logger) *ConsensusMetrics {
	host, port, err := net.SplitHostPort(consenterNodeConfig.MonitoringListenAddress)
	if err != nil {
		logger.Panicf("failed to get hostname: %v", err)
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		logger.Panicf("failed to convert port to int: %v", err)
	}
	partyID := fmt.Sprintf("%d", consenterNodeConfig.PartyId)

	monitor := monitoring.NewMonitor(monitoring.Endpoint{Host: host, Port: portInt}, fmt.Sprintf("consensus_%s", partyID))
	p := monitor.Provider

	return &ConsensusMetrics{
		interval: consenterNodeConfig.MetricsLogInterval,
		partyID:  consenterNodeConfig.PartyId,
		logger:   logger,
		stopChan: make(chan struct{}),
		monitor:  monitor,

		decisionsCount:  p.NewCounter(metrics.CounterOpts(decisionsCountOpts)).With([]string{partyID}...),
		blocksCount:     p.NewCounter(metrics.CounterOpts(blocksCountOpts)).With([]string{partyID}...),
		bafsCount:       p.NewCounter(metrics.CounterOpts(bafsCountOpts)).With([]string{partyID}...),
		complaintsCount: p.NewCounter(metrics.CounterOpts(complaintsCountOpts)).With([]string{partyID}...),
	}
}

func (m *ConsensusMetrics) Start() {
	m.startOnce.Do(func() {
		m.monitor.Start()
		if m.interval > 0 {
			go m.trackMetrics()
		}
	})
}

func (m *ConsensusMetrics) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.logger.Infof("Reporting routine is stopping")
		m.monitor.Stop()
		m.logger.Infof("CONSENSUS_METRICS party_id=%d: decisions: total=%d, blocks: total=%d, bafs: total=%d, complaints: total=%d", m.partyID,
			uint64(monitoring.GetMetricValue(m.decisionsCount.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.blocksCount.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.bafsCount.(prometheus.Metric), m.logger)),
			uint64(monitoring.GetMetricValue(m.complaintsCount.(prometheus.Metric), m.logger)))
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

func (m *ConsensusMetrics) initMetricsFromLedger(l *ledger.ConsensusLedger) {
	h := l.Height()
	var blocks uint64
	for i := uint64(0); i < h; i++ {
		b, err := l.RetrieveBlockByNumber(i)
		if err != nil {
			m.logger.Warnf("failed retrieving block %d: %v", i, err)
			return
		}

		p, _, err := state.BytesToDecision(b.Data.Data[0])
		if err != nil {
			m.logger.Warnf("failed parsing decision %d: %v", i, err)
			return
		}

		var hdr state.Header
		if err := hdr.Deserialize(p.Header); err != nil {
			m.logger.Warnf("failed deserializing header %d: %v", i, err)
			return
		}

		blocks += uint64(len(hdr.AvailableCommonBlocks))
	}

	m.decisionsCount.Add(float64(h))
	m.blocksCount.Add(float64(blocks))
}
