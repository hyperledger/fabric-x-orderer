/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"sync"
	"sync/atomic"
	"time"

	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
)

type ConsensusMetrics struct {
	partyID  arma_types.PartyID
	logger   arma_types.Logger
	interval time.Duration
	stopChan chan struct{}
	stopOnce sync.Once

	// metrics
	decisionsCount  atomic.Uint64
	blocksCount     atomic.Uint64
	bafsCount       atomic.Uint64
	complaintsCount atomic.Uint64
}

func NewConsensusMetrics(partyId arma_types.PartyID, logger arma_types.Logger, interval time.Duration) *ConsensusMetrics {
	return &ConsensusMetrics{
		interval: interval,
		partyID:  partyId,
		logger:   logger,
		stopChan: make(chan struct{}),
	}
}

func (m *ConsensusMetrics) Start() {
	if m.interval > 0 {
		go m.trackMetrics()
	}
}

func (m *ConsensusMetrics) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.logger.Infof("CONSENSUS_METRICS party_id=%d, decisions_total=%d, blocks_total=%d, bafs_total=%d, complaints_total=%d", m.partyID, m.decisionsCount.Load(), m.blocksCount.Load(), m.bafsCount.Load(), m.complaintsCount.Load())
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
			dec := m.decisionsCount.Load()
			blk := m.blocksCount.Load()

			m.logger.Infof("CONSENSUS_METRICS party_id=%d, interval_s=%.2f, decisions_interval=%d, decisions_rate=%.4f, decisions_total=%d, blocks_interval=%d, blocks_rate=%.4f, blocks_total=%d, bafs_total=%d, complaints_total=%d",
				m.partyID,
				sec,
				dec-prevDec, float64(dec-prevDec)/sec,
				dec,
				blk-prevBlk, float64(blk-prevBlk)/sec,
				blk,
				m.bafsCount.Load(),
				m.complaintsCount.Load(),
			)

			prevDec, prevBlk = dec, blk

		case <-m.stopChan:
			return
		}
	}
}
