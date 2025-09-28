/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consensus

import (
	"context"
	"sync/atomic"
	"time"

	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/consensus/state"
	"google.golang.org/protobuf/proto"
)

type ConsensusMetrics struct {
	partyID  arma_types.PartyID
	logger   arma_types.Logger
	enabled  bool
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc

	// metrics
	decisionsCount  atomic.Uint64
	blocksCount     atomic.Uint64
	blocksSize      atomic.Uint64
	decisionsSize   atomic.Uint64
	bafsCount       atomic.Uint64
	complaintsCount atomic.Uint64
}

func NewConsensusMetrics(partyId arma_types.PartyID, logger arma_types.Logger, interval time.Duration, enabled bool) *ConsensusMetrics {
	return &ConsensusMetrics{
		enabled:  enabled,
		interval: interval,
		partyID:  partyId,
		logger:   logger,
	}
}

func (m *ConsensusMetrics) Start() {
	if !m.enabled {
		return
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	go m.trackMetrics()
}

func (m *ConsensusMetrics) Stop() {
	if m.cancel != nil {
		m.cancel()
	}

	m.logger.Infof("CONSENSUS_METRICS party_id=%d, decisions_total=%d, blocks_total=%d, decisions_bytes_total=%d, blocks_bytes_total=%d, bafs_total=%d, complaints_total=%d",
		m.partyID,
		m.decisionsCount.Load(),
		m.blocksCount.Load(),
		m.decisionsSize.Load(),
		m.blocksSize.Load(),
		m.bafsCount.Load(),
		m.complaintsCount.Load(),
	)
}

func (m *ConsensusMetrics) AddBlocksMetrics(hdr *state.Header) {
	m.blocksCount.Add(uint64(len(hdr.AvailableCommonBlocks)))
	var total uint64
	for _, b := range hdr.AvailableCommonBlocks {
		total += uint64(proto.Size(b))
	}
	m.blocksSize.Add(total)
}

func (m *ConsensusMetrics) trackMetrics() {
	first := true
	prevDec, prevBlk := uint64(0), uint64(0)
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			dec := m.decisionsCount.Load()
			blk := m.blocksCount.Load()

			if !first {
				di := dec - prevDec
				bi := blk - prevBlk

				m.logger.Infof("CONSENSUS_METRICS party_id=%d, interval_s=%.2f, decisions_interval=%d, decisions_rate=%.4f, decisions_total=%d, blocks_interval=%d, blocks_rate=%.4f, blocks_total=%d, decisions_bytes_total=%d, blocks_bytes_total=%d, bafs_total=%d, complaints_total=%d",
					m.partyID,
					sec,
					di, float64(di)/sec,
					dec,
					bi, float64(bi)/sec,
					blk,
					m.decisionsSize.Load(),
					m.blocksSize.Load(),
					m.bafsCount.Load(),
					m.complaintsCount.Load(),
				)
			}
			first = false
			prevDec, prevBlk = dec, blk

		case <-m.ctx.Done():
			return
		}
	}
}
