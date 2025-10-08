/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"sync"
	"sync/atomic"
	"time"

	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
)

type BatcherMetrics struct {
	partyID  arma_types.PartyID
	shardID  arma_types.ShardID
	logger   arma_types.Logger
	interval time.Duration
	stopChan chan struct{}
	stopOnce sync.Once

	// metrics
	currentRole         atomic.Uint32 // 1=primary, 2=secondary
	roleChangesTotal    atomic.Uint64
	batchesCreatedTotal atomic.Uint64
	batchesPulledTotal  atomic.Uint64
	batchedTxsTotal     atomic.Uint64
	routerTxsTotal      atomic.Uint64
	complaintsTotal     atomic.Uint64
	memPoolSize         atomic.Int64
	firstResendsTotal   atomic.Uint64
}

func NewBatcherMetrics(partyId arma_types.PartyID, shardId arma_types.ShardID, logger arma_types.Logger, interval time.Duration) *BatcherMetrics {
	return &BatcherMetrics{
		interval: interval,
		partyID:  partyId,
		shardID:  shardId,
		logger:   logger,
		stopChan: make(chan struct{}),
	}
}

func (m *BatcherMetrics) Start() {
	if m.interval > 0 {
		go m.trackMetrics()
	}
}

func (m *BatcherMetrics) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		m.logger.Infof("BATCHER_METRICS party_id=%d, shard_id=%d, role=%s, batches_created_total=%d, batches_pulled_total=%d, first_resends_total=%d, txs_total=%d, mempool_size=%d, router_txs_total=%d, role_changes_total=%d, complaints_total=%d",
			m.partyID,
			m.shardID,
			m.role(),
			m.batchesCreatedTotal.Load(),
			m.batchesPulledTotal.Load(),
			m.firstResendsTotal.Load(),
			m.batchedTxsTotal.Load(),
			m.memPoolSize.Load(),
			m.routerTxsTotal.Load(),
			m.roleChangesTotal.Load(),
			m.complaintsTotal.Load(),
		)
	})
}

func (m *BatcherMetrics) trackMetrics() {
	prevC, prevP, prevR := uint64(0), uint64(0), uint64(0)
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			created := m.batchesCreatedTotal.Load()
			pulled := m.batchesPulledTotal.Load()
			resends := m.firstResendsTotal.Load()

			m.logger.Infof("BATCHER_METRICS party_id=%d, shard_id=%d, role=%s, interval_s=%.2f, batches_created_interval=%d, batches_created_rate=%.4f, batches_created_total=%d, batches_pulled_interval=%d, batches_pulled_rate=%.4f, batches_pulled_total=%d, first_resends_interval=%d, first_resend_rate=%.4f, first_resends_total=%d, txs_total=%d, mempool_size=%d, router_txs_total=%d, role_changes_total=%d, complaints_total=%d",
				m.partyID,
				m.shardID,
				m.role(),
				sec,
				created-prevC, float64(created-prevC)/sec, created,
				pulled-prevP, float64(pulled-prevP)/sec, pulled,
				resends-prevR, float64(resends-prevR)/sec, resends,
				m.batchedTxsTotal.Load(),
				m.memPoolSize.Load(),
				m.routerTxsTotal.Load(),
				m.roleChangesTotal.Load(),
				m.complaintsTotal.Load(),
			)
			prevC, prevP, prevR = created, pulled, resends

		case <-m.stopChan:
			return
		}
	}
}

func (m *BatcherMetrics) role() string {
	if m.currentRole.Load() == 1 {
		return "primary"
	}
	return "secondary"
}
