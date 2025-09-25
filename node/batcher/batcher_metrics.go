/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package batcher

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
)

type BatcherMetrics struct {
	// state
	currentRole atomic.Uint32 // 1=primary, 2=secondary

	// counters
	roleChangesTotal    atomic.Uint64
	batchesCreatedTotal atomic.Uint64
	batchesPulledTotal  atomic.Uint64
	batchedTxsTotal     atomic.Uint64
	routerTxsTotal      atomic.Uint64
	complaintsTotal     atomic.Uint64

	// gauges
	memPoolSize atomic.Int64
}

func (m *BatcherMetrics) role() string {
	if m.currentRole.Load() == 1 {
		return "primary"
	}
	return "secondary"
}

func (m *BatcherMetrics) trackMetrics(ctx context.Context, logger types.Logger, id types.PartyID, shard types.ShardID, interval time.Duration) {
	firstProbe := true
	prevC := uint64(0)
	prevP := uint64(0)
	sec := interval.Seconds()

	for {
		created := m.batchesCreatedTotal.Load()
		pulled := m.batchesPulledTotal.Load()

		if !firstProbe {
			logger.Infof("batcher metrics: interval_time=%.2fs, party_id=%d, shard_id=%d, role=%s, batches_created_total=%d, batches_created_rate_per_sec=%.2f, batches_pulled_total=%d, batches_pulled_rate_per_sec=%.2f, txs_total=%d, mempool_size=%d, router_txs_total=%d, role_changes_total=%d, complaints_total=%d",
				sec, id, int(shard), m.role(), created, float64(created-prevC)/sec, pulled, float64(pulled-prevP)/sec, m.batchedTxsTotal.Load(), m.memPoolSize.Load(), m.routerTxsTotal.Load(), m.roleChangesTotal.Load(), m.complaintsTotal.Load(),
			)
		}

		prevC, prevP = created, pulled
		firstProbe = false

		select {
		case <-time.After(interval):
		case <-ctx.Done():
			logger.Infof("batcher metrics: party_id=%d, shard_id=%d, role=%s, batches_created_total=%d, batches_pulled_total=%d, txs_total=%d, mempool_size=%d, router_txs_total=%d, role_changes_total=%d, complaints_total=%d",
				id, int(shard), m.role(), m.batchesCreatedTotal.Load(), m.batchesPulledTotal.Load(), m.batchedTxsTotal.Load(), m.memPoolSize.Load(), m.routerTxsTotal.Load(), m.roleChangesTotal.Load(), m.complaintsTotal.Load(),
			)
			return
		}
	}
}
