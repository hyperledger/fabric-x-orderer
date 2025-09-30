/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
)

type PartitionPrefetchIndexMetrics struct {
	cacheSize          *uint64
	forcedPutCacheSize *uint64
}

type Metrics struct {
	ledgerMetrics *node_ledger.AssemblerLedgerMetrics
	indexMetrics  *PrefetchIndexMetrics
	logger        arma_types.Logger
	interval      time.Duration
	stopChan      chan struct{}
	stopOnce      sync.Once
}

func NewMetrics(alm *node_ledger.AssemblerLedgerMetrics, pim *PrefetchIndexMetrics, logger arma_types.Logger, interval time.Duration,
) *Metrics {
	return &Metrics{
		ledgerMetrics: alm,
		indexMetrics:  pim,
		interval:      interval,
		logger:        logger,
		stopChan:      make(chan struct{}),
	}
}

func (m *Metrics) Start() {
	if m.interval > 0 {
		go m.trackMetrics()
	}
}

func (m *Metrics) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		txCommitted := atomic.LoadUint64(&m.ledgerMetrics.TransactionCount)
		blocksCommitted := atomic.LoadUint64(&m.ledgerMetrics.BlocksCount)
		blocksSizeCommitted := atomic.LoadUint64(&m.ledgerMetrics.BlocksSize)

		totalPbSz := 0
		var sb strings.Builder

		for partition, pim := range *m.indexMetrics {
			pbsz := atomic.LoadUint64(pim.cacheSize) + atomic.LoadUint64(pim.forcedPutCacheSize)
			sb.WriteString(fmt.Sprintf("<Sh: %d, Pr:%d>:%d; ", partition.Shard, partition.Primary, pbsz))
			totalPbSz += int(pbsz)
		}

		m.logger.Infof("ASSEMBLER_METRICS: total: TXs %d, blocks %d, block size %d, prefetch buffer size: %d(%s) bytes",
			txCommitted, blocksCommitted, blocksSizeCommitted, totalPbSz, strings.TrimRight(sb.String(), "; "))
	})
}

func (m *Metrics) trackMetrics() {
	lastTxCommitted, lastBlocksCommitted := atomic.LoadUint64(&m.ledgerMetrics.TransactionCount), atomic.LoadUint64(&m.ledgerMetrics.BlocksCount)
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			totalPbSz := 0
			var sb strings.Builder

			for partition, pim := range *m.indexMetrics {
				pbsz := atomic.LoadUint64(pim.cacheSize) + atomic.LoadUint64(pim.forcedPutCacheSize)
				sb.WriteString(fmt.Sprintf("<Sh: %d, Pr:%d>:%d; ", partition.Shard, partition.Primary, pbsz))
				totalPbSz += int(pbsz)
			}

			txCommitted := atomic.LoadUint64(&m.ledgerMetrics.TransactionCount)
			blocksCommitted := atomic.LoadUint64(&m.ledgerMetrics.BlocksCount)
			blocksSizeCommitted := atomic.LoadUint64(&m.ledgerMetrics.BlocksSize)

			newBlocks := uint64(0)
			if blocksCommitted > lastBlocksCommitted {
				newBlocks = blocksCommitted - lastBlocksCommitted
			}

			newTXs := uint64(0)
			if txCommitted > lastTxCommitted {
				newTXs = txCommitted - lastTxCommitted
			}

			m.logger.Infof("ASSEMBLER_METRICS: total: TXs %d, blocks %d, block size %d, in the last %.2f seconds: TXs %d, block %d, prefetch buffer size: %d(%s) bytes",
				txCommitted, blocksCommitted, blocksSizeCommitted, sec, newTXs, newBlocks, totalPbSz, strings.TrimRight(sb.String(), "; "))

			lastTxCommitted, lastBlocksCommitted = txCommitted, blocksCommitted
		case <-m.stopChan:
			return
		}
	}
}
