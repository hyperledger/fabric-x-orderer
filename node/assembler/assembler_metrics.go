/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"fmt"
	"strings"
	"sync"
	"time"

	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
)

type AssemblerMetrics struct {
	assembler *Assembler
	al        node_ledger.AssemblerLedgerReaderWriter
	index     PrefetchIndexer
	shardIds  []arma_types.ShardID
	partyIds  []arma_types.PartyID
	logger    arma_types.Logger
	interval  time.Duration
	stopChan  chan struct{}
	stopOnce  sync.Once
}

func NewAssemblerMetrics(assembler *Assembler, al node_ledger.AssemblerLedgerReaderWriter,
	index PrefetchIndexer, shardIds []arma_types.ShardID, partyIds []arma_types.PartyID, logger arma_types.Logger, interval time.Duration,
) *AssemblerMetrics {
	return &AssemblerMetrics{
		assembler: assembler,
		al:        al,
		index:     index,
		shardIds:  shardIds,
		partyIds:  partyIds,
		interval:  interval,
		logger:    logger,
		stopChan:  make(chan struct{}),
	}
}

func (m *AssemblerMetrics) Start() {
	if m.interval > 0 {
		go m.trackMetrics()
	}
}

func (m *AssemblerMetrics) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopChan)
		txCommitted := m.assembler.GetTxCount()
		blocksCommitted := m.assembler.GetBlocksCount()
		blocksSizeCommitted := m.assembler.GetBlocksSize()

		totalPbSz := 0
		var sb strings.Builder

		for _, shardId := range m.shardIds {
			for _, partyId := range m.partyIds {
				pbsz := m.index.PrefetchBufferSize(ShardPrimary{Shard: shardId, Primary: partyId})
				sb.WriteString(fmt.Sprintf("<Sh: %d, Pr:%d>:%d; ", shardId, partyId, pbsz))
				totalPbSz += pbsz
			}
		}

		m.logger.Infof("ASSEMBLER_METRICS ledger height: %d, total committed transactions: %d, total blocks %d bytes, total size:%d, prefetch buffer size: %d(%s) bytes",
			m.al.LedgerReader().Height(), txCommitted, blocksCommitted, blocksSizeCommitted, totalPbSz, strings.TrimRight(sb.String(), "; "))
	})
}

func (m *AssemblerMetrics) trackMetrics() {
	lastTxCommitted, lastBlocksCommitted := uint64(0), uint64(0)
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			totalPbSz := 0
			var sb strings.Builder

			for _, shardId := range m.shardIds {
				for _, partyId := range m.partyIds {
					pbsz := m.index.PrefetchBufferSize(ShardPrimary{Shard: shardId, Primary: partyId})
					sb.WriteString(fmt.Sprintf("<Sh: %d, Pr:%d>:%d; ", shardId, partyId, pbsz))
					totalPbSz += pbsz
				}
			}

			txCommitted := m.assembler.GetTxCount()
			blocksCommitted := m.assembler.GetBlocksCount()
			blocksSizeCommitted := m.assembler.GetBlocksSize()

			m.logger.Infof("ASSEMBLER_METRICS ledger height: %d, total committed transactions: %d, new transactions in the last %.2f seconds: %d, total blocks %d bytes, new blocks in the last %.2f seconds: %d, total size:%d, prefetch buffer size: %d(%s) bytes",
				m.al.LedgerReader().Height(), txCommitted, sec, (txCommitted - lastTxCommitted), blocksCommitted,
				sec, (blocksCommitted - lastBlocksCommitted), blocksSizeCommitted, totalPbSz, strings.TrimRight(sb.String(), "; "))

			lastTxCommitted, lastBlocksCommitted = txCommitted, blocksCommitted
		case <-m.stopChan:
			return
		}
	}
}
