/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
)

type Metrics struct {
	ledgerMetrics *node_ledger.AssemblerLedgerMetrics
	logger        arma_types.Logger
	interval      time.Duration
	stopChan      chan struct{}
	stopOnce      sync.Once
	startOnce     sync.Once
	monitor       *monitoring.Monitor
	partyID       arma_types.PartyID
}

func NewMetrics(assemblerNodeConfig *config.AssemblerNodeConfig, al *node_ledger.AssemblerLedgerMetrics, logger arma_types.Logger) *Metrics {
	host, port, err := net.SplitHostPort(assemblerNodeConfig.MonitoringListenAddress)
	if err != nil {
		logger.Panicf("failed to get hostname: %v", err)
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		logger.Panicf("failed to convert port to int: %v", err)
	}
	partyID := fmt.Sprintf("%d", assemblerNodeConfig.PartyId)

	monitor := monitoring.NewMonitor(monitoring.Endpoint{Host: host, Port: portInt}, fmt.Sprintf("assembler_%s", partyID))
	p := monitor.Provider
	al.Init(p, partyID, logger)

	return &Metrics{
		ledgerMetrics: al,
		interval:      assemblerNodeConfig.MetricsLogInterval,
		logger:        logger,
		stopChan:      make(chan struct{}),
		monitor:       monitor,
		partyID:       assemblerNodeConfig.PartyId,
	}
}

func (m *Metrics) Start() {
	m.startOnce.Do(func() {
		m.monitor.Start()
		if m.interval > 0 {
			go m.trackMetrics()
		}
	})
}

func (m *Metrics) Stop() {
	m.stopOnce.Do(func() {
		m.logger.Infof("Reporting routine is stopping")
		if m.monitor != nil {
			m.monitor.Stop()
			m.monitor = nil
		}
		close(m.stopChan)

		m.logger.Infof("ASSEMBLER_METRICS: party_id=%d, total: TXs=%d, blocks=%d, block_size=%d", m.partyID, m.ledgerMetrics.TransactionCount, m.ledgerMetrics.BlocksCount, m.ledgerMetrics.BlocksSize)
	})
}

func (m *Metrics) trackMetrics() {
	lastTxCommitted, lastBlocksCommitted := m.ledgerMetrics.TransactionCount, m.ledgerMetrics.BlocksCount
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			txCommitted, blocksCommitted, blocksSizeCommitted := m.ledgerMetrics.TransactionCount, m.ledgerMetrics.BlocksCount, m.ledgerMetrics.BlocksSize

			newBlocks := uint64(0)
			if blocksCommitted > lastBlocksCommitted {
				newBlocks = blocksCommitted - lastBlocksCommitted
			}

			newTXs := uint64(0)
			if txCommitted > lastTxCommitted {
				newTXs = txCommitted - lastTxCommitted
			}

			m.logger.Infof("ASSEMBLER_METRICS: total: party_id=%d, TXs=%d, blocks=%d, block=size %d, in the last %.2f seconds: TXs=%d, block=%d", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted, sec, newTXs, newBlocks)

			lastTxCommitted, lastBlocksCommitted = txCommitted, blocksCommitted
		case <-m.stopChan:
			return
		}
	}
}
