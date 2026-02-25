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

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	arma_types "github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/node/config"
	node_ledger "github.com/hyperledger/fabric-x-orderer/node/ledger"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	ledgerMetrics *node_ledger.AssemblerLedgerMetrics
	logger        *flogging.FabricLogger
	interval      time.Duration
	stopChan      chan struct{}
	stopOnce      sync.Once
	startOnce     sync.Once
	monitor       *monitoring.Monitor
	partyID       arma_types.PartyID
}

func NewMetrics(assemblerNodeConfig *config.AssemblerNodeConfig, al *node_ledger.AssemblerLedgerMetrics, logger *flogging.FabricLogger) *Metrics {
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
	al.NewAssemblerLedgerMetrics(p, partyID, logger)

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

		txCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
		blocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
		blocksSizeCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksSize.(prometheus.Counter), m.logger))

		m.logger.Infof("ASSEMBLER_METRICS: party_id=%d, total: TXs=%d, blocks=%d, estimated_block_size=%d", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted)
	})
}

func (m *Metrics) trackMetrics() {
	lastTxCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
	lastBlocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
	sec := m.interval.Seconds()
	t := time.NewTicker(m.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			txCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.TransactionCount.(prometheus.Counter), m.logger))
			blocksCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksCount.(prometheus.Counter), m.logger))
			blocksSizeCommitted := uint64(monitoring.GetMetricValue(m.ledgerMetrics.BlocksSize.(prometheus.Counter), m.logger))

			newBlocks := uint64(0)
			if blocksCommitted > lastBlocksCommitted {
				newBlocks = blocksCommitted - lastBlocksCommitted
			}

			newTXs := uint64(0)
			if txCommitted > lastTxCommitted {
				newTXs = txCommitted - lastTxCommitted
			}

			m.logger.Infof("ASSEMBLER_METRICS: total: party_id=%d, TXs=%d, blocks=%d, estimated_block_size=%d, in the last %.2f seconds: TXs=%d, block=%d", m.partyID, txCommitted, blocksCommitted, blocksSizeCommitted, sec, newTXs, newBlocks)

			lastTxCommitted, lastBlocksCommitted = txCommitted, blocksCommitted
		case <-m.stopChan:
			return
		}
	}
}
