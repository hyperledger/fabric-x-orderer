/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package ledger

import (
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	transactionCountOpts = metrics.CounterOpts{
		Namespace:  "assembler_ledger",
		Name:       "transaction_count_total",
		Help:       "The total number of transactions committed to the ledger.",
		LabelNames: []string{"party_id"},
	}

	blocksSizeOpts = metrics.CounterOpts{
		Namespace:  "assembler_ledger",
		Name:       "blocks_size_bytes_total",
		Help:       "The total size in bytes of blocks committed to the ledger.",
		LabelNames: []string{"party_id"},
	}

	blocksCountOpts = metrics.CounterOpts{
		Namespace:  "assembler_ledger",
		Name:       "blocks_count_total",
		Help:       "The total number of blocks committed to the ledger.",
		LabelNames: []string{"party_id"},
	}
)

type metricCounters struct {
	transactionCount metrics.Counter
	blocksSize       metrics.Counter
	blocksCount      metrics.Counter
}

type AssemblerLedgerMetrics struct {
	metricCounters   *metricCounters
	TransactionCount uint64
	BlocksSize       uint64
	BlocksCount      uint64
	logger           types.Logger
}

func (al *AssemblerLedgerMetrics) Init(p *monitoring.Provider, partyID string, logger types.Logger) {
	al.metricCounters = &metricCounters{
		transactionCount: p.NewCounter(transactionCountOpts).With([]string{partyID}...),
		blocksSize:       p.NewCounter(blocksSizeOpts).With([]string{partyID}...),
		blocksCount:      p.NewCounter(blocksCountOpts).With([]string{partyID}...),
	}

	al.logger = logger

	al.update()
}

func (al *AssemblerLedgerMetrics) update() {
	if al.metricCounters == nil {
		return
	}
	txCommitted := uint64(monitoring.GetMetricValue(al.metricCounters.transactionCount.(prometheus.Metric), al.logger))
	blocksCommitted := uint64(monitoring.GetMetricValue(al.metricCounters.blocksCount.(prometheus.Metric), al.logger))
	blocksSizeCommitted := uint64(monitoring.GetMetricValue(al.metricCounters.blocksSize.(prometheus.Metric), al.logger))

	al.metricCounters.transactionCount.Add(float64(al.TransactionCount - txCommitted))
	al.metricCounters.blocksSize.Add(float64(al.BlocksSize - blocksSizeCommitted))
	al.metricCounters.blocksCount.Add(float64(al.BlocksCount - blocksCommitted))
}
