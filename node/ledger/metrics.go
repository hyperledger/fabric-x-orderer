/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package ledger

import (
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
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
		Help:       "The estimated total size in bytes of blocks committed to the ledger.",
		LabelNames: []string{"party_id"},
	}

	blocksCountOpts = metrics.CounterOpts{
		Namespace:  "assembler_ledger",
		Name:       "blocks_count_total",
		Help:       "The total number of blocks committed to the ledger.",
		LabelNames: []string{"party_id"},
	}
)

type AssemblerLedgerMetrics struct {
	TransactionCount metrics.Counter
	BlocksSize       metrics.Counter
	BlocksCount      metrics.Counter
	logger           *flogging.FabricLogger
}

func (al *AssemblerLedgerMetrics) NewAssemblerLedgerMetrics(p *monitoring.Provider, partyID string, logger *flogging.FabricLogger) {
	al.TransactionCount = p.NewCounter(transactionCountOpts).With([]string{partyID}...)
	al.BlocksSize = p.NewCounter(blocksSizeOpts).With([]string{partyID}...)
	al.BlocksCount = p.NewCounter(blocksCountOpts).With([]string{partyID}...)

	al.logger = logger
}
