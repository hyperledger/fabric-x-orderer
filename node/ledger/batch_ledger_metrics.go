/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
)

var (
	hashingLatencyOpts = metrics.HistogramOpts{
		Namespace:  "batch_ledger",
		Name:       "hashing_latency_seconds",
		Help:       "The latency to compute the batch digest and block header hash.",
		LabelNames: []string{"party_id", "shard_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}

	appendLatencyOpts = metrics.HistogramOpts{
		Namespace:  "batch_ledger",
		Name:       "append_latency_seconds",
		Help:       "The latency to append a batch to the ledger.",
		LabelNames: []string{"party_id", "shard_id"},
		Buckets:    []float64{.0001, .001, .002, .003, .004, .005, .01, .03, .05, .1, .3, .5, 1}, // TODO: adjust buckets after reviewing Grafana
	}
)

type BatchLedgerMetrics struct {
	HashingLatency metrics.Histogram
	AppendLatency  metrics.Histogram
	logger         *flogging.FabricLogger
}

func (bl *BatchLedgerMetrics) NewBatchLedgerMetrics(p metrics.Provider, partyID, shardID string, logger *flogging.FabricLogger) {
	bl.HashingLatency = p.NewHistogram(hashingLatencyOpts).With([]string{partyID, shardID}...)
	bl.AppendLatency = p.NewHistogram(appendLatencyOpts).With([]string{partyID, shardID}...)
	bl.logger = logger
}
