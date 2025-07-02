/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package prefetch_benchmark_test

import (
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
)

func generateDecisions(logger types.Logger, batchesChan <-chan types.BatchID, decisionsPerSecond float64, maxBatchesPerDecision int, monitor *statsMonitor) <-chan []types.BatchID {
	decisionInterval := time.Second / time.Duration(decisionsPerSecond)
	decisionsChan := make(chan []types.BatchID, int(PREFETCH_STRESS_TEST_MEMORY_FACTOR*decisionsPerSecond))
	batches := []types.BatchID{}

	counter := 0
	go func() {
		logger.Debugf("Starting goroutine to generate decisions")
		defer func() {
			logger.Debugf("Exiting goroutine to generate decisions")
		}()
		ticker := time.NewTicker(decisionInterval)
		logger.Debugf("Created decisions ticker with interval %v", decisionInterval)

		createAndSendDecision := func() {
			decision, remainingBatches := selectRandomBatches(batches, maxBatchesPerDecision)
			pre := len(batches)
			counter += len(decision)
			logger.Debugf("Creating a decision of size %d...", len(decision))
			batches = remainingBatches
			if len(decision) > 0 {
				logger.Debugf("Sent decision of size %d  (created: %d, pre: %d, post: %d)", len(decision), counter, pre, len(batches))
				decisionsChan <- decision
				monitor.EventsChan <- &PrefetchBenchEvent{Event: EventDecisionGenerated, Data: decision}
			}
		}

		for {
			select {
			case batch, ok := <-batchesChan:
				if !ok {
					logger.Debugf("Batcher to consensus channel closed, making decisions for all the left batches")
					ticker.Stop()
					for len(batches) > 0 {
						logger.Debugf("Batcher to consensus channel closed, creating a decision, left batches = %d", len(batches))
						createAndSendDecision()
						logger.Debugf("Batcher to consensus channel closed, sleeping for %v after created a decision, left batches = %d", decisionInterval, len(batches))
						time.Sleep(decisionInterval)
					}
					logger.Debugf("Closing decision chan")
					close(decisionsChan)
					return
				}
				batches = append(batches, batch)
				logger.Debugf("Consensus received a batch and has appended it, now it has %d batches", len(batches))
			case <-ticker.C:
				createAndSendDecision()
			}
		}
	}()
	return decisionsChan
}
