/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"context"
	"errors"
	"sync"

	"github.com/hyperledger/fabric-x-orderer/common/types"
)

//go:generate counterfeiter -o ./mocks/prefetcher_controller.go . PrefetcherController
type PrefetcherController interface {
	Start()
	Stop()
}

//go:generate counterfeiter -o ./mocks/prefetcher_factory.go . PrefetcherFactory
type PrefetcherFactory interface {
	Create(shards []types.ShardID, parties []types.PartyID, prefetchIndex PrefetchIndexer, batchFetcher BatchBringer, logger types.Logger) PrefetcherController
}

type DefaultPrefetcherFactory struct{}

func (f *DefaultPrefetcherFactory) Create(shards []types.ShardID, parties []types.PartyID, prefetchIndex PrefetchIndexer, batchFetcher BatchBringer, logger types.Logger) PrefetcherController {
	return NewPrefetcher(shards, parties, prefetchIndex, batchFetcher, logger)
}

type Prefetcher struct {
	logger              types.Logger
	prefetchIndex       PrefetchIndexer
	batchFetcher        BatchBringer
	shards              []types.ShardID
	parties             []types.PartyID
	cancellationContext context.Context
	cancelContextFunc   context.CancelFunc
	// in order to wait for all the go routines to finish
	wg sync.WaitGroup
}

func NewPrefetcher(
	shards []types.ShardID,
	parties []types.PartyID,
	prefetchIndex PrefetchIndexer,
	batchFetcher BatchBringer,
	logger types.Logger,
) *Prefetcher {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Prefetcher{
		logger:              logger,
		prefetchIndex:       prefetchIndex,
		batchFetcher:        batchFetcher,
		shards:              shards,
		parties:             parties,
		cancellationContext: ctx,
		cancelContextFunc:   cancel,
	}
	return p
}

// Starts the prefetcher.
func (p *Prefetcher) Start() {
	for _, shard := range p.shards {
		// This starts the batch fetcher to start pulling from a shard.
		p.wg.Add(1)
		go p.handleReplication(shard)
	}
	p.wg.Add(1)
	go p.handleBatchRequests()
}

// Stops the prefetcher.
func (p *Prefetcher) Stop() {
	p.cancelContextFunc()
	p.wg.Wait()
	p.batchFetcher.Stop()
}

func (p *Prefetcher) handleReplication(shard types.ShardID) {
	defer p.wg.Done()
	batches := p.batchFetcher.Replicate(shard)
	for {
		select {
		case <-p.cancellationContext.Done():
			p.logger.Infof("Prefetcher stopped handling replication from shard %d", shard)
			return
		case batch, ok := <-batches:
			if ok {
				p.logger.Debugf("Got batch %s", BatchToString(batch))
				err := p.prefetchIndex.Put(batch)
				if err != nil {
					p.logger.Errorf("Failed to put batch from shard %d, error: %v", shard, err)
				}
			} else {
				p.logger.Infof("Batch Fetcher replication channel was closed, Prefetcher stopped handling replication from shard %d", shard)
				return
			}
		}
	}
}

func (p *Prefetcher) handleBatchRequests() {
	defer p.wg.Done()
	for {
		select {
		case <-p.cancellationContext.Done():
			p.logger.Infof("Exiting handleBatchRequests")
			return
		case batchId := <-p.prefetchIndex.Requests():
			go func(batchId types.BatchID) {
				batch, err := p.batchFetcher.GetBatch(batchId)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						p.logger.Infof("fetch canceled")
						return
					}

					p.logger.Panicf("error while fetching batch: %v", err)
				}
				err = p.prefetchIndex.PutForce(batch)
				if err != nil {
					p.logger.Errorf("Failed to put-force batch, error: %v", err)
				}
			}(batchId)
		}
	}
}
