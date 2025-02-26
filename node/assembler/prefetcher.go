package assembler

import (
	"context"
	"sync"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
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
		for _, party := range p.parties {
			// after the first pull, this value will be updated
			p.wg.Add(1)
			go p.handleReplication(ShardPrimary{Shard: shard, Primary: party})
		}
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

func (p *Prefetcher) handleReplication(partition ShardPrimary) {
	defer p.wg.Done()
	batches := p.batchFetcher.Replicate(partition.Shard)
	for {
		select {
		case <-p.cancellationContext.Done():
			p.logger.Infof("Exiting replication for partition %v", partition)
			return
		case batch, ok := <-batches:
			if ok {
				p.logger.Infof("Got batch %s", BatchToString(batch))
				p.prefetchIndex.Put(batch)
			} else {
				p.logger.Infof("Batch Fetcher replication channel was closed, Exiting replication for partition %v", partition)
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
					p.logger.Panicf("error while fetching batch %v", err)
				}
				p.prefetchIndex.PutForce(batch)
			}(batchId)
		}
	}
}
