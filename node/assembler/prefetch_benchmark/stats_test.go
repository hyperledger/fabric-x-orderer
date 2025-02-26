package prefetch_benchmark_test

import (
	"sync"
	"time"

	"github.ibm.com/decentralized-trust-research/arma/common/types"
)

type PrefetchBenchEventType int

const (
	EventBatchGenerated PrefetchBenchEventType = iota
	EventBatchDigested
	EventDecisionGenerated
	EventDecisionDigested
	EventPrefetchIndexMissed
	EventBatchDeliveredToAssembler
	EventBatchDeliveredToConsensus
)

func getBenchEventName(eventType PrefetchBenchEventType) string {
	switch eventType {
	case EventBatchGenerated:
		return "EventBatchGenerated"
	case EventBatchDigested:
		return "EventBatchDigested"
	case EventDecisionGenerated:
		return "EventDecisionGenerated"
	case EventDecisionDigested:
		return "EventDecisionDigested"
	case EventPrefetchIndexMissed:
		return "EventPrefetchIndexMissed"
	case EventBatchDeliveredToAssembler:
		return "EventBatchDeliveredToAssembler"
	case EventBatchDeliveredToConsensus:
		return "EventBatchDeliveredToConsensus"
	default:
		panic("unrecognized event type")
	}
}

type PrefetchBenchEvent struct {
	Event PrefetchBenchEventType
	Data  interface{}
}

type testStats struct {
	lock         sync.Mutex
	eventToCount map[PrefetchBenchEventType]int
}

func NewTestStats() *testStats {
	return &testStats{
		eventToCount: make(map[PrefetchBenchEventType]int),
	}
}

func (ts *testStats) RecordEvent(event PrefetchBenchEventType) {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	ts.addEventUnsafe(event, 1)
}

func (ts *testStats) GetEventCount(event PrefetchBenchEventType) int {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	if _, ok := ts.eventToCount[event]; !ok {
		return 0
	}
	return ts.eventToCount[event]
}

func (ts *testStats) addEventUnsafe(event PrefetchBenchEventType, count int) {
	if _, ok := ts.eventToCount[event]; !ok {
		ts.eventToCount[event] = 0
	}
	ts.eventToCount[event] += count
}

func (ts *testStats) Accumulate(other *testStats) {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	other.lock.Lock()
	defer other.lock.Unlock()
	for event, count := range other.eventToCount {
		ts.addEventUnsafe(event, count)
	}
}

type statsMonitor struct {
	wg               sync.WaitGroup
	logger           types.Logger
	EventsChan       chan *PrefetchBenchEvent
	accumulatedStats *testStats
	currentTickStats *testStats
	historyStats     []*testStats
	statsTicker      *time.Ticker
	statsInterval    time.Duration
	doneChan         chan struct{}
}

func newStatsMonitor(logger types.Logger, eventChanSize int, statsInterval time.Duration) *statsMonitor {
	return &statsMonitor{
		logger:           logger,
		EventsChan:       make(chan *PrefetchBenchEvent, eventChanSize),
		accumulatedStats: NewTestStats(),
		currentTickStats: NewTestStats(),
		historyStats:     []*testStats{},
		statsInterval:    statsInterval,
	}
}

func (sm *statsMonitor) snapshotStats() {
	sm.accumulatedStats.Accumulate(sm.currentTickStats)
	sm.historyStats = append(sm.historyStats, sm.currentTickStats)
	sm.currentTickStats = NewTestStats()
}

func (sm *statsMonitor) GetAccumulatedEventCount(event PrefetchBenchEventType) int {
	return sm.accumulatedStats.GetEventCount(event)
}

func (sm *statsMonitor) Start() {
	sm.logger.Debugf("Starting stats monitor")
	sm.doneChan = make(chan struct{})
	sm.statsTicker = time.NewTicker(sm.statsInterval)
	sm.wg.Add(1)
	go func() {
		sm.logger.Debugf("Starting goroutine to monitor stats")
		defer func() {
			sm.logger.Debugf("Exiting goroutine to monitor stats")
			sm.wg.Done()
		}()
		for {
			select {
			case <-sm.statsTicker.C:
				sm.logger.Debugf("Snapshot tick - taking stats monitor")
				sm.snapshotStats()
			case event, ok := <-sm.EventsChan:
				if ok {
					sm.logger.Debugf("Event %s arrived, will record it", getBenchEventName(event.Event))
					sm.currentTickStats.RecordEvent(event.Event)
				}
			case <-sm.doneChan:
				sm.logger.Debugf("Stats monitor is closing, recording remaining events")
				sm.snapshotStats()
				return
			}
		}
	}()
}

func (sm *statsMonitor) Stop() {
	sm.logger.Debugf("Stats monitor Stop")
	close(sm.doneChan)
	close(sm.EventsChan)
	sm.statsTicker.Stop()
	sm.wg.Wait()
}
