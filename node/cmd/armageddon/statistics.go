package armageddon

import (
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
)

// Statistics holds aggregated data related to transactions and blocks per second
type Statistics struct {
	timeStamp     float64 // the time passed since the experiment started in seconds
	numOfTxs      int
	numOfBlocks   int
	sumOfTxsDelay float64 // the cumulative sum of transaction delays in seconds
	sumOfTxsSize  int
}

type StatisticsAggregator struct {
	mu        sync.Mutex
	startTime int64
	statistic Statistics
}

func (sta *StatisticsAggregator) Add(numOfTxs int, numOfBlocks int, sumOfTxsDelay float64, sumOfTxsSize int) {
	sta.mu.Lock()
	defer sta.mu.Unlock()
	sta.statistic.numOfTxs += numOfTxs
	sta.statistic.numOfBlocks += numOfBlocks
	sta.statistic.sumOfTxsDelay += sumOfTxsDelay
	sta.statistic.sumOfTxsSize += sumOfTxsSize
}

func (sta *StatisticsAggregator) ReadAndReset() Statistics {
	sta.mu.Lock()
	defer sta.mu.Unlock()
	currentTime := time.Now().UnixMilli()
	timeSinceStartMs := currentTime - sta.startTime
	timeSinceStartS := float64(timeSinceStartMs) / 1000
	val := Statistics{
		timeStamp:     timeSinceStartS,
		numOfTxs:      sta.statistic.numOfTxs,
		numOfBlocks:   sta.statistic.numOfBlocks,
		sumOfTxsDelay: sta.statistic.sumOfTxsDelay,
		sumOfTxsSize:  sta.statistic.sumOfTxsSize,
	}
	sta.statistic.numOfTxs = 0
	sta.statistic.numOfBlocks = 0
	sta.statistic.sumOfTxsDelay = 0.0
	sta.statistic.sumOfTxsSize = 0
	return val
}

// BlockWithTime holds the block and the time the block was pulled from the assembler
type BlockWithTime struct {
	block        *common.Block
	acceptedTime time.Time
}
