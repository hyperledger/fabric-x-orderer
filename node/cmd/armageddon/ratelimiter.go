package armageddon

import (
	"sync"
	"time"

	ab "github.com/hyperledger/fabric-protos-go/orderer"
)

// sendTxRateLimiter controls the rate of txs sent to the routers.
// by such a rate limiter, txs are sent to routers at a specified constant rate that they can deal with in terms of memory and CPU.
type sendTxRateLimiterBucket struct {
	capacity     int           // Maximum number of tokens in the bucket at a time
	tokens       int           // Current number of tokens in the bucket
	fillInterval time.Duration // Time interval to add one token
	mutex        sync.Mutex    // Mutex to synchronize access to the bucket
	cond         sync.Cond     // Condition variable to wait for tokens
	stopChan     chan bool     // Channel to stop the bucket
}

// newSendTxRateLimiterBucket creates a new sendTxRateLimiterBucket with a given capacity and fill rate.
func newSendTxRateLimiterBucket(capacity int, fillRate int) *sendTxRateLimiterBucket {
	rl := &sendTxRateLimiterBucket{
		capacity:     capacity,
		tokens:       0,
		fillInterval: time.Second / time.Duration(fillRate),
		stopChan:     make(chan bool),
	}
	rl.cond = sync.Cond{L: &rl.mutex}

	go rl.fillBucket()
	return rl
}

// fillBucket fills the bucket at the specified interval.
func (rl *sendTxRateLimiterBucket) fillBucket() {
	ticker := time.NewTicker(rl.fillInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rl.mutex.Lock()
			if rl.tokens < rl.capacity {
				rl.tokens++
				rl.cond.Signal()
			}
			rl.mutex.Unlock()
		case <-rl.stopChan:
			return
		}
	}
}

// stop stops the bucket.
func (rl *sendTxRateLimiterBucket) stop() {
	rl.stopChan <- true
}

// removeFromBucketAndSendTx sends a tx if there is a token available.
// the function gets as an argument a function that is responsible for send txs to router.
// this function can be mocked in tests that measure TPS.
func (rl *sendTxRateLimiterBucket) removeFromBucketAndSendTx(send func(txsMap map[string]struct{}, streams []ab.AtomicBroadcast_BroadcastClient, i int), txsMap map[string]struct{}, streams []ab.AtomicBroadcast_BroadcastClient, i int) bool {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	for rl.tokens <= 0 {
		rl.cond.Wait()
	}

	rl.tokens--

	// send txs to all routers
	send(txsMap, streams, i)
	return true
}
