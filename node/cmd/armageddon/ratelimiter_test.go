package armageddon

import (
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSendTxRateLimiterBucket(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)

	capacity := 500
	fillRate := 500
	transactions := 1000
	rl := newSendTxRateLimiterBucket(capacity, fillRate)

	sendFunc := func(txsMap map[string]struct{}, streams []ab.AtomicBroadcast_BroadcastClient, i int) {
		return
	}

	start := time.Now()

	for i := 0; i < transactions; i++ {
		status := rl.removeFromBucketAndSendTx(sendFunc, nil, nil, i)
		require.True(t, status)
	}
	rl.stop()

	elapsed := time.Since(start)

	TPS := float64(transactions / int(elapsed.Seconds()))

	max := float64(fillRate) * 1.3
	min := float64(fillRate) * 0.7

	require.Less(t, TPS, max)
	require.Less(t, min, TPS)
}
