package core_test

import (
	"sync/atomic"
	"testing"
	"time"

	"arma/core"
	"arma/testutil"

	"github.com/stretchr/testify/require"
)

func TestAcker(t *testing.T) {
	acker := core.NewAcker(100, 10, 4, 2, testutil.CreateLogger(t, 0))

	var done uint32

	wait := func() {
		acker.WaitForSecondaries(110)
		atomic.AddUint32(&done, 1)
	}

	go wait()

	require.Equal(t, uint32(0), atomic.LoadUint32(&done))
	acker.HandleAck(100, 2)
	require.Equal(t, uint32(0), atomic.LoadUint32(&done))
	acker.HandleAck(100, 3)
	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&done) == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)

	acker.Stop()

	acker = core.NewAcker(100, 10, 4, 2, testutil.CreateLogger(t, 0))

	var done2 uint32

	waitStop := func() {
		atomic.AddUint32(&done2, 1)
		acker.WaitForSecondaries(110)
		atomic.AddUint32(&done2, 1)
	}

	go waitStop()

	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&done2) == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)
	acker.HandleAck(100, 2)
	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&done2) == uint32(1)
	}, 10*time.Second, 10*time.Millisecond)

	acker.Stop()

	require.Eventually(t, func() bool {
		return atomic.LoadUint32(&done2) == uint32(2)
	}, 10*time.Second, 10*time.Millisecond)

	acker.Stop()
}
