/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring_test

import (
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestHistogramInterval(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "test_latency_seconds",
	})

	h.Observe(100)
	interval := monitoring.NewHistogramInterval(h, logger)

	h.Observe(1)
	h.Observe(3)
	require.Equal(t, 2.0, interval.Next(logger))

	h.Observe(10)
	require.Equal(t, 10.0, interval.Next(logger))

	require.Zero(t, interval.Next(logger))

	h.Observe(5)
	h.Observe(7)
	require.Equal(t, 6.0, interval.Next(logger))
}
