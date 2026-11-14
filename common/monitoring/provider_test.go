/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring_test

import (
	"errors"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/common/monitoring"
	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/prometheus/client_golang/prometheus"
	promgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestHistogramInterval(t *testing.T) {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "test_latency_seconds",
	})

	h.Observe(100)
	interval := monitoring.NewHistogramInterval(h, testutil.CreateLogger(t, 0))

	h.Observe(1)
	h.Observe(3)
	require.Equal(t, 2.0, interval.Next())

	h.Observe(10)
	require.Equal(t, 10.0, interval.Next())

	require.Zero(t, interval.Next())

	h.Observe(5)
	h.Observe(7)
	require.Equal(t, 6.0, interval.Next())
}

func TestHistogramIntervalReadError(t *testing.T) {
	h := &stubHistogram{sum: 100, count: 1}
	interval := monitoring.NewHistogramInterval(h, testutil.CreateLogger(t, 0))

	h.sum, h.count, h.err = 106, 3, errors.New("read error")
	require.Zero(t, interval.Next())

	h.sum, h.count, h.err = 112, 4, nil
	require.Equal(t, 4.0, interval.Next())
}

func TestHistogramIntervalCountDecrease(t *testing.T) {
	h := &stubHistogram{sum: 100, count: 10}
	interval := monitoring.NewHistogramInterval(h, testutil.CreateLogger(t, 0))

	h.sum, h.count = 6, 2
	require.Zero(t, interval.Next())

	h.sum, h.count = 14, 4
	require.Equal(t, 4.0, interval.Next())

	h.sum, h.count = 0, 0
	require.Zero(t, interval.Next())

	h.sum, h.count = 8, 2
	require.Equal(t, 4.0, interval.Next())
}

type stubHistogram struct {
	sum   float64
	count uint64
	err   error
}

func (s *stubHistogram) Desc() *prometheus.Desc { return nil }

func (s *stubHistogram) Write(out *promgo.Metric) error {
	if s.err != nil {
		return s.err
	}
	out.Histogram = &promgo.Histogram{SampleSum: &s.sum, SampleCount: &s.count}
	return nil
}

func TestCounterInterval(t *testing.T) {
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_total",
	})

	c.Add(100)
	interval := monitoring.NewCounterInterval(c, testutil.CreateLogger(t, 0))

	c.Add(5)
	delta, total := interval.Next()
	require.Equal(t, 5.0, delta)
	require.Equal(t, 105.0, total)

	delta, total = interval.Next()
	require.Zero(t, delta)
	require.Equal(t, 105.0, total)

	c.Add(7)
	delta, total = interval.Next()
	require.Equal(t, 7.0, delta)
	require.Equal(t, 112.0, total)
}

func TestCounterIntervalReadError(t *testing.T) {
	c := &stubCounter{value: 10}
	interval := monitoring.NewCounterInterval(c, testutil.CreateLogger(t, 0))

	c.value, c.err = 20, errors.New("read error")
	delta, total := interval.Next()
	require.Zero(t, delta)
	require.Equal(t, 10.0, total)

	c.value, c.err = 25, nil
	delta, total = interval.Next()
	require.Equal(t, 15.0, delta)
	require.Equal(t, 25.0, total)
}

func TestCounterIntervalDecrease(t *testing.T) {
	c := &stubCounter{value: 10}
	interval := monitoring.NewCounterInterval(c, testutil.CreateLogger(t, 0))

	c.value = 3
	delta, total := interval.Next()
	require.Zero(t, delta)
	require.Equal(t, 3.0, total)

	c.value = 5
	delta, total = interval.Next()
	require.Equal(t, 2.0, delta)
	require.Equal(t, 5.0, total)

	c.value = 0
	delta, total = interval.Next()
	require.Zero(t, delta)
	require.Zero(t, total)

	c.value = 4
	delta, total = interval.Next()
	require.Equal(t, 4.0, delta)
	require.Equal(t, 4.0, total)
}

type stubCounter struct {
	value float64
	err   error
}

func (s *stubCounter) Desc() *prometheus.Desc { return nil }

func (s *stubCounter) Write(out *promgo.Metric) error {
	if s.err != nil {
		return s.err
	}
	out.Counter = &promgo.Counter{Value: &s.value}
	return nil
}
