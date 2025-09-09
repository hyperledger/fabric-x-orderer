/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package promutil

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// AddToCounter adds a value to a prometheus counter.
func AddToCounter(c prometheus.Counter, n int) {
	c.Add(float64(n))
}

// AddToCounterVec add a value to given labels on a prometheus counter vector.
func AddToCounterVec(c *prometheus.CounterVec, labels []string, n int) {
	c.WithLabelValues(labels...).Add(float64(n))
}

// AddToGauge adds a value to a prometheus gauge.
func AddToGauge(g prometheus.Gauge, n int) {
	g.Add(float64(n))
}

// SubFromGauge subtracts a given value from a prometheus gauge.
func SubFromGauge(g prometheus.Gauge, n int) {
	g.Sub(float64(n))
}

// SetGaugeVec sets a value to given labels on the prometheus gauge vector.
func SetGaugeVec(c *prometheus.GaugeVec, labels []string, n int) {
	c.WithLabelValues(labels...).Set(float64(n))
}

// SetGauge sets a value to a prometheus gauge.
func SetGauge(queue prometheus.Gauge, n int) {
	queue.Set(float64(n))
}

// SetUint64Gauge sets a uint64 value to a prometheus gauge.
func SetUint64Gauge(queue prometheus.Gauge, n uint64) {
	queue.Set(float64(n))
}

// Observe observes a prometheus histogram.
func Observe(h prometheus.Observer, d time.Duration) {
	h.Observe(d.Seconds())
}

// ObserveSize observes a prometheus histogram size.
func ObserveSize(h prometheus.Observer, size int) {
	h.Observe(float64(size))
}
