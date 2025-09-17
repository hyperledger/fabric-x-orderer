/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/prometheus/client_golang/prometheus"
)

type MonitoringMetric struct {
	Interval time.Duration
	Value    *uint64
	Labels   []string
}

func (m *MonitoringMetric) metricMonitorBuilder(counter prometheus.Counter) func(ctx context.Context) {
	return func(ctx context.Context) {
		ticker := time.NewTicker(m.Interval)
		defer ticker.Stop()
		lastValue := uint64(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				v := atomic.LoadUint64(m.Value)
				counter.Add(float64(v - lastValue))
				lastValue = v
			}
		}
	}
}

type Monitor struct {
	monitor  *Provider
	logger   types.Logger
	endpoint Endpoint
	monitors []func(context.Context)
	// stop is used to stop the monitoring service
	stop context.CancelFunc
}

type MonitorOpts struct {
	Endpoint
	// CounterOpts are the options used to create the CounterVec
	prometheus.CounterOpts
	Labels  []string
	Metrics []MonitoringMetric
}

// NewMonitor creates and returns a new Monitor instance using the provided MonitorOpts and logger.
// It initializes a metrics provider, sets up a CounterVec with the specified options and labels,
// and constructs monitor functions for each metric defined in the MonitorOpts.
// The returned Monitor is configured to track and log metrics as specified.
//
// Parameters:
//   - monitorOpts: Pointer to MonitorOpts containing configuration for metrics, labels, and host.
//   - logger:      Logger instance for logging within the Monitor.
//
// Returns:
//   - Pointer to the initialized Monitor.
func NewMonitor(monitorOpts *MonitorOpts, logger types.Logger) *Monitor {
	provider := NewProvider()
	counterVec := provider.NewCounterVec(monitorOpts.CounterOpts, monitorOpts.Labels)

	monitors := make([]func(context.Context), len(monitorOpts.Metrics))
	for i := range monitorOpts.Metrics {
		monitors[i] = monitorOpts.Metrics[i].metricMonitorBuilder(counterVec.WithLabelValues(monitorOpts.Metrics[i].Labels...))
	}

	return &Monitor{monitor: provider, endpoint: monitorOpts.Endpoint, logger: logger, monitors: monitors}
}

func (m *Monitor) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	m.stop = cancel
	serverConfig := ServerConfig{Endpoint: m.endpoint}

	go func() {
		m.monitor.StartPrometheusServer(ctx, &serverConfig, m.monitors...)
	}()
}

func (m *Monitor) Stop() {
	if m.stop != nil {
		m.stop()
	}
}

func (m *Monitor) Address() string {
	return fmt.Sprintf("http://%s/metrics", m.endpoint.Address())
}
