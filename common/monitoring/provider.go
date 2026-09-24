/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/prometheus/client_golang/prometheus"
	promgo "github.com/prometheus/client_model/go"
)

var providerRegistryLock = &sync.Mutex{}

// Provider is a prometheus metrics provider.
type Provider struct {
	disabled bool
}

func NewProvider(providerType string, logger *flogging.FabricLogger) metrics.Provider {
	var provider metrics.Provider

	switch providerType {
	case "prometheus":
		provider = &Provider{}
	default:
		if providerType != "disabled" {
			logger.Warnf("Unknown provider type: %s; metrics disabled", providerType)
		}
		provider = &Provider{disabled: true}
	}

	return provider
}

func (p *Provider) register(c prometheus.Collector) error {
	if p.disabled {
		return nil
	}

	providerRegistryLock.Lock()
	defer providerRegistryLock.Unlock()

	// Attempt to register the collector
	if err := prometheus.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			// Unregister the existing collector and register the new one
			if !prometheus.Unregister(are.ExistingCollector) {
				return fmt.Errorf("failed to unregister existing collector")
			}
			// Retry registration after unregistering
			if err := prometheus.Register(c); err != nil {
				return fmt.Errorf("failed to re-register collector: %w", err)
			}
			return nil
		}
		return fmt.Errorf("failed to register collector: %w", err)
	}

	return nil
}

func (p *Provider) NewCounter(o metrics.CounterOpts) metrics.Counter {
	c := &Counter{
		cv: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: o.Namespace,
				Subsystem: o.Subsystem,
				Name:      o.Name,
				Help:      o.Help,
			},
			o.LabelNames,
		),
	}

	if err := p.register(c.cv); err != nil {
		panic(err)
	}

	if len(o.LabelNames) == 0 {
		c.Counter = c.cv.WithLabelValues()
	}

	return c
}

func (p *Provider) NewGauge(o metrics.GaugeOpts) metrics.Gauge {
	g := &Gauge{
		gv: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: o.Namespace,
				Subsystem: o.Subsystem,
				Name:      o.Name,
				Help:      o.Help,
			},
			o.LabelNames,
		),
	}

	if err := p.register(g.gv); err != nil {
		panic(err)
	}
	return g
}

func (p *Provider) NewHistogram(o metrics.HistogramOpts) metrics.Histogram {
	h := &Histogram{
		hv: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: o.Namespace,
				Subsystem: o.Subsystem,
				Name:      o.Name,
				Help:      o.Help,
				Buckets:   o.Buckets,
			},
			o.LabelNames,
		),
	}

	if err := p.register(h.hv); err != nil {
		panic(err)
	}
	return h
}

type Counter struct {
	prometheus.Counter
	cv *prometheus.CounterVec
}

func (c *Counter) With(labelValues ...string) metrics.Counter {
	return &Counter{Counter: c.cv.WithLabelValues(labelValues...), cv: c.cv}
}

type Gauge struct {
	prometheus.Gauge
	gv *prometheus.GaugeVec
}

func (g *Gauge) With(labelValues ...string) metrics.Gauge {
	return &Gauge{Gauge: g.gv.WithLabelValues(labelValues...), gv: g.gv}
}

type Histogram struct {
	prometheus.Histogram
	hv *prometheus.HistogramVec
}

func (h *Histogram) With(labelValues ...string) metrics.Histogram {
	return &Histogram{Histogram: h.hv.WithLabelValues(labelValues...).(prometheus.Histogram), hv: h.hv}
}

func GetMetricValue(m prometheus.Metric, logger *flogging.FabricLogger) float64 {
	gm := promgo.Metric{}
	err := m.Write(&gm)
	if err != nil {
		logger.Error(err)
		return 0
	}

	switch {
	case gm.Gauge != nil:
		return gm.Gauge.GetValue()
	case gm.Counter != nil:
		return gm.Counter.GetValue()
	case gm.Untyped != nil:
		return gm.Untyped.GetValue()
	case gm.Summary != nil:
		return gm.Summary.GetSampleSum()
	case gm.Histogram != nil:
		return gm.Histogram.GetSampleSum()
	default:
		logger.Errorf("unsupported metric")
		return 0
	}
}
