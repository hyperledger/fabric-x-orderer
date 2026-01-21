/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"net/url"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/prometheus/client_golang/prometheus"
	promgo "github.com/prometheus/client_model/go"
)

const (
	scheme         = "http://"
	metricsSubPath = "/metrics"
)

// Provider is a prometheus metrics provider.
type Provider struct {
	// registry *prometheus.Registry
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
		// provider = &disabled.Provider{}
	}

	return provider
}

// MakeMetricsURL construct the Prometheus metrics URL.
func MakeMetricsURL(address string) (string, error) {
	return url.JoinPath(scheme, address, metricsSubPath)
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

	if !p.disabled {
		// Unregister the counter vector in case it was already registered, to avoid panic.
		prometheus.Unregister(c.cv)
		prometheus.MustRegister(c.cv)
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

	if !p.disabled {
		// Unregister the gauge vector in case it was already registered, to avoid panic.
		prometheus.Unregister(g.gv)
		prometheus.MustRegister(g.gv)
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

	if !p.disabled {
		// Unregister the histogram vector in case it was already registered, to avoid panic.
		prometheus.Unregister(h.hv)
		prometheus.MustRegister(h.hv)
	}
	return h
}

type Counter struct {
	prometheus.Counter
	cv *prometheus.CounterVec
}

func (c *Counter) With(labelValues ...string) metrics.Counter {
	return &Counter{Counter: c.cv.WithLabelValues(labelValues...)}
}

type Gauge struct {
	prometheus.Gauge
	gv *prometheus.GaugeVec
}

func (g *Gauge) With(labelValues ...string) metrics.Gauge {
	return &Gauge{Gauge: g.gv.WithLabelValues(labelValues...)}
}

type Histogram struct {
	prometheus.Histogram
	hv *prometheus.HistogramVec
}

func (h *Histogram) With(labelValues ...string) metrics.Histogram {
	return &Histogram{Histogram: h.hv.WithLabelValues(labelValues...).(prometheus.Histogram)}
}

func GetMetricValue(m prometheus.Metric, logger *flogging.FabricLogger) float64 {
	gm := promgo.Metric{}
	err := m.Write(&gm)
	if err != nil {
		logger.Errorf("%v", err.Error())
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
