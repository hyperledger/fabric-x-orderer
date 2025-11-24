/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	promgo "github.com/prometheus/client_model/go"
	"golang.org/x/sync/errgroup"
)

const (
	scheme         = "http://"
	metricsSubPath = "/metrics"
)

// Provider is a prometheus metrics provider.
type Provider struct {
	logger   types.Logger
	registry *prometheus.Registry
	url      string
}

// NewProvider creates a new prometheus metrics provider.
func NewProvider(logger types.Logger) *Provider {
	return &Provider{logger: logger, registry: prometheus.NewRegistry()}
}

// StartPrometheusServer starts a prometheus server.
// It also starts the given monitoring methods. Their context will cancel once the server is cancelled.
// This method returns once the server is shutdown and all monitoring methods returns.
func (p *Provider) StartPrometheusServer(
	ctx context.Context, listener net.Listener, monitor ...func(context.Context),
) error {
	p.logger.Debugf("Creating prometheus server")
	mux := http.NewServeMux()
	mux.Handle(
		metricsSubPath,
		promhttp.HandlerFor(
			p.Registry(),
			promhttp.HandlerOpts{
				Registry: p.Registry(),
			},
		),
	)
	server := &http.Server{
		ReadTimeout: 30 * time.Second,
		Handler:     mux,
	}

	var err error
	p.url, err = MakeMetricsURL(listener.Addr().String())
	if err != nil {
		return errors.Wrap(err, "failed formatting URL")
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		p.logger.Infof("Prometheus serving on URL: %s", p.url)
		defer p.logger.Infof("Prometheus stopped serving")
		return server.Serve(listener)
	})

	// The following ensures the method does not return before all monitor methods return.
	for _, m := range monitor {
		g.Go(func() error {
			m(gCtx)
			return nil
		})
	}

	// The following ensures the method does not return before the close procedure is complete.
	stopAfter := context.AfterFunc(ctx, func() {
		go func() error {
			if errClose := server.Close(); errClose != nil {
				return errors.Wrap(errClose, "failed to close prometheus server")
			}
			return nil
		}()
	})
	defer stopAfter()

	if err = g.Wait(); !errors.Is(err, http.ErrServerClosed) {
		return errors.Wrap(err, "prometheus server stopped with an error")
	}
	return nil
}

// URL returns the prometheus server URL.
func (p *Provider) URL() string {
	return p.url
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

	p.registry.MustRegister(c.cv)
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

	p.registry.MustRegister(g.gv)
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

	p.registry.MustRegister(h.hv)
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

// Registry returns the prometheus registry.
func (p *Provider) Registry() *prometheus.Registry {
	return p.registry
}

func GetMetricValue(m prometheus.Metric, logger types.Logger) float64 {
	gm := promgo.Metric{}
	err := m.Write(&gm)
	if err != nil {
		logger.Infof("%v", err.Error())
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
		logger.Infof("unsupported metric")
		return 0
	}
}
