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

	kitmetrics "github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/pkg/errors"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

const (
	scheme         = "http://"
	metricsSubPath = "/metrics"
)

// Provider is a prometheus metrics provider.
type Provider struct {
	logger types.Logger
	url    string
}

// NewProvider creates a new prometheus metrics provider.
func NewProvider(logger types.Logger) *Provider {
	return &Provider{logger: logger}
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
		promhttp.Handler(),
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
		g.Go(func() error {
			if errClose := server.Close(); err != nil {
				return errors.Wrap(errClose, "failed to close prometheus server")
			}
			return nil
		})
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
	return &Counter{
		Counter: prometheus.NewCounterFrom(
			prom.CounterOpts{
				Namespace: o.Namespace,
				Subsystem: o.Subsystem,
				Name:      o.Name,
				Help:      o.Help,
			},
			o.LabelNames,
		),
	}
}

func (p *Provider) NewGauge(o metrics.GaugeOpts) metrics.Gauge {
	return &Gauge{
		Gauge: prometheus.NewGaugeFrom(
			prom.GaugeOpts{
				Namespace: o.Namespace,
				Subsystem: o.Subsystem,
				Name:      o.Name,
				Help:      o.Help,
			},
			o.LabelNames,
		),
	}
}

func (p *Provider) NewHistogram(o metrics.HistogramOpts) metrics.Histogram {
	return &Histogram{
		Histogram: prometheus.NewHistogramFrom(
			prom.HistogramOpts{
				Namespace: o.Namespace,
				Subsystem: o.Subsystem,
				Name:      o.Name,
				Help:      o.Help,
				Buckets:   o.Buckets,
			},
			o.LabelNames,
		),
	}
}

type Counter struct{ kitmetrics.Counter }

func (c *Counter) With(labelValues ...string) metrics.Counter {
	return &Counter{Counter: c.Counter.With(labelValues...)}
}

type Gauge struct{ kitmetrics.Gauge }

func (g *Gauge) With(labelValues ...string) metrics.Gauge {
	return &Gauge{Gauge: g.Gauge.With(labelValues...)}
}

type Histogram struct{ kitmetrics.Histogram }

func (h *Histogram) With(labelValues ...string) metrics.Histogram {
	return &Histogram{Histogram: h.Histogram.With(labelValues...)}
}
