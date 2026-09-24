/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics"
)

const queryTimeout = 5 * time.Second

type Reader struct {
	address string
	window  time.Duration
	client  *http.Client
}

// NewReader returns a Reader that queries the Prometheus server at address.
// tlsConfig configures TLS for HTTPS connections.
func NewReader(address string, window time.Duration, tlsConfig *tls.Config) *Reader {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig

	return &Reader{
		address: address,
		window:  window,
		client:  &http.Client{Timeout: queryTimeout, Transport: transport},
	}
}

// Total returns the latest counter value from Prometheus.
func (r *Reader) Total(opts metrics.CounterOpts, labelValues ...string) (uint64, error) {
	value, err := r.query(r.selector(opts.Namespace, opts.Name, opts.LabelNames, labelValues))
	return uint64(value), err
}

// Gauge returns the current gauge value.
func (r *Reader) Gauge(opts metrics.GaugeOpts, labelValues ...string) (float64, error) {
	return r.query(r.selector(opts.Namespace, opts.Name, opts.LabelNames, labelValues))
}

// HistogramAverage returns the cumulative histogram average.
func (r *Reader) HistogramAverage(opts metrics.HistogramOpts, labelValues ...string) (float64, error) {
	sum, count := r.histogramSelectors(opts, labelValues)
	return r.query(fmt.Sprintf("%s / clamp_min(%s, 1)", sum, count))
}

// HistogramIntervalAverage returns the histogram average over the last interval.
func (r *Reader) HistogramIntervalAverage(opts metrics.HistogramOpts, labelValues ...string) (float64, error) {
	sum, count := r.histogramSelectors(opts, labelValues)

	window := fmt.Sprintf("%dms", r.window.Milliseconds())
	if r.window%time.Second == 0 {
		window = fmt.Sprintf("%ds", r.window/time.Second)
	}

	avg, err := r.query(fmt.Sprintf("rate(%s[%s]) / rate(%s[%s])", sum, window, count, window))
	if err != nil {
		return 0, err
	}
	if math.IsNaN(avg) {
		return 0, nil
	}

	return avg, nil
}

func (r *Reader) histogramSelectors(opts metrics.HistogramOpts, labelValues []string) (string, string) {
	return r.selector(opts.Namespace, opts.Name+"_sum", opts.LabelNames, labelValues), r.selector(opts.Namespace, opts.Name+"_count", opts.LabelNames, labelValues)
}

func (r *Reader) query(query string) (float64, error) {
	queryURL := fmt.Sprintf("%s/api/v1/query?query=%s", strings.TrimRight(r.address, "/"), url.QueryEscape(query))

	resp, err := r.client.Get(queryURL) //nolint:gosec
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("prometheus returned status %s", resp.Status)
	}

	var response struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return 0, err
	}

	if response.Status != "success" || len(response.Data.Result) != 1 {
		return 0, fmt.Errorf("invalid Prometheus response")
	}

	var value string
	if err := json.Unmarshal(response.Data.Result[0].Value[1], &value); err != nil {
		return 0, err
	}

	return strconv.ParseFloat(value, 64)
}

func (r *Reader) selector(namespace, name string, labelNames, labelValues []string) string {
	if namespace != "" {
		name = namespace + "_" + name
	}

	if len(labelNames) == 0 {
		return name
	}

	matchers := make([]string, len(labelNames))
	for i := range labelNames {
		matchers[i] = fmt.Sprintf("%s=%q", labelNames[i], labelValues[i])
	}

	return fmt.Sprintf("%s{%s}", name, strings.Join(matchers, ","))
}
