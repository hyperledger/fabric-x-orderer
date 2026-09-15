/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger/fabric-lib-go/common/metrics"
	"github.com/pkg/errors"
)

var queryClient = &http.Client{Timeout: 5 * time.Second}

type Reader struct {
	address string
	window  time.Duration
	err     error
}

func NewReader(address string, window time.Duration) *Reader {
	return &Reader{address: address, window: window}
}

func (r *Reader) Err() error {
	return r.err
}

// Total returns the current counter value.
func (r *Reader) Total(opts metrics.CounterOpts, labelValues ...string) uint64 {
	return uint64(r.query(r.selector(opts.Namespace, opts.Name, opts.LabelNames, labelValues)))
}

// Gauge returns the current gauge value.
func (r *Reader) Gauge(opts metrics.GaugeOpts, labelValues ...string) float64 {
	return r.query(r.selector(opts.Namespace, opts.Name, opts.LabelNames, labelValues))
}

// HistogramAverage returns the cumulative histogram average.
func (r *Reader) HistogramAverage(opts metrics.HistogramOpts, labelValues ...string) float64 {
	sum, count := r.histogramSelectors(opts, labelValues)
	return r.query(fmt.Sprintf("%s / clamp_min(%s, 1)", sum, count))
}

// HistogramIntervalAverage returns the histogram average over the last interval.
func (r *Reader) HistogramIntervalAverage(opts metrics.HistogramOpts, labelValues ...string) float64 {
	sum, count := r.histogramSelectors(opts, labelValues)

	window := fmt.Sprintf("%dms", r.window.Milliseconds())
	if r.window%time.Second == 0 {
		window = fmt.Sprintf("%ds", r.window/time.Second)
	}

	avg := r.query(fmt.Sprintf("rate(%s[%s]) / rate(%s[%s])", sum, window, count, window))
	if math.IsNaN(avg) {
		return 0
	}

	return avg
}

func (r *Reader) histogramSelectors(opts metrics.HistogramOpts, labelValues []string) (string, string) {
	return r.selector(opts.Namespace, opts.Name+"_sum", opts.LabelNames, labelValues), r.selector(opts.Namespace, opts.Name+"_count", opts.LabelNames, labelValues)
}

func (r *Reader) query(query string) float64 {
	if r.err != nil {
		return 0
	}

	queryURL := fmt.Sprintf("%s/api/v1/query?query=%s", strings.TrimRight(r.address, "/"), url.QueryEscape(query))

	resp, err := queryClient.Get(queryURL) //nolint:gosec
	if err != nil {
		r.err = errors.Wrapf(err, "failed to reach Prometheus for %s", query)
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		r.err = errors.Errorf("Prometheus returned status %s for %s", resp.Status, query)
		return 0
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
		r.err = errors.Wrapf(err, "failed to decode Prometheus response for %s", query)
		return 0
	}

	if response.Status != "success" || len(response.Data.Result) != 1 {
		r.err = errors.Errorf("invalid Prometheus response for %s", query)
		return 0
	}

	var value string
	if err := json.Unmarshal(response.Data.Result[0].Value[1], &value); err != nil {
		r.err = err
		return 0
	}

	result, err := strconv.ParseFloat(value, 64)
	if err != nil {
		r.err = err
		return 0
	}

	return result
}

func (r *Reader) selector(namespace, name string, labelNames, labelValues []string) string {
	if namespace != "" {
		name = namespace + "_" + name
	}

	if len(labelNames) != len(labelValues) {
		r.err = errors.Errorf("metric %s declares %d labels, but %d values were given", name, len(labelNames), len(labelValues))
		return ""
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
