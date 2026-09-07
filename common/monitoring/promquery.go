/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package monitoring

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const queryTimeout = 5 * time.Second

var queryClient = &http.Client{Timeout: queryTimeout}

// Selector builds a PromQL selector for a metric and its labels.
func Selector(namespace, name string, labelNames, labelValues []string) string {
	if len(labelNames) != len(labelValues) {
		panic(fmt.Sprintf("metric %s_%s declares %d labels, but %d label values were given",
			namespace, name, len(labelNames), len(labelValues)))
	}

	fqName := name
	if namespace != "" {
		fqName = namespace + "_" + name
	}
	if len(labelNames) == 0 {
		return fqName
	}

	matchers := make([]string, 0, len(labelNames))
	for i, labelName := range labelNames {
		matchers = append(matchers, fmt.Sprintf("%s=%q", labelName, labelValues[i]))
	}

	return fmt.Sprintf("%s{%s}", fqName, strings.Join(matchers, ","))
}

// Query runs a Prometheus instant query and returns a single sample.
func Query(address string, expr string) (float64, error) {
	queryURL := fmt.Sprintf("%s/api/v1/query?query=%s", address, url.QueryEscape(expr))

	resp, err := queryClient.Get(queryURL) //nolint:gosec // the address is operator supplied local configuration
	if err != nil {
		return 0, errors.Wrap(err, "failed to reach Prometheus")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, errors.Errorf("Prometheus returned status %s for %s", resp.Status, expr)
	}

	var result struct {
		Data struct {
			Result []struct {
				Value [2]json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, errors.Wrapf(err, "failed to decode response for %s", expr)
	}

	if len(result.Data.Result) != 1 {
		return 0, errors.Errorf("expected a single sample for %s, got %d", expr, len(result.Data.Result))
	}

	var value string
	if err := json.Unmarshal(result.Data.Result[0].Value[1], &value); err != nil {
		return 0, errors.Wrapf(err, "failed to read sample value for %s", expr)
	}

	return strconv.ParseFloat(value, 64)
}
