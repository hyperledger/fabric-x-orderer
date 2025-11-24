/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"context"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func GetAvailablePort(t *testing.T) (port string, ll net.Listener) {
	addr := "127.0.0.1:0"
	listenConfig := net.ListenConfig{}

	ll, err := listenConfig.Listen(context.Background(), "tcp", addr)
	require.NoError(t, err)

	endpoint := ll.Addr().String()
	_, portS, err := net.SplitHostPort(endpoint)
	require.NoError(t, err)

	return portS, ll
}

// GetCounterMetricValueByRegexp retrieves the value of a counter metric from a given URL by matching a regular expression pattern.
// The function makes an HTTP GET request to the specified URL and searches the response body for the first match of the provided regex pattern.
// The matched value is expected to be in a format where the metric value is the second element when split by whitespace.
//
// Parameters:
//   - t: Testing object for assertions and logging
//   - re: Regular expression pattern to match the metric
//   - url: The URL endpoint to fetch metrics from
//
// Returns:
//   - int: The numeric value of the matched counter metric
//   - Returns -1 if no matches are found or if the value cannot be converted to an integer
func GetCounterMetricValueByRegexp(t *testing.T, re *regexp.Regexp, url string) int {
	resp, err := http.Get(url)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	t.Log(string(body))

	// Find all matches
	matches := re.FindAllString(string(body), -1)

	if len(matches) > 0 {
		val, err := strconv.Atoi(strings.Split(matches[0], " ")[1])
		if err != nil {
			return -1
		}
		return val
	}

	return -1
}

// CaptureArmaNodePrometheusServiceURL retrieves the Prometheus metrics endpoint URL from the given ArmaNodeInfo's session output.
// It waits until the URL is found or times out, and returns the metrics endpoint as a string.
func CaptureArmaNodePrometheusServiceURL(t *testing.T, armaNodeInfo *ArmaNodeInfo) string {
	var url string
	re := regexp.MustCompile(`Prometheus serving on URL:\s+(https?://[^/\s]+/metrics)`)
	require.Eventually(t, func() bool {
		output := string(armaNodeInfo.RunInfo.Session.Err.Contents())
		matches := re.FindStringSubmatch(output)
		if len(matches) > 1 {
			url = matches[1]
			return true
		}
		return false
	}, 60*time.Second, 10*time.Millisecond)

	return url
}
