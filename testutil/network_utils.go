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

// FetchPrometheusMetricValue fetches the value of a Prometheus metric from the specified URL using the provided regular expression.
// It returns the metric value as an integer. If the metric is not found or cannot be converted to an integer, it returns -1.
func FetchPrometheusMetricValue(t *testing.T, re *regexp.Regexp, url string) int {
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
