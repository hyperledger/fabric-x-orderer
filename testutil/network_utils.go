/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/healthz"
	"github.com/hyperledger/fabric-x-orderer/common/operations"
	"github.com/stretchr/testify/require"
)

type wrapListener struct {
	net.Listener
	once     sync.Once
	closeErr error
}

func (w *wrapListener) Close() error {
	w.once.Do(func() {
		w.closeErr = w.Listener.Close()
	})
	return w.closeErr
}

func GetAvailablePort(t testing.TB) (port string, ll net.Listener) {
	addr := "127.0.0.1:0"
	listenConfig := net.ListenConfig{}

	listener, err := listenConfig.Listen(context.Background(), "tcp", addr)
	require.NoError(t, err)

	ll = &wrapListener{Listener: listener}

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

func FetchVersionInfoValue(t *testing.T, re *regexp.Regexp, url string) *operations.VersionInfoHandler {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	require.NoError(t, err)

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	t.Log(string(body))

	// Find all matches
	matches := re.FindAllString(string(body), -1)

	if len(matches) > 0 {
		val := &operations.VersionInfoHandler{}
		err := json.Unmarshal([]byte(matches[0]), val)
		require.NoError(t, err)
		return val
	}

	return nil
}

// CaptureArmaNodePrometheusServiceURL retrieves the Prometheus metrics endpoint URL from the given ArmaNodeInfo's session output.
// It waits until the URL is found or times out, and returns the metrics endpoint as a string.
func CaptureArmaNodePrometheusServiceURL(t *testing.T, armaNodeInfo *ArmaNodeInfo) string {
	return captureArmaNodeServiceURL(t, armaNodeInfo, `Prometheus serving on URL:\s+(https?://[^/\s]+/metrics)`)
}

// CaptureArmaNodeVersionInfoServiceURL retrieves the version info endpoint URL from the given ArmaNodeInfo's session output.
// It waits until the URL is found or times out, and returns the version info endpoint as a string.
func CaptureArmaNodeVersionInfoServiceURL(t *testing.T, armaNodeInfo *ArmaNodeInfo) string {
	return captureArmaNodeServiceURL(t, armaNodeInfo, `Version info serving on URL:\s+(https?://[^/\s]+/version)`)
}

func captureArmaNodeServiceURL(t *testing.T, armaNodeInfo *ArmaNodeInfo, regex string) string {
	var url string
	re := regexp.MustCompile(regex)
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

// GetHealthCheckStatus retrieves the health status from the given health check endpoint URL using the provided regular expression.
// It returns true if the status is "OK", false if the status is "Unavailable", and fails the test for any unexpected response.
func GetHealthCheckStatus(t *testing.T, re *regexp.Regexp, url string) bool {
	resp, err := http.Get(url)
	require.NoError(t, err)

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	t.Log(string(body))

	// Find all matches
	matches := re.FindStringSubmatch(string(body))
	if len(matches) > 1 {
		if matches[1] == healthz.StatusOK {
			return true
		}
		if matches[1] == healthz.StatusUnavailable {
			return false
		}
	}
	t.Fatalf("Unexpected health check response: %s", string(body))
	return false
}

// CaptureArmaNodeHealthCheckServiceURL retrieves the health check endpoint URL from the given ArmaNodeInfo's session output.
// It waits until the URL is found or times out, and returns the health check endpoint as a string.
func CaptureArmaNodeHealthCheckServiceURL(t *testing.T, armaNodeInfo *ArmaNodeInfo) string {
	var url string
	re := regexp.MustCompile(`Health check serving on URL:\s+(https?://[^/\s]+/healthz)`)
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
