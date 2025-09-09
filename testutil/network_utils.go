/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package testutil

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-orderer/common/types"
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

// RouterIncomingTxMetric retrieves the value of the "router_requests_completed" metric for a given party ID
// from the specified URL. It sends an HTTP GET request to the URL, parses the response body to find the metric
// line matching the provided party ID, and returns the metric value as an integer. If the metric is not found
// or an error occurs during parsing, it returns -1.
//
// Parameters:
//   - t: The testing context used for assertions and logging.
//   - partID: The PartyID for which the metric should be retrieved.
//   - url: The URL to query for the metric.
//
// Returns:
//   - int: The value of the "router_requests_completed" metric for the given party ID, or -1 if not found or on error.
func RouterIncomingTxMetric(t *testing.T, partID types.PartyID, url string) int {
	resp, err := http.Get(url)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	t.Log(string(body))

	pattern := fmt.Sprintf(`router_requests_completed\{party_id="%d"\} \d+`, partID)

	re := regexp.MustCompile(pattern)

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

// WaitForPrometheusServiceURL retrieves the Prometheus metrics endpoint URL from the given ArmaNodeInfo's session output.
// It waits until the URL is found or times out, and returns the metrics endpoint as a string.
func WaitForPrometheusServiceURL(t *testing.T, armaNodeInfo *ArmaNodeInfo) string {
	var url string
	require.Eventually(t, func() bool {
		output := string(armaNodeInfo.RunInfo.Session.Err.Contents())
		re := regexp.MustCompile(`Prometheus serving on URL:\s+(https?://[^/\s]+/metrics)`)
		matches := re.FindStringSubmatch(output)
		if len(matches) > 1 {
			url = matches[1]
			return true
		}
		return false
	}, 60*time.Second, 10*time.Millisecond)

	return url
}
