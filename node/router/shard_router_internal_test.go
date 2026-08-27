/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// numOfRequestIDsPerStream is how many request IDs are drawn per stream in the pool. A coupon
// collector over n streams needs about n*ln(n) draws to hit them all, so 100 draws per stream
// leaves a wide margin.
const numOfRequestIDsPerStream = 100

// TestStreamIndexesCoverAllStreams
// Scenario:
//  1. For each (connections, streams-per-connection) layout, create a shard router with that layout.
//  2. Derive request IDs from distinct payloads with the router's own CRC64 request-ID function.
//  3. Map every request ID to a (connection, stream-in-connection) pair.
//  4. Assert both indexes are within the layout's bounds.
//  5. Assert the pairs cover every one of the connections*streams-per-connection streams.
func TestStreamIndexesCoverAllStreams(t *testing.T) {
	for _, layout := range []struct {
		connections    int
		streamsPerConn int
	}{
		{connections: 1, streamsPerConn: 1},
		{connections: 10, streamsPerConn: 5}, // the shipped defaults
		{connections: 10, streamsPerConn: 20},
		{connections: 4, streamsPerConn: 4},
		{connections: 3, streamsPerConn: 7},
	} {
		t.Run(fmt.Sprintf("%dconns_%dstreams", layout.connections, layout.streamsPerConn), func(t *testing.T) {
			sr := &ShardRouter{
				router2batcherConnPoolSize:   layout.connections,
				router2batcherStreamsPerConn: layout.streamsPerConn,
			}
			numOfSlots := layout.connections * layout.streamsPerConn

			hitsPerSlot := make(map[[2]int]int, numOfSlots)
			for i := 0; i < numOfSlots*numOfRequestIDsPerStream; i++ {
				reqID, _ := CRC64RequestToShard(1)([]byte(fmt.Sprintf("request-%d", i)))

				connIndex, streamInConnIndex := sr.streamIndexes(reqID)
				require.GreaterOrEqual(t, connIndex, 0)
				require.Less(t, connIndex, layout.connections)
				require.GreaterOrEqual(t, streamInConnIndex, 0)
				require.Less(t, streamInConnIndex, layout.streamsPerConn)

				hitsPerSlot[[2]int{connIndex, streamInConnIndex}]++
			}

			require.Len(t, hitsPerSlot, numOfSlots, "streams that were never selected: %v",
				missingStreams(hitsPerSlot, layout.connections, layout.streamsPerConn))
		})
	}
}

// TestStreamIndexesSpreadRequestsEvenly
// Scenario:
//  1. Create a shard router with 10 connections and 20 streams per connection.
//  2. Derive request IDs from distinct payloads with the router's own CRC64 request-ID function.
//  3. Count how many request IDs land on each stream.
//  4. Assert no stream takes more than twice its even share, and none less than half of it.
func TestStreamIndexesSpreadRequestsEvenly(t *testing.T) {
	sr := &ShardRouter{router2batcherConnPoolSize: 10, router2batcherStreamsPerConn: 20}
	numOfSlots := sr.router2batcherConnPoolSize * sr.router2batcherStreamsPerConn

	hitsPerSlot := make(map[[2]int]int, numOfSlots)
	for i := 0; i < numOfSlots*numOfRequestIDsPerStream; i++ {
		reqID, _ := CRC64RequestToShard(1)([]byte(fmt.Sprintf("request-%d", i)))
		connIndex, streamInConnIndex := sr.streamIndexes(reqID)
		hitsPerSlot[[2]int{connIndex, streamInConnIndex}]++
	}

	for slot, hits := range hitsPerSlot {
		require.LessOrEqual(t, hits, 2*numOfRequestIDsPerStream, "stream %v is overloaded", slot)
		require.GreaterOrEqual(t, hits, numOfRequestIDsPerStream/2, "stream %v is starved", slot)
	}
}

// TestStreamIndexesOnMalformedRequestID
// Scenario:
//  1. Create a shard router with 10 connections and 20 streams per connection.
//  2. Map request IDs that are nil, shorter than a CRC64 checksum, and longer than one.
//  3. Assert none of them panics and every resulting index is within bounds.
func TestStreamIndexesOnMalformedRequestID(t *testing.T) {
	sr := &ShardRouter{router2batcherConnPoolSize: 10, router2batcherStreamsPerConn: 20}

	for _, reqID := range [][]byte{
		nil,
		{},
		{0xff},
		{0xff, 0xff},
		{1, 2, 3, 4, 5, 6, 7},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	} {
		t.Run(fmt.Sprintf("len%d", len(reqID)), func(t *testing.T) {
			connIndex, streamInConnIndex := sr.streamIndexes(reqID)
			require.GreaterOrEqual(t, connIndex, 0)
			require.Less(t, connIndex, sr.router2batcherConnPoolSize)
			require.GreaterOrEqual(t, streamInConnIndex, 0)
			require.Less(t, streamInConnIndex, sr.router2batcherStreamsPerConn)
		})
	}
}

func missingStreams(hitsPerSlot map[[2]int]int, connections int, streamsPerConn int) [][2]int {
	var missing [][2]int
	for connIndex := 0; connIndex < connections; connIndex++ {
		for streamInConnIndex := 0; streamInConnIndex < streamsPerConn; streamInConnIndex++ {
			if _, ok := hitsPerSlot[[2]int{connIndex, streamInConnIndex}]; !ok {
				missing = append(missing, [2]int{connIndex, streamInConnIndex})
			}
		}
	}
	return missing
}
