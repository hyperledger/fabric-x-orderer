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

// shardCounts are the shard counts a stream selection is checked against. Two is the case that
// matters: a shard count dividing the number of streams is where a stream selected from the same
// bits as the shard would confine each shard to part of the stream pool.
var shardCounts = []uint16{1, 2}

// TestStreamIndexesCoverAllStreams
// Scenario:
//  1. For each (connections, streams-per-connection) layout and each shard count, create a shard
//     router with that layout.
//  2. Derive request IDs from distinct payloads with the router's own CRC64 request-ID function,
//     which maps each payload to a shard as well.
//  3. Map every request ID to a (connection, stream-in-connection) pair.
//  4. Assert both indexes are within the layout's bounds.
//  5. Assert the pairs cover every one of the connections*streams-per-connection streams, in every
//     shard: which shard a request is mapped to must not restrict which streams can carry it.
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
		for _, shardCount := range shardCounts {
			name := fmt.Sprintf("%dconns_%dstreams_%dshards", layout.connections, layout.streamsPerConn, shardCount)
			t.Run(name, func(t *testing.T) {
				sr := &ShardRouter{
					router2batcherConnPoolSize:   layout.connections,
					router2batcherStreamsPerConn: layout.streamsPerConn,
				}
				numOfSlots := layout.connections * layout.streamsPerConn

				for shard, hitsPerSlot := range hitsPerShardStream(t, sr, shardCount, numOfRequestIDsPerStream) {
					require.Len(t, hitsPerSlot, numOfSlots, "streams shard %d never selected: %v", shard,
						missingStreams(hitsPerSlot, layout.connections, layout.streamsPerConn))
				}
			})
		}
	}
}

// TestStreamIndexesSpreadRequestsEvenly
// Scenario:
//  1. For each shard count, create a shard router with 10 connections and 20 streams per connection.
//  2. Derive request IDs from distinct payloads with the router's own CRC64 request-ID function,
//     which maps each payload to a shard as well.
//  3. Count how many request IDs land on each stream of each shard.
//  4. Assert no stream takes more than twice its even share, and none less than half of it.
func TestStreamIndexesSpreadRequestsEvenly(t *testing.T) {
	for _, shardCount := range shardCounts {
		t.Run(fmt.Sprintf("%dshards", shardCount), func(t *testing.T) {
			sr := &ShardRouter{router2batcherConnPoolSize: 10, router2batcherStreamsPerConn: 20}

			for shard, hitsPerSlot := range hitsPerShardStream(t, sr, shardCount, numOfRequestIDsPerStream) {
				for slot, hits := range hitsPerSlot {
					require.LessOrEqual(t, hits, 2*numOfRequestIDsPerStream,
						"stream %v of shard %d is overloaded", slot, shard)
					require.GreaterOrEqual(t, hits, numOfRequestIDsPerStream/2,
						"stream %v of shard %d is starved", slot, shard)
				}
			}
		})
	}
}

// hitsPerShardStream derives request IDs the way the router does, maps each of them to a shard and
// to a stream, and reports how many landed on each stream of each shard. Enough request IDs are
// drawn for every shard to receive drawsPerStream of them per stream of its pool.
func hitsPerShardStream(t *testing.T, sr *ShardRouter, shardCount uint16, drawsPerStream int) map[uint16]map[[2]int]int {
	t.Helper()

	numOfSlots := sr.router2batcherConnPoolSize * sr.router2batcherStreamsPerConn
	mapRequest := CRC64RequestToShard(shardCount)

	hitsPerShardSlot := make(map[uint16]map[[2]int]int, shardCount)
	for shard := uint16(0); shard < shardCount; shard++ {
		hitsPerShardSlot[shard] = make(map[[2]int]int, numOfSlots)
	}

	for i := 0; i < int(shardCount)*numOfSlots*drawsPerStream; i++ {
		reqID, shard := mapRequest([]byte(fmt.Sprintf("request-%d", i)))

		connIndex, streamInConnIndex := sr.streamIndexes(reqID)
		require.GreaterOrEqual(t, connIndex, 0)
		require.Less(t, connIndex, sr.router2batcherConnPoolSize)
		require.GreaterOrEqual(t, streamInConnIndex, 0)
		require.Less(t, streamInConnIndex, sr.router2batcherStreamsPerConn)

		hitsPerShardSlot[shard][[2]int{connIndex, streamInConnIndex}]++
	}

	return hitsPerShardSlot
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
