/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package router

import (
	"bytes"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/hyperledger/fabric-x-orderer/testutil"
	"github.com/stretchr/testify/require"
)

func TestMultiShardMapper(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	// test MapperCRC64 with 4 shards
	var numShards uint16 = 4
	m := MapperCRC64{logger, numShards}

	// check that a request consistently receives the same shard and request-ID (i.e., no randomness in the mapping).
	t.Run("Remap Test", func(t *testing.T) {
		req1 := []byte("request 123")
		s, reqID := m.Map(req1)
		for i := 0; i < 100; i++ {
			s1, reqID1 := m.Map(req1)
			require.True(t, s == s1)
			require.True(t, bytes.Equal(reqID, reqID1))
		}
	})

	// check that the mapper uniformly distributes requests across all shards.
	t.Run("Uniform Mapping Test", func(t *testing.T) {
		// caclculate the required number of samples using Hoeffding's inequality
		p := 1 / float64(numShards)                               // probability to map to each shard
		d := 0.1                                                  // deviation from mean. i.e |avg - mean| < d * mean
		fp := 0.01                                                // fail probability
		n := int(math.Ceil(math.Log(2/fp) / (2 * p * p * d * d))) // number of samples needed
		µ := float64(n) * p                                       // mean

		// fmt.Printf(" shards: %d, n: %d \n", numShards, n)
		countMap := make(map[uint16]int)
		r := rand.New(rand.NewSource(int64(42))) // create a Rand object with a seed value (42)
		for i := 0; i < n; i++ {
			req := []byte("request " + strconv.Itoa(r.Intn(1000000)))
			s, _ := m.Map(req)
			countMap[s]++
		}

		// check that each shard appears approximatly close to the average
		for _, count := range countMap {
			require.True(t, math.Abs(float64(count)-µ)/µ < d)
		}
	})

	// check that the mapper assigns different request-IDs to different requests.
	t.Run("Unique ID's Test", func(t *testing.T) {
		numOfRequest := 999
		IDsMap := make(map[string]int)
		for i := 0; i < numOfRequest; i++ {
			req := []byte("request " + strconv.Itoa(i))
			_, reqID := m.Map(req)
			IDsMap[string(reqID)]++
		}
		require.Equal(t, numOfRequest, len(IDsMap))
	})
}

func TestOneSharMapper(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	// test MapperCRC64 with 1 shard
	m := MapperCRC64{logger, 1}

	// check that with a single shard, requests are consistently mapped to the same shard.
	t.Run("One Shard Test", func(t *testing.T) {
		// check that requests are mapped to the same shard
		r := []byte("a request")
		shard, _ := m.Map(r)
		for i := 0; i < 100; i++ {
			req := []byte("request" + strconv.Itoa(i))
			s, _ := m.Map(req)
			require.Equal(t, shard, s)
		}

		// check that different requests get different ID's
		req1 := []byte("request 123")
		req2 := []byte("request ABC")

		s1, reqID1 := m.Map(req1)
		s2, reqID2 := m.Map(req2)

		require.Equal(t, shard, s1)
		require.Equal(t, shard, s2)
		require.False(t, bytes.Equal(reqID1, reqID2))
	})
}
