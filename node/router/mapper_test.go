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

// scenario: check that with a single shard, requests are consistently mapped to the same shard.
func OneShardTest(t *testing.T, m MapperInterface) {
	t.Run("Basic Test", func(t *testing.T) {
		require.Equal(t, uint16(1), m.GetNumberOfShards())
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

// scenario: check that a request consistently receives the same shard and request-ID (i.e., no randomness in the mapping).
func RemapTest(t *testing.T, m MapperInterface) {
	t.Run("Remap Test", func(t *testing.T) {
		req1 := []byte("request 123")
		s, reqID := m.Map(req1)
		for i := 0; i < 100; i++ {
			s1, reqID1 := m.Map(req1)
			require.True(t, s == s1)
			require.True(t, bytes.Equal(reqID, reqID1))
		}
	})
}

// scenario: check that the mapper uniformly distributes requests across all shards.
func UniformMappingTest(t *testing.T, m MapperInterface) {
	t.Run("Uniform Mapping Test", func(t *testing.T) {
		numOfRequest := 999
		countMap := make(map[int]int)
		r := rand.New(rand.NewSource(int64(42))) // create a Rand object with a seed value (42)
		for i := 0; i < numOfRequest; i++ {
			req := []byte("request " + strconv.Itoa(r.Intn(10000)))
			s, _ := m.Map(req)
			countMap[int(s)]++

		}
		numShards := m.GetNumberOfShards()
		average := float64(numOfRequest) / float64(numShards)
		// check that each shard appears approximatly close to the average
		for _, count := range countMap {
			require.True(t, math.Abs(float64(count)-average)/average < 0.1)
		}
	})
}

// scenario: check that the mapper assigns different request-IDs to different requests.
func UniqueIDsTest(t *testing.T, m MapperInterface) {
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

func TestMappers(t *testing.T) {
	logger := testutil.CreateLogger(t, 0)

	// Test MapperCRC64
	OneShardTest(t, MapperCRC64{logger, uint16(1)})
	RemapTest(t, MapperCRC64{logger, uint16(4)})
	UniformMappingTest(t, MapperCRC64{logger, uint16(4)})
	UniqueIDsTest(t, MapperCRC64{logger, uint16(4)})
}
