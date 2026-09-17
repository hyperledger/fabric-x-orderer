/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package request

import (
	"runtime"
	"sync"
)

// parallelForEachKey applies fn to every key, fanning the work out across up to
// runtime.NumCPU() workers. Keys are partitioned into contiguous chunks so each
// key is handled by exactly one worker and each worker iterates only its own
// chunk. If fn returns false the owning worker stops early for the rest of its
// chunk (mirrors the sync.Map.Range idiom).
//
// For small key sets (fewer keys than workers) the goroutine-spawn/join cost
// outweighs the work, so the keys are processed serially in the caller's
// goroutine instead.
func parallelForEachKey(keys []string, fn func(key string) bool) {
	if len(keys) == 0 {
		return
	}

	workerNum := runtime.NumCPU()
	if len(keys) < workerNum {
		for _, key := range keys {
			if !fn(key) {
				return
			}
		}
		return
	}

	chunkSize := (len(keys) + workerNum - 1) / workerNum

	var wg sync.WaitGroup
	for start := 0; start < len(keys); start += chunkSize {
		end := start + chunkSize
		if end > len(keys) {
			end = len(keys)
		}

		wg.Add(1)
		go func(chunk []string) {
			defer wg.Done()
			for _, key := range chunk {
				if !fn(key) {
					return
				}
			}
		}(keys[start:end])
	}

	wg.Wait()
}
