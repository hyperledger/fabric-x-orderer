package request

import (
	"encoding/binary"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	requestInspector := &reqInspector{}

	workerNum := runtime.NumCPU()
	workPerWorker := 1000

	batch := &batch{}

	assert.False(t, batch.isEnqueued())
	batch.markEnqueued()
	assert.True(t, batch.isEnqueued())

	loaded := make(chan string, workerNum*workPerWorker)

	var wg sync.WaitGroup
	wg.Add(workerNum)

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workPerWorker; i++ {
				key := make([]byte, 8)
				binary.BigEndian.PutUint32(key, uint32(worker))
				binary.BigEndian.PutUint32(key[4:], uint32(i))
				keyID := requestInspector.RequestID(key)
				batch.Store(keyID, struct{}{})
				loaded <- keyID
			}
		}(worker)
	}

	wg.Wait()
	close(loaded)

	count := 0

	batch.Range(func(key, value any) bool {
		count++
		return true
	})

	assert.Equal(t, workerNum*workPerWorker, count)

	wg.Add(workerNum)

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workPerWorker; i++ {
				key := make([]byte, 8)
				binary.BigEndian.PutUint32(key, uint32(worker))
				binary.BigEndian.PutUint32(key[4:], uint32(i))
				keyID := requestInspector.RequestID(key)
				if _, ok := batch.Load(keyID); !ok {
					break
				}
				batch.Delete(keyID)
			}
		}(worker)
	}

	wg.Wait()

	zero := 0

	batch.Range(func(key, value any) bool {
		zero++
		return true
	})

	assert.Zero(t, zero)
}
