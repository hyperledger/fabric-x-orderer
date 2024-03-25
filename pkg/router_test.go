package arma

import (
	"crypto/rand"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSumBasedRequestToShard(t *testing.T) {
	for txSize := 1; txSize < 100; txSize++ {
		tx := make([]byte, txSize)
		_, err := rand.Read(tx)
		assert.NoError(t, err)

		for n := 1; n < 50; n++ {
			_, shardID := SumBasedRequestToShard(uint16(n))(tx)
			assert.True(t, shardID < uint16(n))
		}
	}

}
