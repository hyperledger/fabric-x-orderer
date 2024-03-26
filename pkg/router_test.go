package arma

import (
	"crypto/rand"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestRouterErr(t *testing.T) {
	r := &Router{
		RequestToShard: CRC32RequestToShard(10),
		Logger:         createLogger(t, 0),
		Forward: func(shard uint16, request []byte) (BackendError, error) {
			return fmt.Errorf("500 Internal Server Error"), nil
		},
	}

	err := r.Submit([]byte{1, 2, 3})
	require.EqualError(t, err, "{\"BackendErr\": \"500 Internal Server Error\", \"ForwardErr\": \"\", \"ReqID\": \"55bc801d\"}")
}
