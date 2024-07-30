package consensus

import (
	"os"
	"testing"

	"arma/testutil"

	"github.com/stretchr/testify/assert"
)

func TestBatchAttestationDB(t *testing.T) {
	dir, err := os.MkdirTemp("", t.Name())
	assert.NoError(t, err)

	defer os.RemoveAll(dir)

	l := testutil.CreateLogger(t, 0)
	db, err := NewBatchAttestationDB(dir, l)
	assert.NoError(t, err)

	defer db.Close()

	db.Put([][]byte{{1}, {2}, {3}}, []uint64{1, 2, 3})
	assert.True(t, db.Exists([]byte{1}))
	assert.True(t, db.Exists([]byte{2}))
	assert.True(t, db.Exists([]byte{3}))
	assert.False(t, db.Exists([]byte{4}))
	digests, epochs := db.List()
	assert.Len(t, digests, 3)
	assert.Len(t, epochs, 3)

	db.Clean(3)
	assert.False(t, db.Exists([]byte{1}))
	assert.False(t, db.Exists([]byte{2}))
	assert.True(t, db.Exists([]byte{3}))
	digests, epochs = db.List()
	assert.Len(t, digests, 1)
	assert.Len(t, epochs, 1)
}
