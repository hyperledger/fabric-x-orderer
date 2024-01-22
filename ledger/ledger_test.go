package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestCheapLedger(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", t.Name())
	assert.NoError(t, err)

	defer os.RemoveAll(tmpDir)

	cl := NewCheapLedger(filepath.Join(tmpDir, "ledger.dat"))

	seq2data := make(map[uint64][]byte)

	go func() {
		for i := 0; i < 100; i++ {
			buff := make([]byte, 2)
			binary.BigEndian.PutUint16(buff, uint16(i))
			seq2data[uint64(i)] = buff
			id := sha256.Sum256(buff)
			cl.Write(buff, id[:])
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	c := cl.Read(0, ctx)
	for entry := range c {
		if entry.Seq == 99 {
			break
		}
	}

	expected := 90
	for entry := range cl.Read(90, ctx) {
		assert.Equal(t, expected, int(entry.Seq))
		expectedID := sha256.Sum256([]byte{byte(entry.Seq >> 8), byte(entry.Seq)})
		assert.Equal(t, expectedID[:], entry.Id)
		assert.Equal(t, []byte{byte(entry.Seq >> 8), byte(entry.Seq)}, entry.Data)
		expected++
		if expected == 99 {
			cancel()
			break
		}
	}

	for seq, expectedData := range seq2data {
		assert.Equal(t, expectedData, cl.Load(seq).Data)
	}
}
